package redisstreamsadapter

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	socketio "github.com/somprasongd/go-socketio-v4"
)

var recoveryOptions = socketio.RecoveryOptions{MaxDisconnectionDuration: time.Minute, MaxSessions: 10, MaxBufferedEvents: 10}

func testAdapter(t *testing.T, opts *Options) (*Adapter, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr(), MaxRetries: -1})
	if opts == nil {
		opts = &Options{}
	}
	if opts.OnError == nil {
		opts.OnError = func(error) {}
	}
	a, err := New(context.Background(), "/admin", rdb, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.Close(); _ = rdb.Close() })
	return a, mr
}

func appendRecord(t *testing.T, a *Adapter, rooms []string, except map[string]struct{}, event string, args ...any) string {
	t.Helper()
	r, err := socketio.NewRecoveryRecord(a.nsp, rooms, except, event, args, false)
	if err != nil {
		t.Fatal(err)
	}
	if err = a.PublishRecord(r); err != nil {
		t.Fatal(err)
	}
	tail, err := a.rdb.XRevRangeN(context.Background(), a.stream, "+", "-", 1).Result()
	if err != nil || len(tail) != 1 {
		t.Fatalf("tail=%v err=%v", tail, err)
	}
	return tail[0].ID
}

func save(t *testing.T, a *Adapter, offset string, data any, opts socketio.RecoveryOptions) socketio.RecoverySession {
	t.Helper()
	s := socketio.RecoverySession{PID: "private", SID: "socket", Rooms: []string{"socket", "room"}, Data: data, LastOffset: offset, IssuedOffsets: []string{offset}}
	if err := a.SaveSession(context.Background(), s, opts); err != nil {
		t.Fatal(err)
	}
	return s
}

func TestClaimReplayBinaryRoutingAndRefresh(t *testing.T) {
	a, _ := testAdapter(t, nil)
	seed := appendRecord(t, a, []string{"socket"}, nil, "seed")
	save(t, a, seed, map[string]any{"user": "alice"}, recoveryOptions)
	appendRecord(t, a, []string{"room"}, nil, "binary", []byte{1, 2, 3})
	appendRecord(t, a, []string{"other"}, nil, "outside")
	appendRecord(t, a, nil, map[string]struct{}{"socket": {}}, "excluded")
	volatile, _ := socketio.NewRecoveryRecord(a.nsp, nil, nil, "volatile", nil, true)
	if err := a.PublishRecord(volatile); err != nil {
		t.Fatal(err)
	}
	claim, err := a.ClaimSession(context.Background(), "private", seed, recoveryOptions)
	if err != nil {
		t.Fatal(err)
	}
	if len(claim.Records) != 1 || len(claim.Records[0].Bins) != 1 || claim.Records[0].Bins[0][2] != 3 {
		t.Fatalf("replay=%+v", claim.Records)
	}
	if claim.Session.Data.(map[string]any)["user"] != "alice" {
		t.Fatal(claim.Session.Data)
	}
	appendRecord(t, a, []string{"socket"}, nil, "during-middleware")
	if err = a.RefreshClaim(context.Background(), claim, recoveryOptions); err != nil {
		t.Fatal(err)
	}
	if len(claim.Records) != 2 {
		t.Fatal(claim.Records)
	}
	if err = a.FinishClaim(context.Background(), claim, true); err != nil {
		t.Fatal(err)
	}
	if _, err = a.ClaimSession(context.Background(), "private", seed, recoveryOptions); !errors.Is(err, socketio.ErrRecoveryUnavailable) {
		t.Fatal(err)
	}
}

func TestAtomicClaimReleaseAndLease(t *testing.T) {
	a, mr := testAdapter(t, nil)
	seed := appendRecord(t, a, []string{"socket"}, nil, "seed")
	save(t, a, seed, nil, recoveryOptions)
	var wg sync.WaitGroup
	claims := make(chan *socketio.RecoveryClaim, 2)
	for range 2 {
		wg.Go(func() {
			c, err := a.ClaimSession(context.Background(), "private", seed, recoveryOptions)
			if err == nil {
				claims <- c
			} else if !errors.Is(err, socketio.ErrRecoveryUnavailable) {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	close(claims)
	var winner *socketio.RecoveryClaim
	count := 0
	for c := range claims {
		winner = c
		count++
	}
	if count != 1 {
		t.Fatalf("winners=%d", count)
	}
	if err := a.FinishClaim(context.Background(), winner, false); err != nil {
		t.Fatal(err)
	}
	next, err := a.ClaimSession(context.Background(), "private", seed, recoveryOptions)
	if err != nil {
		t.Fatal(err)
	}
	mr.FastForward(11 * time.Second)
	another, err := a.ClaimSession(context.Background(), "private", seed, recoveryOptions)
	if err != nil {
		t.Fatal(err)
	}
	if err = a.FinishClaim(context.Background(), next, true); err == nil {
		t.Fatal("expired lease consumed newer claim")
	}
	if err = a.FinishClaim(context.Background(), another, true); err != nil {
		t.Fatal(err)
	}
}

func TestMissingHistoryExpiryAndReplayCap(t *testing.T) {
	t.Run("trim", func(t *testing.T) {
		a, _ := testAdapter(t, &Options{MaxLen: 2})
		seed := appendRecord(t, a, []string{"socket"}, nil, "seed")
		save(t, a, seed, nil, recoveryOptions)
		for range 3 {
			appendRecord(t, a, nil, nil, "later")
		}
		_, err := a.ClaimSession(context.Background(), "private", seed, recoveryOptions)
		if !errors.Is(err, socketio.ErrRecoveryUnavailable) {
			t.Fatal(err)
		}
	})
	t.Run("expiry", func(t *testing.T) {
		a, mr := testAdapter(t, nil)
		seed := appendRecord(t, a, []string{"socket"}, nil, "seed")
		save(t, a, seed, nil, recoveryOptions)
		mr.FastForward(2 * time.Minute)
		_, err := a.ClaimSession(context.Background(), "private", seed, recoveryOptions)
		if !errors.Is(err, socketio.ErrRecoveryUnavailable) {
			t.Fatal(err)
		}
	})
	t.Run("cap", func(t *testing.T) {
		a, _ := testAdapter(t, nil)
		seed := appendRecord(t, a, []string{"socket"}, nil, "seed")
		save(t, a, seed, nil, recoveryOptions)
		appendRecord(t, a, nil, nil, "a")
		appendRecord(t, a, nil, nil, "b")
		opts := recoveryOptions
		opts.MaxBufferedEvents = 1
		_, err := a.ClaimSession(context.Background(), "private", seed, opts)
		if !errors.Is(err, socketio.ErrRecoveryUnavailable) {
			t.Fatal(err)
		}
	})
	t.Run("invalid", func(t *testing.T) {
		a, _ := testAdapter(t, nil)
		seed := appendRecord(t, a, []string{"socket"}, nil, "seed")
		save(t, a, seed, nil, recoveryOptions)
		for _, offset := range []string{"", "oops", "0-0", "1-0", "-1-0"} {
			_, err := a.ClaimSession(context.Background(), "private", offset, recoveryOptions)
			if !errors.Is(err, socketio.ErrRecoveryUnavailable) {
				t.Fatal(offset, err)
			}
		}
	})
}

type typedData struct{ Name string }
type typedCodec struct{}

func (typedCodec) Encode(v any) ([]byte, error) { return json.Marshal(v) }
func (typedCodec) Decode(b []byte) (any, error) {
	var v typedData
	err := json.Unmarshal(b, &v)
	return v, err
}

func TestCustomCodecSessionLimitAndInvalidation(t *testing.T) {
	a, _ := testAdapter(t, &Options{DataCodec: typedCodec{}})
	seed := appendRecord(t, a, []string{"socket"}, nil, "seed")
	session := save(t, a, seed, typedData{"typed"}, recoveryOptions)
	c, err := a.ClaimSession(context.Background(), session.PID, seed, recoveryOptions)
	if err != nil {
		t.Fatal(err)
	}
	if c.Session.Data.(typedData).Name != "typed" {
		t.Fatal(c.Session.Data)
	}
	if err = a.FinishClaim(context.Background(), c, false); err != nil {
		t.Fatal(err)
	}
	opts := recoveryOptions
	opts.MaxSessions = 1
	session.PID = "second"
	if err = a.SaveSession(context.Background(), session, opts); err != nil {
		t.Fatal(err)
	}
	if _, err = a.ClaimSession(context.Background(), "private", seed, opts); !errors.Is(err, socketio.ErrRecoveryUnavailable) {
		t.Fatal(err)
	}
	if err = a.InvalidateSession(context.Background(), "second"); err != nil {
		t.Fatal(err)
	}
	if _, err = a.ClaimSession(context.Background(), "second", seed, opts); !errors.Is(err, socketio.ErrRecoveryUnavailable) {
		t.Fatal(err)
	}
}

func TestAppendAndSaveFailures(t *testing.T) {
	a, mr := testAdapter(t, &Options{OperationTimeout: 150 * time.Millisecond, BlockTime: 20 * time.Millisecond})
	if err := a.SaveSession(context.Background(), socketio.RecoverySession{PID: "bad", Data: make(chan int)}, recoveryOptions); err == nil {
		t.Fatal("unsupported data saved")
	}
	mr.Close()
	record, _ := socketio.NewRecoveryRecord(a.nsp, nil, nil, "lost", nil, false)
	if err := a.PublishRecord(record); err == nil {
		t.Fatal("append to offline Redis succeeded")
	}
	if err := a.SaveSession(context.Background(), socketio.RecoverySession{PID: "offline"}, recoveryOptions); err == nil {
		t.Fatal("save to offline Redis succeeded")
	}
}

func TestClaimRejectsUnissuedOffset(t *testing.T) {
	a, _ := testAdapter(t, nil)
	seed := appendRecord(t, a, []string{"socket"}, nil, "seed")
	save(t, a, seed, nil, recoveryOptions)
	unseen := appendRecord(t, a, []string{"socket"}, nil, "never-delivered")
	if _, err := a.ClaimSession(context.Background(), "private", unseen, recoveryOptions); !errors.Is(err, socketio.ErrRecoveryUnavailable) {
		t.Fatalf("unissued offset accepted: %v", err)
	}
}

func TestRefreshRejectsExpiredSession(t *testing.T) {
	a, mr := testAdapter(t, nil)
	seed := appendRecord(t, a, []string{"socket"}, nil, "seed")
	opts := recoveryOptions
	opts.MaxDisconnectionDuration = 3 * time.Second
	save(t, a, seed, nil, opts)
	claim, err := a.ClaimSession(context.Background(), "private", seed, opts)
	if err != nil {
		t.Fatal(err)
	}
	mr.FastForward(4 * time.Second)
	if err = a.RefreshClaim(context.Background(), claim, opts); !errors.Is(err, socketio.ErrRecoveryUnavailable) {
		t.Fatal(err)
	}
}

func TestRefreshRejectsHistoryTrimmedDuringMiddleware(t *testing.T) {
	a, _ := testAdapter(t, &Options{MaxLen: 2})
	seed := appendRecord(t, a, []string{"socket"}, nil, "seed")
	save(t, a, seed, nil, recoveryOptions)
	claim, err := a.ClaimSession(context.Background(), "private", seed, recoveryOptions)
	if err != nil {
		t.Fatal(err)
	}
	for range 3 {
		appendRecord(t, a, nil, nil, "evict")
	}
	if err = a.RefreshClaim(context.Background(), claim, recoveryOptions); !errors.Is(err, socketio.ErrRecoveryUnavailable) {
		t.Fatal(err)
	}
}

func TestIssuedOffsetStillValidAfterLeavingItsRoom(t *testing.T) {
	a, _ := testAdapter(t, nil)
	seed := appendRecord(t, a, []string{"left-room"}, nil, "seed")
	// IssuedOffsets, rather than current room membership, proves that this
	// client received the seed before leaving its room.
	save(t, a, seed, nil, recoveryOptions)
	claim, err := a.ClaimSession(context.Background(), "private", seed, recoveryOptions)
	if err != nil {
		t.Fatal(err)
	}
	if err = a.FinishClaim(context.Background(), claim, false); err != nil {
		t.Fatal(err)
	}
}
