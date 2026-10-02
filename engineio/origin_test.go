package engineio

import (
	"net/http"
	"strings"
	"testing"

	"github.com/gorilla/websocket"
)

func TestOriginAllowList(t *testing.T) {
	h := newHarness(t, &Options{AllowedOrigins: []string{"https://kiosk.example"}})

	req, _ := http.NewRequest(http.MethodGet, h.url+"?EIO=4&transport=polling", nil)
	req.Header.Set("Origin", "https://evil.example")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("disallowed origin status = %d, want 403", resp.StatusCode)
	}

	req, _ = http.NewRequest(http.MethodGet, h.url+"?EIO=4&transport=polling", nil)
	req.Header.Set("Origin", "https://kiosk.example")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("allowed origin status = %d, want 200", resp.StatusCode)
	}
}

func TestOriginWildcardAllowsAll(t *testing.T) {
	h := newHarness(t, &Options{AllowedOrigins: []string{"*"}})

	req, _ := http.NewRequest(http.MethodGet, h.url+"?EIO=4&transport=polling", nil)
	req.Header.Set("Origin", "https://anything.example")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("wildcard status = %d, want 200", resp.StatusCode)
	}
}

func TestOriginEmptyAllowsAll(t *testing.T) {
	h := newHarness(t, nil)

	req, _ := http.NewRequest(http.MethodGet, h.url+"?EIO=4&transport=polling", nil)
	req.Header.Set("Origin", "https://anything.example")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("empty list status = %d, want 200 (allow-all default)", resp.StatusCode)
	}
}

func TestOriginAppliesToWebSocketUpgrade(t *testing.T) {
	h := newHarness(t, &Options{AllowedOrigins: []string{"https://kiosk.example"}})

	c := h.dialWS("EIO=4&transport=websocket") // no Origin header → allowed
	_, frame, _ := readFrame(t, c)
	if !strings.HasPrefix(frame, "0") {
		t.Fatalf("allowed ws first frame = %q", frame)
	}
	_ = c.Close()

	if _, _, err := dialWSWithOrigin(h, "EIO=4&transport=websocket", "https://evil.example"); err == nil {
		t.Fatal("ws with disallowed origin connected")
	}
}

// dialWSWithOrigin opens a WebSocket carrying a custom Origin header.
func dialWSWithOrigin(h *harness, query, origin string) (*websocket.Conn, *http.Response, error) {
	wsURL := "ws" + strings.TrimPrefix(h.ts.URL, "http") + "/engine.io/?" + query
	header := http.Header{"Origin": []string{origin}}
	return websocket.DefaultDialer.Dial(wsURL, header)
}
