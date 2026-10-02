// Command compliance-server runs an Engine.IO echo server shaped exactly
// like the reference configuration the official protocol test-suite expects
// (pingInterval: 300, pingTimeout: 200, maxPayload: 1e6), listening on
// :3000. Run the suite against it with compliance/run.sh.
package main

import (
	"log"
	"net/http"
	"time"

	"github.com/somprasongd/go-socketio-v4/engineio"
)

func main() {
	eio := engineio.NewServer(&engineio.Options{
		PingInterval: 300 * time.Millisecond,
		PingTimeout:  200 * time.Millisecond,
		MaxPayload:   1_000_000,
	})
	eio.OnMessage = func(s *engineio.Session, data []byte, isBinary bool) {
		if isBinary {
			_ = s.SendBinary(data)
			return
		}
		_ = s.SendText(string(data))
	}

	log.Println("engine.io compliance server on http://localhost:3000/engine.io/")
	log.Fatal(http.ListenAndServe("localhost:3000", eio))
}
