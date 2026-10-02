// Command example runs a small Socket.IO server demonstrating the API:
// echo with acknowledgements, broadcast, rooms and binary payloads.
// Point a socket.io v4 client at http://localhost:9901.
package main

import (
	"log"
	"net/http"

	socketio "github.com/somprasongd/go-socketio-v4"
)

func main() {
	srv := socketio.New(nil)
	ns := srv.DefaultNamespace()

	ns.OnConnect(func(s *socketio.Socket) {
		log.Printf("connect %s", s.ID())
		s.Emit("hello", "welcome — send 'echo', 'broadcast', 'room' or 'photo'")
	})

	ns.OnEvent("echo", func(s *socketio.Socket, args []any, ack func(response ...any)) {
		if ack != nil {
			ack(args...)
		}
	})

	ns.OnEvent("broadcast", func(s *socketio.Socket, args []any, ack func(response ...any)) {
		s.Broadcast().Emit("broadcast", args...)
	})

	ns.OnEvent("room", func(s *socketio.Socket, args []any, ack func(response ...any)) {
		s.Join("lounge")
		ns.To("lounge").Emit("lounge", args...)
	})

	ns.OnEvent("photo", func(s *socketio.Socket, args []any, ack func(response ...any)) {
		if blob, ok := args[0].([]byte); ok {
			log.Printf("photo: %d bytes", len(blob))
			s.Emit("photo-ok", len(blob))
		}
	})

	ns.OnDisconnect(func(s *socketio.Socket, reason string) {
		log.Printf("disconnect %s: %s", s.ID(), reason)
	})

	log.Println("listening on http://localhost:9901/socket.io/")
	log.Fatal(http.ListenAndServe("localhost:9901", srv))
}
