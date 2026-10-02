# Engine.IO protocol compliance suite

The files here (`test-suite.js`, `node-imports.js`, `package.json`,
`package-lock.json`) are copied **verbatim** from the official protocol
repository — https://github.com/socketio/engine.io-protocol/tree/main/test-suite,
commit `f21de7b00ed09b3bbad2807db718ea5d6bc36aba` (2024-07-01, MIT) — so the
assertions stay exactly what the socket.io team wrote. Update them by
re-copying from a newer commit and re-running.

Run against our implementation:

```sh
compliance/run.sh
```

That starts `cmd/compliance-server` (an echo server with the reference
settings the suite expects: `pingInterval: 300`, `pingTimeout: 200`,
`maxPayload: 1e6`) on localhost:3000 and executes the suite with mocha.
