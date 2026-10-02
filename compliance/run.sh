#!/bin/sh
# Runs the official Engine.IO v4 protocol test-suite against our server.
#
# The suite lives in this directory, copied verbatim from
# https://github.com/socketio/engine.io-protocol/tree/main/test-suite
# (commit f21de7b00ed09b3bbad2807db718ea5d6bc36aba, MIT). It expects an echo
# server on localhost:3000 with pingInterval:300, pingTimeout:200,
# maxPayload:1e6 — which is what cmd/compliance-server provides.
set -e
cd "$(dirname "$0")"

if [ ! -d node_modules ]; then
  npm ci --silent
fi

(cd .. && go build -o /tmp/go-socketio-v4-compliance-server ./cmd/compliance-server)
/tmp/go-socketio-v4-compliance-server &
SERVER_PID=$!
trap 'kill "$SERVER_PID" 2>/dev/null' EXIT

for i in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15; do
  if curl -sf -m 2 -o /dev/null "http://localhost:3000/engine.io/?EIO=4&transport=polling"; then
    break
  fi
  sleep 0.3
done

# --exit: the suite leaves sockets open by design; don't wait on them
npx mocha --exit --timeout 5000 test-suite.js
