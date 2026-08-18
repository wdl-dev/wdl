# WebSocket and streaming smoke worker

Manual live diagnostic worker for ordinary HTTP, WebSocket upgrade/close,
server-sent streaming, and downstream cancellation behavior.

```bash
npm install --ignore-scripts --prefix examples/ws-echo
wdl deploy examples/ws-echo --ns demo
curl -H "Host: demo.workers.local" "http://localhost:8080/ws-echo/"
curl -N -H "Host: demo.workers.local" "http://localhost:8080/ws-echo/stream"
npx wscat -H "Host: demo.workers.local" -c "ws://localhost:8080/ws-echo/ws"
```

Send ordinary text to receive `echo:<text>`, or send `bye` for a normal 1000
close. Request `/wait` and cancel the client to inspect cancellation behavior in
the worker logs.
