# แผนดำเนินงาน: go-socketio-v4

Socket.IO v4 + Engine.IO v4 server implementation สำหรับ Go — **เขียนใหม่ตาม protocol spec**
(ไม่ fork โค้ดเดิม) โดยใช้ official compliance suite และ JS client จริงเป็นตัวตรวจ

- **ที่มา:** Go ecosystem ไม่มี Socket.IO v4 server ที่ mature — `googollee/go-socket.io` หยุดที่
  protocol v2 และไม่มีคนดูแล; `ffenix113/go-socketio` รองรับ v4 แต่ websocket-only ไม่มี polling
  ซึ่งเป็น default ของ client จริง จึงเลือกเขียนใหม่จาก spec
- **Spec อ้างอิง:**
  - [Engine.IO protocol v4](https://socket.io/docs/v4/engine-io-protocol/) +
    [compliance test-suite ทางการ](https://github.com/socketio/engine.io-protocol/tree/main/test-suite)
  - [Socket.IO protocol v5](https://socket.io/docs/v4/socket-io-protocol/)
- **Dependencies:** `gorilla/websocket` เท่านั้น (polling เป็น net/http ล้วน)
- **วิธีทำงาน:** แต่ละ milestone = 1 issue + อย่างน้อย 1 commit; อัปเดตสถานะในไฟล์นี้ทุกครั้ง;
  ปิด issue เมื่อ milestone ผ่านเกณฑ์ตรวจของมันเอง

## สถานะรวม

| Milestone | Issue | สถานะ |
|---|---|---|
| M0 scaffold: repo + license + แผน + remote | — | ✅ เสร็จ |
| M1 engine.io core: packet + payload codec | #1 | ✅ เสร็จ |
| M2 session manager + polling transport | #2 | ✅ เสร็จ |
| M3 websocket transport + upgrade dance | #3 | ✅ เสร็จ |
| M4 engine.io compliance harness (official suite) | #4 | ✅ เสร็จ — 24/24 ผ่าน |
| M5 socket.io parser v5 (text + binary attachments) | #5 | ✅ เสร็จ |
| M6 socket.io server core: nsp/rooms/ack/broadcast | #6 | ✅ เสร็จ |
| M7 public API + http.Handler wiring + e2e | #7 | ✅ เสร็จ |
| M8 JS interop tests (socket.io-client v4 จริง) | #8 | ✅ เสร็จ — 11/11 ผ่าน |
| M9 docs + example + สรุปสถานะ | #9 | ✅ เสร็จ — v0.1.0 |

## รายละเอียดแต่ละ milestone

### M0 — scaffold ✅

git init, go.mod (`github.com/somprasongd/go-socketio-v4`, go 1.27), MIT LICENSE,
.gitignore, Makefile, PLAN.md นี้, สร้าง remote repo ผ่าน `gh repo create` แล้ว push

### M1 — engine.io core: packet + payload codec (issue #1) ✅

> เสร็จ: `engineio/packet` (Type 0–6, Packet, EncodePacket/DecodePacket,
> EncodePayload/DecodePayload รองรับ text/binary/mixed + Separator `0x1e`),
> `engineio.Options` + defaults, Handshake JSON byte-pinned
> ตรวจแล้ว: `go vet` + golden tests ทั้งหมดเขียว (`go test ./engineio/packet`)

- `engineio/packet`: type 0-6 (open, close, ping, pong, message, upgrade, noop),
  packet = `<type><data>`, message packet มีได้ทั้ง string และ binary
- Payload codec สำหรับ polling: text คั่นด้วย `\x1e`, binary encode เป็น `b`+base64,
  decode ทั้ง text-only และ mixed payload
- Options: PingInterval=25s, PingTimeout=20s, MaxPayload=1MB (ตาม default ของ engine.io)
- **เกณฑ์ตรวจ:** golden test byte-exact — handshake JSON, `2probe`/`3probe`,
  encode/decode round-trip ทั้ง text/binary/mixed, edge case ตัวคั่น

### M2 — session manager + polling transport (issue #2) ✅

> เสร็จ: `engineio.Server` (http.Handler, registry, handshake, Close),
> `engineio.Session` (Send/SendText/SendBinary, heartbeat timer + hbArmed
> stamp, dispatch goroutine ส่ง OnMessage/OnClose ตามลำดับ), `engineio/polling`
> (long-poll GET จอดรอ + pollCap ตัดด้วย noop, POST decode + per-packet
> maxPayload) บทเรียนที่แก้ระหว่างทาง: mutex ไม่ reentrant (ต้องมี sendLocked),
> ปิด inbound channel ตอน session ตายเพื่อให้ OnClose fire, ลิมิต POST ต้อง
> เผื่อ slack ให้ per-packet check ทำงานก่อน
> ตรวจแล้ว: 11 tests + `go test -race` เขียว

- Handshake: GET → OPEN packet `{sid, upgrades, pingInterval, pingTimeout, maxPayload}`
- Long-poll GET: ค้างรอ packet ใน buffer, ตอบทันทีเมื่อมีข้อมูล, หลาย GET พร้อมกันต้องไม่พัง
- POST: decode payload → dispatch (ping→ตอบ pong, close→teardown, message→callback)
- Heartbeat แบบ v4: client ping, server pong; ไม่ได้รับอะไรภายใน pingInterval+pingTimeout → ปิด session
- maxPayload enforcement ทั้งรับและส่ง, session expiry เมื่อไม่มี transport ต่อเนื่อง
- **เกณฑ์ตรวจ:** httptest — handshake round-trip, echo ผ่าน polling, ping/pong, session หมดอายุ

### M3 — websocket transport + upgrade dance (issue #3) ✅

> เสร็จ: `engineio/ws.go` — wsTransport (1 frame ต่อ 1 packet, writer token
> กัน concurrent write), ws-only session (OPEN ส่งบน socket เลย), upgrade
> dance (2probe→3probe→5 แล้วค่อยสลับ; probe ล้มเหลวไม่ฆ่า session เดิม),
> ตอนสลับ forward packet ที่ค้างใน buf ออกทาง ws แล้วปลุก parked poll ให้
> จบด้วย noop, polling หลังสลับ = protocol violation ปิด session
> แก้ระหว่างทาง: inbound เปลี่ยนจาก channel เป็น queue+cond และ deliver
> callback นอก lock เพื่อกัน deadlock กรณี handler echo กลับเข้า session
> ตรวจแล้ว: 17 tests + `go test -race` เขียว (รวม upgrade/binary/failed-probe)

- ws transport: 1 frame ต่อ 1 packet (text frame = string, binary frame = binary)
- Upgrade: client เปิด ws เพิ่มด้วย sid → `2probe` → `3probe` → `5` → server NOOP ช่อง polling เก่า แล้วสลับ
- รองรับ ws-first (เชื่อมมาที่ transport=websocket โดยไม่มี sid ก็ได้)
- **เกณฑ์ตรวจ:** test ด้วย gorilla client จริง — upgrade สำเร็จ, echo หลังสลับช่อง, heartbeat บน ws

### M4 — engine.io compliance harness (issue #4) ✅ 24/24 ผ่าน

> เสร็จ: `compliance/` เก็บ official suite แบบ verbatim (engine.io-protocol
> commit f21de7b, MIT) + `compliance/run.sh` + `cmd/compliance-server`
> (echo server ตาม reference config ของ suite)
> การแข่งกับ suite ทำให้ต้องแก้ protocol จริง 3 เรื่อง:
> 1. **ทิศ heartbeat กลับข้าง** — engine.io 6.4+ เป็น server ส่ง ping "2"
>    client ตอบ pong "3" (ไม่ใช่ client ping ตามที่ implement แรก); client
>    ping = protocol violation
> 2. **probe สำเร็จ = flush ช่องเก่าทันที** — หลังตอบ "3probe" ช่อง polling
>    ต้องจบด้วย noop แม้ client ยังไม่ส่ง "5"
> 3. **close semantics** — client สั่งปิดเอง: parked/late poll ได้ noop "6";
>    ปิดโดย server (timeout/protocol/shutdown): ได้ close "1"; duplicate
>    poll ปิด session ทิ้งตาม spec; poll หลัง upgrade = 400 โดย session
>    ยังอยู่
> ตรวจแล้ว: `compliance/run.sh` → 24 passing + `go test -race` เขียว

- `cmd/compliance-server`: echo server ผูกกับ library ของเรา ตั้งค่าตามที่ suite กำหนด
  (pingInterval:300, pingTimeout:200, maxPayload:1e6)
- รัน official test-suite ผ่าน Node ที่มีในเครื่อง; บันทึกวิธีรันใน README
- **เกณฑ์ตรวจ:** suite ผ่าน หรือถ้า suite ติดขัดด้าน environment ให้เหลือ Go-side protocol
  tests ครบ + บันทึกเหตุผลและวิธีรันชัดเจน

### M5 — socket.io parser v5 (issue #5) ✅

> เสร็จ: `parser` package — Type 0–6, `Decode(text, attachments)` /
> `Encode(packet) (text, attachments)`, format
> `<type>[<n>-][<nsp>,][<ack id>]<json>`; binary ทำงานสองทาง: decode แทน
> placeholder ด้วย []byte ตรวจจำนวนให้ตรง declaration, encode หยิบ []byte
> ที่ซ่อนใน args ทุกความลึกออกเป็น attachments เอง
> ตรวจแล้ว: golden tests ใช้ตัวอย่าง byte-exact จาก spec + round-trip
> (รวมข้อความไทย TIS-free UTF-8) + 16 error cases เขียว

- Packet types: 0 CONNECT, 1 DISCONNECT, 2 EVENT, 3 ACK, 4 CONNECT_ERROR,
  5 BINARY_EVENT, 6 BINARY_ACK; format `<type>[<n>-][<nsp>,][<ack id>]<json>`
- Binary: placeholder `{"_placeholder":true,"num":N}` + attachments ตามมาเป็น engine.io
  packet แยก; ฝั่งส่งต้อง replace และแยก attachments ให้ถูก
- **เกณฑ์ตรวจ:** table-driven test ใช้ตัวอย่างจาก spec ตรงตัว ทั้ง text และ binary

### M6 — socket.io server core (issue #6) ✅

> เสร็จ: root package `socketio` — Server/Of/DefaultNamespace, client
> (state machine รวม attachment buffering สำหรับ BINARY_EVENT, ack
> bookkeeping), Namespace (handlers + rooms + adapter ในตัว), Socket
> (Emit/EmitWithAck/Join/Leave/Rooms/Broadcast/To/Disconnect), แผนที่
> disconnect reason เป็นคำศัพท์ของ socket.io ("io client disconnect",
> "transport close", "ping timeout", "parse error", ...)
> การออกแบบสำคัญ: 2 locks — mu คุมลำดับ packet ขาเข้า, sendMu คุม
> ลำดับบนสาย (text+attachments ต้องติดกัน); handler รันใต้ mu และส่ง
> ผ่าน sendMu จึงไม่ deadlock เวลา handler echo กลับ
> ตรวจแล้ว: 12 tests ผ่าน fake connection + `go test -race` เขียว

- Namespace: `/` + นอกจากนี้, connect handshake พร้อม auth payload, CONNECT_ERROR
- Event dispatch, ack round-trip (ตอบ ACK งานที่ client ส่ง id มา + server emit แบบขอ ack)
- Rooms: join/leave/broadcast.to/in, adapter interface + in-memory adapter
- **เกณฑ์ตรวจ:** unit test ผ่าน fake engine.io conn — connect/auth, event+ack,
  broadcast ตาม room, disconnect สะอาด

### M7 — public API + wiring (issue #7) ✅

> เสร็จ: `New(opts)` ต่อ engine.io (OnSession/OnMessage/OnClose → core),
> `Server.ServeHTTP` mount ได้ที่ path ใดก็ได้ (แนะนำ /socket.io/),
> `EngineIO()` เผื่อปรับแต่งชั้นล่าง
> e2e: client จริง (gorilla) พูด wire packets ผ่านทั้งสอง transport —
> connect → push → event+ack → binary ขาเข้า/ออก → broadcast → disconnect;
> บทเรียน: socket.io packet ต้องห่อใน engine.io MESSAGE ("40" ไม่ใช่ "0")
> ซึ่ง client ดิบที่ส่งผิดถูก server ปฏิเสธถูกต้องตาม spec
> ตรวจแล้ว: e2e 2 tests + `go test -race` + vet + gofmt เขียวทั้งหมด

- Root package `socketio`: `New()`, OnConnect/OnDisconnect/OnEvent/OnError,
  Socket.Emit/Join/Leave/Rooms/Broadcast/Disconnect, mount ที่ `/socket.io/`,
  Close ด้วย context (shutdown สะอาด — จุดที่ googollee พังและเราออกแบบตั้งแต่ต้น)
- **เกณฑ์ตรวจ:** e2e httptest ด้วย client จริงฝั่ง Go: connect → event → ack →
  broadcast → disconnect; `go test -race` ผ่าน

### M8 — JS interop tests (issue #8) ✅ 11/11 ผ่าน

> เสร็จ: `interop/interop.mjs` ใช้ socket.io-client v4.8.4 จริง,
> `interop_test.go` spawn node (skip พร้อมเหตุผลเมื่อไม่มี node/npm)
> ครอบคลุม: polling-first connect + upgrade เป็น websocket (ตรวจ transport
> ที่ client ใช้จริง), ws-only, client event + ack, server EmitWithAck →
> client ตอบ, binary ขึ้น-ลง, room broadcast, socket.To ตัด sender,
> custom namespace, server-side kick
> บั๊กที่ interop จับได้: handler ที่เรียก `Socket.Disconnect()` deadlock
> เพราะ handler เคยรันใต้ `c.mu` — จัดโครงใหม่เป็น 3 locks (handlerMu →
> mu → sendMu) โดย handler รันนอก `c.mu`
> ตรวจแล้ว: 11/11 PASS + `go test -race` + รันซ้ำ 3 ครั้งสม่ำเสมอ

- Node script ใช้ `socket.io-client` v4 จริง: connect (polling-first default), ws-only,
  ack, binary ส่ง/รับ, broadcast, reconnect หลัง server ปิด-เปิด
- รันผ่าน `go test` ที่ spawn node; ถ้าไม่มี node ให้ skip พร้อมเหตุผลชัดเจน
- **เกณฑ์ตรวจ:** ผ่านด้วย node ในเครื่องนี้ (v24 มีอยู่จริง)

### M9 — docs + example + สรุป (issue #9) ✅ v0.1.0

> เสร็จ: README ครบ (quick start, API, ตาราง protocol support, วิธีรัน
> ทุกชุดทดสอบ, เหตุผลที่เขียนใหม่), `cmd/example` (echo/broadcast/room/
> binary — ผ่านการ smoke test กับ socket.io-client จริง: welcome, echo ack,
> photo binary, disconnect เหตุผลถูกต้อง), `make compliance`
> สถานะสุดท้าย: tag v0.1.0

## สรุปตอนจบ

- **ตัวเลขรวม:** official engine.io compliance suite 24/24 ✅ · JS interop
  (socket.io-client v4.8.4) 11/11 ✅ · unit/golden/e2e tests เขียวทั้งหมด ·
  `go test -race` ผ่าน · vet + gofmt สะอาด · dependency ภายนอกมีแค่
  gorilla/websocket
- **ที่มาของแต่ละการแก้สำคัญ** อ่านได้จากหัวข้อ milestone ข้างบน — สองจุดที่
  spec ไม่ได้บอกชัดและต้องเรียนจาก suite/interop คือทิศ heartbeat ของ
  engine.io 6.4+ และพฤติกรรม flush/noop รอบ ๆ การ upgrade
- **ยังไม่รองรับ (documented):** middleware, volatile events, Redis adapter,
  connection-state recovery

- README: การติดตั้ง, ตัวอย่างใช้งาน, ตาราง protocol support, วิธีรัน compliance/interop tests
- `cmd/example`: echo server สาธิต API
- สรุปสถานะจริงทั้งหมดลง PLAN.md, tag v0.1.0
- **เกณฑ์ตรวจ:** `go build ./... && go test ./... && go vet ./... && gofmt -l .` เขียวทั้งหมด

## v0.2 milestones

| Milestone | Issue | สถานะ |
|---|---|---|
| M10 middleware + auth payload + origin allow-list | #10 | ✅ เสร็จ — interop 13/13 |
| M11 volatile events | #11 | 🔲 รอทำ |
| M12 adapter interface + Redis adapter | #12 | 🔲 รอทำ |
| M13 connection-state recovery | #13 | 🔲 รอทำ |

### M11 — volatile events (#11) ✅

> `engineio.Session.Writable()` = transport.writable ของ socket.io
> (ws: เปิดแล้ว true; polling: true เฉพาะเมื่อมี parked GET);
> `Socket.Volatile()` / `BroadcastTarget.Volatile()` / `Namespace.Volatile()`
> — emit โดนทิ้งเงียบ ๆ เมื่อ client รับไม่ได้ ณ ตอนนั้น, plain Emit
> ไม่เคยถูก drop; ทดสอบทั้งกรณี drop/deliver/broadcast ผสม

### M12 — adapter interface + Redis adapter (#12) ✅

> แยก `socketio.Adapter` interface (AddSocket/RemoveSocket/Add/Del/All/
> Members/SocketRooms/Broadcast) + `InMemoryAdapter` default (พฤติกรรมเดิม
> ทุกอย่าง, tests เดิมผ่านหมดโดยไม่แก้); except เปลี่ยนเป็น map[socketID]
> เพื่อให้ข้าม process ได้
> `redisadapter`: Broadcast = publish JSON ลง channel → ทุก instance
> (รวมตัวเอง) deliver ให้ member ของตัวเองผ่าน subscription; membership
> เป็น local per-process (เอกสารชัด); binary ใน args เดินทางด้วย
> base64 marker; test hermetic ด้วย miniredis — 3 server process จำลอง
> (cross-process broadcast, room scoping, binary relay, namespace isolation)
> บทเรียน: gorilla ห้าม read ซ้ำหลัง read error → negative assertion
> ต้องใช้ reader goroutine; CONNECT ข้าม namespace ต้องมี comma ปิดท้าย
