// Interoperability suite: drives the Go server with the real socket.io v4
// JavaScript client, covering the paths protocol unit tests cannot reach —
// the client's polling-first connect and upgrade, and its exact wire
// behaviour on both transports.
//
// Run by go test (see ../interop_test.go); INTEROP_URL points at the server.
import { io } from "socket.io-client";

const URL = process.env.INTEROP_URL;
if (!URL) {
  console.error("INTEROP_URL is not set");
  process.exit(1);
}

const results = [];
const check = (name, cond, detail = "") => {
  results.push({ name, ok: !!cond, detail });
  console.log(`${cond ? "PASS" : "FAIL"} ${name}${cond ? "" : ": " + detail}`);
};

const once = (emitter, event, what) =>
  new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      emitter.off(event, handler);
      reject(new Error(`${what}: no ${event} within 2000ms`));
    }, 2000);
    const handler = (...args) => {
      clearTimeout(timer);
      resolve(args);
    };
    emitter.once(event, handler);
  });

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// --- 1. default connect: polling first, upgraded to websocket ------------
{
  const socket = io(URL); // engine.io default: polling, then upgrade
  try {
    await once(socket, "connect", "polling-first connect");
    check("connect polling-first with upgrade", true);
    // give the upgrade a moment, then ask the client which transport won
    await sleep(300);
    check("upgraded to websocket", socket.io.engine.transport.name === "websocket",
      `transport is ${socket.io.engine.transport.name}`);
  } catch (e) {
    check("connect polling-first with upgrade", false, e.message);
  } finally {
    socket.disconnect();
  }
}

// --- main client: ws-only keeps the rest of the suite tight --------------
const socket = io(URL, { transports: ["websocket"] });
await once(socket, "connect", "ws-only connect");
check("connect websocket-only", true);

// --- 2. client event, server acks ----------------------------------------
{
  const reply = await socket.emitWithAck("echo", "marco");
  check("client event with ack", reply === "marco", `got ${JSON.stringify(reply)}`);
}

// --- 3. server acked emit (client answers) -------------------------------
{
  // the client's answer to the server's acked emit
  socket.on("provide", (cb) => cb(42));
  const answered = once(socket, "provide-result", "server acked emit");
  socket.emit("ask");
  const [value] = await answered;
  check("server EmitWithAck got the client's ack", value === 42, `got ${JSON.stringify(value)}`);
}

// --- 4. binary both ways --------------------------------------------------
{
  const binDone = new Promise((resolve) => {
    socket.on("bin-down", (buf) => resolve(Buffer.from(buf)));
  });
  socket.emit("bin-up", Buffer.from([0xde, 0xad, 0xbe, 0xef]));
  const got = await Promise.race([binDone, sleep(2500).then(() => null)]);
  check("binary echo up and down", got !== null && got.equals(Buffer.from([0xde, 0xad, 0xbe, 0xef])),
    `got ${got && got.toString("hex")}`);
}

// --- 5. second client (ws-only too), rooms, broadcast --------------------
{
  const peer = io(URL, { transports: ["websocket"] });
  await once(peer, "connect", "peer connect");
  peer.emit("join-room");
  socket.emit("join-room");
  await sleep(200);

  const gotHere = once(socket, "room-msg", "room broadcast here");
  const gotThere = once(peer, "room-msg", "room broadcast peer");
  socket.emit("to-room");

  const [mine] = await gotHere;
  const [theirs] = await gotThere;
  check("room broadcast reaches members", mine === "hello room" && theirs === "hello room",
    `${JSON.stringify(mine)} / ${JSON.stringify(theirs)}`);

  // socket.To(room) excludes the sender but reaches the peer
  const selfGot = new Promise((resolve) => socket.once("exclusive", resolve));
  const peerGot = once(peer, "exclusive", "peer exclusive");
  socket.emit("to-room-except-me");
  const exclusive = await Promise.race([selfGot.then(() => "got it"), sleep(700).then(() => "silent")]);
  check("socket.To excludes the sender", exclusive === "silent", exclusive);
  const [msg] = await peerGot;
  check("peer still received it", msg === "just you", JSON.stringify(msg));

  peer.disconnect();
}

// --- 6. custom namespace --------------------------------------------------
{
  const admin = io(`${URL}/admin`);
  const [welcome] = await once(admin, "welcome", "admin namespace");
  check("custom namespace welcome", welcome === "admin here", JSON.stringify(welcome));
  admin.disconnect();
}

// --- 7. server-side kick --------------------------------------------------
{
  const kicked = once(socket, "disconnect", "server kick");
  socket.emit("kick-me");
  const [reason] = await kicked;
  check("server-side disconnect reaches the client", socket.disconnected,
    `reason was ${reason}`);
}

// --- 8. connection middleware: auth enforced ------------------------------
{
  const authURL = process.env.INTEROP_AUTH_URL;
  if (!authURL) {
    check("middleware auth scenario", false, "INTEROP_AUTH_URL not set");
  } else {
    const refused = io(authURL, { auth: { token: "wrong" }, transports: ["websocket"] });
    const [err] = await once(refused, "connect_error", "refused connection");
    check("middleware refuses bad token", /invalid credentials/i.test(err?.message || ""),
      JSON.stringify(err?.message));

    const allowed = io(authURL, { auth: { token: "sekrit" }, transports: ["websocket"] });
    await once(allowed, "connect", "authorized connect");
    check("middleware accepts good token", true);
    allowed.disconnect();
  }
}

console.log(results.every((r) => r.ok) ? "ALL INTEROP TESTS PASSED" : "INTEROP FAILURES PRESENT");
process.exit(results.every((r) => r.ok) ? 0 : 1);
