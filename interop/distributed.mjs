import assert from "node:assert/strict";
import { io } from "socket.io-client";

const [A, B, C] = [process.env.DISTRIBUTED_A, process.env.DISTRIBUTED_B, process.env.DISTRIBUTED_C];
assert(A && B && C, "three process URLs are required");
const once = (emitter, name) => new Promise((resolve, reject) => {
  const timer = setTimeout(() => { emitter.off(name, handler); reject(new Error(`timeout: ${name}`)); }, 5000);
  const handler = (...args) => { clearTimeout(timer); resolve(args); };
  emitter.once(name, handler);
});
const status = async (url) => (await fetch(`${url}/status`)).json();
const publish = async (namespace, event, extras = {}) => {
  const response = await fetch(`${A}/publish`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ namespace, event, ...extras }) });
  assert.equal(response.status, 204);
};
const waitForPersist = async (url, before) => {
  const deadline = Date.now() + 5000;
  while ((await status(url)).disconnected <= before) {
    assert(Date.now() < deadline, "disconnect was not persisted");
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
};

for (const namespace of ["/", "/admin"]) {
  const suffix = namespace === "/" ? "" : namespace;
  const socket = io(B + suffix, { forceNew: true, transports: ["websocket"], reconnection: false, autoConnect: false });
  const received = [];
  socket.onAny((event) => received.push(event));
  try {
    const initial = once(socket, "state");
    socket.connect();
    const [state] = await initial;
    assert.equal(state.recovered, false);
    const sid = socket.id;
    const pid = socket._pid;
    const seedOffset = socket._lastOffset;
    assert(seedOffset.includes("-"));
    const before = (await status(B)).disconnected;
    const closed = once(socket, "disconnect");
    socket.io.engine.close();
    await closed;
    await waitForPersist(B, before);
    await publish(namespace, "missed-a", { rooms: ["room"], args: ["from A"] });
    await publish(namespace, "missed-binary", { rooms: [sid], binary: Buffer.from([1, 2, 3]).toString("base64") });
    await publish(namespace, "excluded", { except: { [sid]: {} } });
    await publish(namespace, "outside-room", { rooms: ["other"] });
    await publish(namespace, "volatile-gap", { volatile: true });
    await publish(namespace === "/" ? "/admin" : "/", "other-namespace");
    const replay = once(socket, "missed-a");
    const binary = once(socket, "missed-binary");
    const restoredState = once(socket, "state");
    const duringRestore = once(socket, "during-restore");
    socket.auth = { duringRestore: true };
    socket.io.uri = C;
    socket.connect();
    assert.equal((await replay)[0], "from A");
    assert.deepEqual(Buffer.from((await binary)[0]), Buffer.from([1, 2, 3]));
    await duringRestore;
    const [restored] = await restoredState;
    assert.equal(socket.recovered, true);
    assert.equal(socket.id, sid);
    assert.equal(socket._pid, pid);
    assert.deepEqual(restored.data, { user: "synthetic" });
    assert(restored.rooms.includes("room") && restored.rooms.includes(sid));
    assert(received.indexOf("missed-a") < received.indexOf("during-restore"));
    assert(received.indexOf("during-restore") < received.lastIndexOf("state"));
    assert.equal(received.filter((e) => e === "during-restore").length, 1);
    const live = once(socket, "live-after-restore");
    await publish(namespace, "live-after-restore", { rooms: [sid] });
    await live;
    assert.equal(received.filter((e) => e === "missed-a").length, 1);
    assert.equal(received.filter((e) => e === "missed-binary").length, 1);
    for (const event of ["excluded", "outside-room", "volatile-gap", "other-namespace"]) assert(!received.includes(event), event);
    const directAfterLeave = once(socket, "left-own");
    socket.emit("leave-own");
    await directAfterLeave;
    const beforeRetry = (await status(C)).disconnected;
    const closedAgain = once(socket, "disconnect");
    socket.io.engine.close();
    await closedAgain;
    await waitForPersist(C, beforeRetry);
    socket.auth = { rejectRecovery: true };
    socket.io.uri = B;
    const rejection = once(socket, "connect_error");
    socket.connect();
    assert.equal((await rejection)[0].message, "blocked recovery");
    socket.auth = {};
    const retry = once(socket, "state");
    socket.connect();
    assert.equal((await retry)[0].recovered, true);
    assert.equal(socket.id, sid);
    console.log(`PASS ${namespace}: B -> C restores identity, rooms, data and ordered binary replay from A; no duplicates or excluded events`);
  } finally { socket.disconnect(); }
}
console.log("ALL DISTRIBUTED JS TESTS PASSED");
