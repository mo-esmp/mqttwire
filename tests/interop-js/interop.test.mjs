// MQTT.js against MqttWire's server (design 6.13 item 7): an independent MQTT 5 client checks the
// server at QoS 0, 1 and 2 in both directions, and the ends a client sees: a refusal, a takeover,
// a publish over the limit and the host stopping. The server is tests/MqttWire.TestHost, started
// as a process of its own by host.mjs.

import assert from "node:assert/strict";
import { once } from "node:events";
import { after, before, test } from "node:test";
import mqtt from "mqtt";
import { startHost } from "./host.mjs";

/** How long one test may take, in milliseconds: starting a host is the slowest step. */
const timeout = 60_000;

/** Every client a test opened, so that `after` can end the ones a failed test left open. */
const clients = new Set();

/** The host most tests share, with the server's default options. */
let host;

/** Counts the clients, so that every client id and topic of a test is its own. */
let sequence = 0;

/**
 * Opens an MQTT 5 client that never reconnects, and does not wait for its CONNACK.
 *
 * @param {string} url The WebSocket URL of the MQTT endpoint.
 * @param {import("mqtt").IClientOptions} [options] Options that replace the defaults.
 * @returns {import("mqtt").MqttClient} The client, connecting.
 */
function open(url, options = {}) {
  const client = mqtt.connect(url, {
    protocolVersion: 5,
    clientId: `js-${++sequence}`,
    reconnectPeriod: 0,
    connectTimeout: 10_000,
    ...options,
  });

  // Without a listener an "error" event would end the process; `once` below still sees it.
  client.on("error", () => {});
  clients.add(client);
  return client;
}

/**
 * Opens a client and waits for its CONNACK.
 *
 * @param {string} url The WebSocket URL of the MQTT endpoint.
 * @param {import("mqtt").IClientOptions} [options] Options that replace the defaults.
 * @returns {Promise<{ client: import("mqtt").MqttClient, connack: import("mqtt").IConnackPacket }>}
 *   The connected client and the CONNACK it got. Rejects with MQTT.js's error when the server refuses.
 */
async function connect(url, options = {}) {
  const client = open(url, options);
  const [connack] = await once(client, "connect");
  return { client, connack };
}

/**
 * Collects the messages a client receives.
 *
 * @param {import("mqtt").MqttClient} client The subscribed client.
 * @param {number} count How many messages to wait for.
 * @returns {Promise<{ topic: string, payload: Buffer, packet: import("mqtt").IPublishPacket }[]>}
 *   The first `count` messages, in the order they arrived.
 */
function messages(client, count) {
  return new Promise((resolve) => {
    const received = [];
    client.on("message", (topic, payload, packet) => {
      received.push({ topic, payload, packet });
      if (received.length === count) {
        resolve(received);
      }
    });
  });
}

before(async () => {
  host = await startHost();
}, { timeout });

after(async () => {
  await Promise.all([...clients].map((client) => client.endAsync(true)));
  await host?.stop();
}, { timeout });

test("connects and reads the CONNACK properties", { timeout }, async () => {
  // Arrange, Act
  const { connack } = await connect(host.url);

  // Assert
  assert.equal(connack.reasonCode, 0);
  assert.equal(connack.properties.receiveMaximum, 256);
});

for (const qos of [0, 1, 2]) {
  test(`a publish at QoS ${qos} from MQTT.js reaches a subscriber once`, { timeout }, async () => {
    // Arrange
    const topic = `t/js/publish-${qos}`;
    const { client: subscriber } = await connect(host.url);
    const { client: publisher } = await connect(host.url);
    await subscriber.subscribeAsync(topic, { qos });
    const received = messages(subscriber, 2);

    // Act: the second message proves that nothing else came between it and the first.
    await publisher.publishAsync(topic, "hello", { qos });
    await publisher.publishAsync(topic, "end", { qos });

    // Assert
    const [first, last] = await received;
    assert.equal(first.topic, topic);
    assert.equal(first.payload.toString(), "hello");
    assert.equal(first.packet.qos, qos);
    assert.equal(last.payload.toString(), "end");
  });

  test(`MQTT.js is granted QoS ${qos} when it asks for it`, { timeout }, async () => {
    // Arrange
    const { client } = await connect(host.url);

    // Act
    const granted = await client.subscribeAsync(`t/js/granted-${qos}`, { qos });

    // Assert
    assert.equal(granted.length, 1);
    assert.equal(granted[0].qos, qos);
  });
}

test("a refused user name is reported as 0x86", { timeout }, async () => {
  // Arrange, Act
  const refused = connect(host.url, { username: "refuse-me" });

  // Assert
  await assert.rejects(refused, (error) => error.code === 0x86);
});

test("a second client with the same id takes the first one over", { timeout }, async () => {
  // Arrange
  const { client: first } = await connect(host.url, { clientId: "js-takeover" });
  const disconnected = once(first, "disconnect");

  // Act
  const { client: second } = await connect(host.url, { clientId: "js-takeover" });

  // Assert
  const [packet] = await disconnected;
  assert.equal(packet.reasonCode, 0x8e);
  assert.equal(second.connected, true);
});

test("a publish over the limit is refused with 0x97 and the connection stays open", { timeout }, async () => {
  // Arrange: the server's default limit is 65,536 bytes of payload.
  const topic = "t/js/over-the-limit";
  const { client } = await connect(host.url);
  await client.subscribeAsync(topic, { qos: 1 });
  const received = messages(client, 1);

  // Act
  const refused = client.publishAsync(topic, Buffer.alloc(70_000, 0x61), { qos: 1 });

  // Assert
  await assert.rejects(refused, (error) => error.code === 0x97);
  assert.equal(client.connected, true);
  await client.publishAsync(topic, "still here", { qos: 1 });
  const [message] = await received;
  assert.equal(message.payload.toString(), "still here");
});

test("a publish of 200,000 bytes from the host arrives whole", { timeout }, async () => {
  // Arrange: a host of its own, which accepts payloads of up to 262,144 bytes.
  const largeHost = await startHost(["--max-publish-payload-bytes", "262144"]);
  try {
    const topic = "t/js/large";
    const payload = Buffer.alloc(200_000);
    for (let i = 0; i < payload.length; i++) {
      payload[i] = (i * 31) & 0xff;
    }

    const { client: subscriber } = await connect(largeHost.url);
    const { client: publisher } = await connect(largeHost.url);
    await subscriber.subscribeAsync(topic, { qos: 1 });
    const received = messages(subscriber, 1);

    // Act
    await publisher.publishAsync(topic, payload, { qos: 1 });

    // Assert
    const [message] = await received;
    assert.equal(message.payload.length, payload.length);
    assert.ok(message.payload.equals(payload));
    await Promise.all([subscriber.endAsync(), publisher.endAsync()]);
  } finally {
    await largeHost.stop();
  }
});

test("two hundred QoS 2 publishes in a row all arrive once", { timeout }, async () => {
  // Arrange
  const topic = "t/js/two-hundred";
  const count = 200;
  const { client: subscriber } = await connect(host.url);
  const { client: publisher } = await connect(host.url);
  await subscriber.subscribeAsync(topic, { qos: 2 });
  const received = messages(subscriber, count + 1);

  // Act: none is awaited before the next is sent; the last message proves that nothing came twice.
  const published = [];
  for (let i = 0; i < count; i++) {
    published.push(publisher.publishAsync(topic, `m${i}`, { qos: 2 }));
  }

  await Promise.all(published);
  await publisher.publishAsync(topic, "end", { qos: 2 });

  // Assert
  const payloads = (await received).map((message) => message.payload.toString());
  const expected = Array.from({ length: count }, (_, i) => `m${i}`);
  assert.deepEqual(payloads, [...expected, "end"]);
});

test("the client is told 0x8B when the host stops", { timeout }, async () => {
  // Arrange: a host of its own, because this test stops it.
  const stoppingHost = await startHost();
  const { client } = await connect(stoppingHost.url);
  const disconnected = once(client, "disconnect");

  // Act
  await stoppingHost.stop();

  // Assert
  const [packet] = await disconnected;
  assert.equal(packet.reasonCode, 0x8b);
});
