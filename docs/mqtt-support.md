# The part of MQTT 5 that MqttWire implements

This is the stated scope of MqttWire `0.1.0-preview.1`: what its server and its client do with
every packet and property of MQTT 5, the limits and deadlines they hold, and what is not
implemented. A difference between this document and what MqttWire does on the wire is a bug in
one of the two; please report it.

MqttWire speaks **MQTT 5.0 over WebSockets only**: binary messages, WebSocket sub-protocol
`mqtt`. A request to the server's endpoint that does not ask for that sub-protocol is answered
with HTTP 400, and a text message closes the WebSocket with status 1003.

**MQTT packets and WebSocket messages are not the same thing.** Both sides read the incoming
messages as one stream of bytes: a packet may span messages, and a message may hold several
packets. What each side writes:

- **The server** writes acknowledgements that wait together as one message, and a PUBLISH whose
  packet is over 16,384 bytes as several messages: its header, then its payload in pieces of at
  most 65,536 bytes. A client must not assume one packet per message.
- **The client** writes one packet per message.

After a refusing CONNACK or a DISCONNECT, the WebSocket is closed with status 1000.

The server is a connection server, not a broker: it handles the protocol for each connection
and leaves who may connect, what a subscription means and where a message goes to the host's
handler. Where this document says "the host", that handler decides.

## 1. At a glance

| | |
|---|---|
| Protocol | MQTT 5.0. A CONNECT for another version is refused with `0x84`. |
| Transport | WebSockets, on ASP.NET Core (server) and `ClientWebSocket` (client). No raw TCP. |
| QoS | 0, 1 and 2, in both directions, as complete exchanges inside one connection. |
| Sessions | None. The server does not look at the Clean Start flag: every connection starts with nothing, and the CONNACK says Session Present 0. Session Expiry Interval is answered with 0. Nothing is kept for a client that is away, and nothing is sent a second time. |
| Subscriptions | Passed to the host, filter by filter. MqttWire keeps no subscription table and matches no wildcard. |
| Retained messages | Not stored. The retain flag of a publish is passed to the host. |
| Authentication | User name and password in CONNECT, checked by the host. Enhanced authentication (AUTH) is refused. |
| Not implemented | Will messages, topic aliases, shared subscriptions, subscription identifiers, stored sessions, MQTT 3.x (section 8). |

## 2. Packets

| Packet | Server reads | Server writes | Client reads | Client writes |
|---|---|---|---|---|
| CONNECT | yes | | | yes |
| CONNACK | | yes | yes | |
| PUBLISH (QoS 0, 1, 2) | yes | yes | yes | yes |
| PUBACK, PUBREC, PUBREL, PUBCOMP | yes | yes | yes | yes |
| SUBSCRIBE | yes | | | yes |
| SUBACK | | yes | yes | |
| UNSUBSCRIBE | yes | | | yes |
| UNSUBACK | | yes | yes | |
| PINGREQ | yes | | | yes |
| PINGRESP | | yes | yes | |
| DISCONNECT | yes | yes | yes | yes |
| AUTH | recognised only to refuse it | | | |

Malformed input is answered with a reason code and the end of the connection, never with an
exception in the host.

## 3. What the server does with each packet

**The first packet** must be a complete CONNECT, within 10 seconds of the WebSocket being
accepted.

The checks run in the order of this table, and the first row that applies answers:

| CONNECT | Answer |
|---|---|
| The first packet is not a CONNECT, or its fixed header is malformed | No MQTT packet; the WebSocket is closed with status 1002. |
| Longer than 16,384 bytes | CONNACK `0x95` (packet too large). |
| The protocol name or version is not that of MQTT 5 | CONNACK `0x84` (unsupported protocol version). |
| Malformed, or holding something that is not allowed | CONNACK with the reason code of the fault: `0x81`, `0x82` (Authentication Data without an Authentication Method is one such), or `0x90` (topic name invalid) for a Will topic that is empty or holds a wildcard. |
| Carries an Authentication Method | CONNACK `0x8C` (bad authentication method). |
| Carries a Will | CONNACK `0x87` (not authorized), "Will messages are not supported." |
| Maximum Packet Size below 4,096 | CONNACK `0x95`. |
| The client id is empty, longer than 256 bytes, or holds a control character (U+0001 to U+001F, U+007F to U+009F) or a line or paragraph separator (U+2028, U+2029) | CONNACK `0x85` (client identifier not valid). |
| The server is stopping | CONNACK `0x8B` (server shutting down); a new WebSocket request gets HTTP 503. |
| The host refuses | CONNACK with the host's reason code (`0x80` or above) and reason string. |
| The host does not answer within 10 seconds | CONNACK `0x88` (server unavailable). |
| Accepted | CONNACK success with the properties of section 5. |

So a CONNECT with a Will and an Authentication Method gets `0x8C`, and one with an empty client
id and a Maximum Packet Size of 100 gets `0x95`.

A refusing CONNACK respects the Maximum Packet Size the CONNECT announced: its reason string is
left out when the packet would not fit, and when not even its five bytes fit, no MQTT packet is
written and the WebSocket is closed with status 1002. Two refusals cannot respect a limit,
because it is not known when they are written: the one for a CONNECT that is malformed, and
the one for a CONNECT over 16,384 bytes.

**One connection per client id.** A new connection with a client id that is in use takes the
old one over, when the host allows it: the old connection gets DISCONNECT `0x8E` (session taken
over).

**After the CONNACK:**

| Packet | What the server does |
|---|---|
| PUBLISH, QoS 0 | Handed to the host. No answer. |
| PUBLISH, QoS 1 | Handed to the host. PUBACK: success, or the host's refusal. |
| PUBLISH, QoS 2 | Handed to the host once. PUBREC: success, or the host's refusal. A repeated identifier that is still remembered is answered with PUBREC success again and not handed on, whatever its DUP flag says. |
| PUBLISH above the server's highest QoS | DISCONNECT `0x9B` (QoS not supported). The CONNACK announced Maximum QoS 1 in that case. |
| PUBLISH with a payload over the server's limit | Refused with `0x97` (quota exceeded), "The message is too large.": a PUBACK at QoS 1, a failing PUBREC at QoS 2, nothing at QoS 0. The connection stays open. The host is not called. |
| PUBLISH too large to be read at all (below) | DISCONNECT `0x95` (packet too large). |
| PUBREL | PUBCOMP. For an identifier that is not remembered, PUBCOMP `0x92` (packet identifier not found). |
| PUBACK, PUBREC, PUBCOMP | They end, or move on, the exchange of a message the server sent (section 4). |
| SUBSCRIBE | The host is asked once per filter. SUBACK carries one reason code per filter, and the reason string of the host's first refusal. A grant is the lowest of what the host granted, what the client asked for and the server's highest QoS. A filter that is not a valid MQTT filter gets `0x8F` (topic filter invalid) without the host being asked. Subscription options (No Local, Retain As Published, Retain Handling) are read and have no effect, except that No Local on a `$share/` filter is a protocol error (`0x82`), as MQTT says. |
| UNSUBSCRIBE | The host is told once per filter. UNSUBACK success. |
| PINGREQ | PINGRESP. |
| DISCONNECT | The connection is closed; no DISCONNECT is sent back. |
| A second CONNECT; AUTH; CONNACK, SUBACK, UNSUBACK, PINGRESP; the reserved type 0 | DISCONNECT `0x82` (protocol error). |
| A malformed packet | DISCONNECT `0x81` (malformed packet). |
| A PUBLISH whose topic holds `+` or `#` | DISCONNECT `0x90` (topic name invalid). |
| Any packet other than PUBLISH over 16,384 bytes | DISCONNECT `0x95` (packet too large). |

Where MQTT names a more specific reason code than `0x81` or `0x82`, that one is used.

**How large a PUBLISH may be.** The server judges a PUBLISH from its Remaining Length, which
covers the topic, the packet identifier, the properties and the payload, in this order:

1. **Up to the payload limit plus 5,120 bytes: read whole,** then judged exactly. A payload
   over the limit is refused with `0x97`; anything else goes to the host. The 5,120 bytes are
   room for the topic and the properties.
2. **Above that, up to 16 times the payload limit: skipped.** The packet is not buffered; it is
   discarded as it arrives, and then refused with `0x97` like the first.
3. **Above both: DISCONNECT `0x95`.**

The first step comes first whatever the limit is. So a connection can make the server buffer a
PUBLISH of up to the payload limit plus 5,120 bytes, however small the limit: with a limit of 1
byte, a PUBLISH with a 20-byte topic and a 1-byte payload is read and accepted. And with a
limit below 342 bytes there is no second step, because 16 times the limit is then less than
the limit plus 5,120.

**Reason codes the server sends on its own account,** besides those above:

| Code | When |
|---|---|
| `0x83` implementation specific error | The host's handler threw. |
| `0x8B` server shutting down | The host is stopping. |
| `0x8D` keep alive timeout | No packet from the client for 1.5 times the keep-alive. |
| `0x93` receive maximum exceeded | The client has more than 256 publishes at QoS 1 or 2 unanswered. |
| `0x94` topic alias invalid | A PUBLISH carries a Topic Alias. |
| `0x95` packet too large | A message the host sends does not fit the client's Maximum Packet Size, or is over the outgoing byte limit. Also a SUBACK or UNSUBACK that would not fit the client's Maximum Packet Size: the connection ends before the host is asked. |
| `0x97` quota exceeded | The client has fallen too far behind: more messages or bytes wait for it than the outgoing limits allow, or more than 1,024 acknowledgements, or 65,536 bytes of them. |
| `0xA1` subscription identifiers not supported | A SUBSCRIBE carries a Subscription Identifier. |
| Any code the host chooses | The host closes a connection itself (`MqttWireConnection.Close`), with a reason code and a reason string of its own. |

When a write to the client is already in progress as the server decides to end a connection,
the link is aborted and no DISCONNECT is sent: a socket that is not taking data is not asked to
carry its own notice.

## 4. QoS

All three levels work in both directions. The connecting client chooses the QoS of what it
publishes; the host chooses the QoS of what it sends. The server's `MaxQoS` option (1 or 2) caps
both.

- **The exchanges are complete and standard, inside one connection.** They add no guarantee
  across reconnects: there is no stored session, and a message is never sent a second time.
- **From the client, QoS 2:** the identifier is remembered from PUBREC until PUBREL. A publish
  the host refuses is answered with a failing PUBREC and is not remembered.
- **To the client, QoS 1 and 2:** the server honours the client's Receive Maximum, at most 256.
  A message that has to wait for a free place counts towards the outgoing limits.
- **Acknowledgements the server does not expect:** one for an unknown identifier is ignored,
  except that an unknown PUBREC without a failure code is answered with PUBREL `0x92`. One of
  the wrong kind for a known identifier is a protocol error (`0x82`).
- **A phase of an exchange that the client does not complete within 30 seconds** ends the
  connection with `0x82`.
- **Unsubscribing does not cancel a message the server has already taken from the host.**
  `TrySend` answering `Taken` means accepted, not delivered: the message is written when the
  socket allows, also after an UNSUBSCRIBE, and it is dropped if the connection closes or is
  evicted first. It is not a delivery receipt.

The client (`MqttWireClient`) follows the same rules from its side: it announces a Receive
Maximum of 1,024, honours the server's, never has more than 1,024 publishes in flight, and
treats the connection as lost when an acknowledgement is outstanding for longer than twice the
keep-alive interval.

## 5. Properties

"Ignored" means: checked for form, then without effect, which MQTT 5 allows for that property.
An ignored property must still be one that is allowed in its packet, appear no more often than
it may, and hold a value of the right type; otherwise the packet is malformed (`0x81`). A value
that is of the right type but not allowed, such as a Payload Format Indicator of 2 or a Response
Topic with a wildcard, is a protocol error (`0x82`).

**Which error is which:**

- **Malformed packet (`0x81`):** the bytes do not form a valid packet. A length that runs past
  the packet, a bad variable-length integer, invalid UTF-8, wrong reserved flags, an unknown
  property, a property in a packet it does not belong to, a once-only property twice.
- **Protocol error (`0x82`):** the packet is well formed, but not allowed now or with that
  value. A second CONNECT, an acknowledgement of the wrong kind, Receive Maximum 0, Maximum
  Packet Size 0, a Subscription Identifier in a client's PUBLISH, a packet identifier of 0, an
  AUTH packet.

**CONNECT, read by the server**

| Property | Treatment |
|---|---|
| Session Expiry Interval | Overridden: when the client asks for more than 0, the CONNACK carries Session Expiry Interval 0. |
| Receive Maximum | Honoured for what the server sends at QoS 1 and 2, up to 256. 0 is a protocol error. |
| Maximum Packet Size | Honoured for everything the server sends, the refusing CONNACK included. 0 is a protocol error; less than 4,096 is refused. |
| Topic Alias Maximum | Ignored: the server never uses aliases. |
| Request Response Information | Ignored. |
| Request Problem Information | Honoured. When it is 0, PUBACK, PUBREC, PUBREL, PUBCOMP, SUBACK and UNSUBACK carry their reason code but no reason string. |
| User Property | Passed to the host. |
| Authentication Method, Authentication Data | Refused: CONNACK `0x8C`. |
| Will flag and Will properties | Refused: CONNACK `0x87`. |

**PUBLISH, SUBSCRIBE, UNSUBSCRIBE and DISCONNECT, read by the server**

| Property | Treatment |
|---|---|
| Payload Format Indicator, Content Type, Message Expiry Interval, Response Topic, Correlation Data, User Property (in a PUBLISH) | Ignored. They are not passed to the host in this version. |
| Topic Alias | Refused: DISCONNECT `0x94`. A Topic Alias of 0 is a protocol error (`0x82`). |
| Subscription Identifier in a PUBLISH | Protocol error (`0x82`). |
| Subscription Identifier in a SUBSCRIBE | Refused: DISCONNECT `0xA1`. A Subscription Identifier of 0 is a protocol error (`0x82`). |
| User Property, Reason String, Session Expiry Interval (in the other packets) | Ignored. |

**CONNACK, written by the server**

| Property | When |
|---|---|
| Session Present | Always 0. |
| Receive Maximum | Always 256. |
| Subscription Identifiers Available | Always 0. |
| User Property | The ones the host returned, at most 1,024 bytes. |
| Maximum QoS | 1, only when the server's `MaxQoS` is 1. |
| Session Expiry Interval | 0, only when the client asked for more. |
| Server Keep Alive | 60, only when the client asked for none or for more than 60 seconds. |
| Reason String | On a refusal, when there is one. A refusing CONNACK carries nothing else. |

Maximum Packet Size, Retain Available, Wildcard Subscription Available and Shared Subscription
Available are not announced. What a host does with a large or a retained publish, or with a
wildcard or shared filter, is its own answer in the acknowledgement.

**Everything else the server writes.** A message to a client is a PUBLISH with the host's
topic, payload and QoS, and no properties; its retain flag is never set. Acknowledgements and
DISCONNECT carry a reason code and, for a refusal, a Reason String of at most 256 bytes.

**CONNECT, written by the client**

| Field or property | Value |
|---|---|
| Clean Start | Always 1. |
| Keep Alive | The `KeepAlive` option, 1 to 65,535 seconds; 15 by default. |
| Receive Maximum | 1,024. |
| Maximum Packet Size | The `MaxIncomingPacketBytes` option, 4,096 to 268,435,460; 1 MiB by default. |
| Client id, user name, password | From the options. |
| Will, Session Expiry Interval, authentication properties, user properties | Never sent. |

**CONNACK, read by the client**

| Property | Treatment |
|---|---|
| Session Present | Must be 0; otherwise the client ends the connection with DISCONNECT `0x82`. |
| Reason String, User Property | Given to the caller in the connect result. |
| Server Keep Alive, Receive Maximum, Maximum Packet Size | Honoured. Maximum Packet Size limits every packet sent after the CONNACK. |
| Maximum QoS | Honoured: a publish above it fails at once and is not sent. |
| Everything else | Ignored. |

**What else the client does with what it reads.** A PUBLISH with a Topic Alias ends the
connection with DISCONNECT `0x94`; a packet over `MaxIncomingPacketBytes` with `0x95`, before
it is buffered; too many unanswered QoS 2 publishes from the server with `0x93`. The client's
own publishes, subscriptions and unsubscriptions carry no properties; a publish never sets the
retain flag, and a SUBSCRIBE asks for one filter with a QoS and no other option.

**How the client ends a connection:**

| When | What it sends |
|---|---|
| The server's packet is malformed, or not allowed | DISCONNECT `0x81` or `0x82`. |
| The application's `OnMessage` throws | DISCONNECT `0x83`. |
| The application disposes the client | DISCONNECT `0x00`. |
| A timeout, or a failed read or write | Nothing: the link is aborted. |

## 6. Limits

**The server's options:**

| Option | Default | Range | Meaning |
|---|---|---|---|
| `MaxQoS` | 2 | 1 or 2 | The highest QoS accepted from clients and used towards them. |
| `MaxPublishPayloadBytes` | 65,536 | 1 to 16,777,216 | The largest payload a client may publish. |
| `MaxOutgoingMessages` | 1,000 | at least 1 | How many messages may wait for one client. |
| `MaxOutgoingBytes` | 1,048,576 | at least 65,536 | How many bytes may wait for one client: the topic and the payload of every message taken and not yet written, and the waiting acknowledgements. |

A client that falls further behind than the two outgoing limits allow is disconnected.

**The server's fixed limits:**

| What | Limit |
|---|---|
| Receive Maximum announced to clients | 256 |
| Messages in flight to a client at QoS 1 and 2 | the client's Receive Maximum, at most 256 |
| Any packet other than PUBLISH | 16,384 bytes |
| A PUBLISH read whole into a buffer | a Remaining Length of up to the payload limit plus 5,120 bytes, however small the limit |
| A PUBLISH skipped unbuffered and refused with `0x97` | a Remaining Length above that, up to 16 times the payload limit |
| A PUBLISH that ends the connection with `0x95` | a Remaining Length above both |
| Client id | 1 to 256 bytes, no control character |
| Maximum Packet Size a client may announce | at least 4,096 |
| Keep-alive | what the client asks for, at most 60 seconds |
| Topic of a message the host sends | 65,526 bytes |
| Reason String the server sends | 256 bytes |
| User properties in a CONNACK | 1,024 bytes |
| Acknowledgements waiting to be written | 1,024 packets and 65,536 bytes; a client that lets more wait is disconnected with `0x97` |

**The client's limits:**

| What | Limit |
|---|---|
| Largest packet accepted (`MaxIncomingPacketBytes`) | 1 MiB by default; 4,096 to 268,435,460 |
| Receive Maximum announced | 1,024 |
| Publishes in flight at QoS 1 and 2 | the server's Receive Maximum, at most 1,024 |
| Publish bytes waiting for a slow link | 1 MiB; a publish beyond it fails with `SendQueueFull`, except that one is always taken when nothing waits |

## 7. Deadlines

The server measures every deadline on a monotonic clock and enforces them in one sweep that
runs every 2 seconds, so a deadline is acted on up to 2 seconds after it passes. The lengths are
fixed.

| Deadline | Length | What happens |
|---|---|---|
| Connect | 10 s from the WebSocket's acceptance to a complete CONNECT | The link is aborted. |
| Authentication | 10 s for the host to answer | CONNACK `0x88`, then close. |
| Partial packet | 10 s from the start of a packet to its last byte | Close, `0x82`. |
| Keep-alive | 1.5 times the connection's keep-alive without a packet from the client | Close, `0x8D`. |
| Write | 15 s for one piece of at most 64 KiB to be taken by the network | The link is aborted. |
| Acknowledgement | 30 s for each phase of a QoS 1 or 2 exchange | Close, `0x82`. |
| Close | 2 s from the decision to close | The link is aborted. |

**When the host stops,** a new WebSocket request gets HTTP 503, every connection is closed with
`0x8B` within the close deadline, and the server waits until all have ended. Two exceptions:

- **A connection with a write in progress** is aborted and gets no `0x8B`, like every close
  that meets a write in progress.
- **A host whose `OnConnectAsync` blocks its calling thread and ignores its token** is not
  waited for. Two seconds after the token was cancelled the server aborts that connection's
  link, logs the call (event 22) and stops waiting for it. The stop then completes while the
  call is still running, and what only that call's return releases stays alive until then: the
  connection's slot, its request, and a read buffer if packets came behind the CONNECT.

**The client:**

| Deadline | Length | What happens |
|---|---|---|
| Connect | 10 s to open the WebSocket and receive the CONNACK | The connect fails. |
| Keep-alive | The Server Keep Alive from the CONNACK when there is one, else `KeepAlive`. A PINGREQ is sent when nothing was sent for half the interval. | A PINGRESP that does not come within one interval ends the connection. |
| Acknowledgement | Twice the keep-alive interval for the oldest unanswered request | The connection is treated as lost. |
| Close | 2 s | The link is aborted. |

**When the CONNACK sets Server Keep Alive to 0,** the client sends no PINGREQ and has no ping
deadline. The acknowledgement deadline stays, at twice the client's own `KeepAlive`: with a
`KeepAlive` of 10 seconds, an unanswered publish at QoS 1 ends the connection after 20.

The reconnecting client (`MqttWireReconnectingClient`) makes a new connection for every
attempt. By default it waits 500 ms before the first retry and doubles the wait up to 15
seconds (`InitialDelay` and `MaxDelay`), each with a random part of ±20 %. It subscribes again to what was subscribed through it, sends nothing a
second time, and stops for good on CONNACK `0x84`, `0x85`, `0x86`, `0x87` and `0x8A`.

## 8. Not implemented

**Not planned:**

- **Sessions that survive a reconnect,** and with them offline messages and re-sending.
- **Will messages.**
- **Topic aliases,** in either direction.
- **Shared subscriptions** as a feature of the server. A `$share/` filter is passed to the host
  like any other.
- **Subscription identifiers.**
- **Enhanced authentication** (the AUTH packet).
- **MQTT 3.1 and 3.1.1.**
- **A raw TCP endpoint,** and WebSocket compression.

**Not in this version; each may come later, with a design of its own:**

- **Routing between clients:** a subscription table, wildcard matching, fan-out. Today a host
  does that on top; `samples/GroupChat` shows how.
- **Retained messages.** Today the retain flag of a publish is passed to the host.
- **The properties of a PUBLISH** (Response Topic, Correlation Data, Content Type, user
  properties and the others): today they are accepted and dropped, not passed to the host.
- **Metrics,** and limits across connections.

[The roadmap](roadmap.md) is where this is decided.

## 9. What it is tested against

Besides its own tests, which check the codec against byte sequences from the standard, MqttWire
is run against two independent MQTT 5 implementations. Their versions are pinned, and raising
one is a deliberate change.

| Implementation | Version | What is run |
|---|---|---|
| [MQTTnet](https://github.com/dotnet/MQTTnet) | 5.2.0.1603 | Its client against MqttWire's server, and MqttWire's client against its broker, over Kestrel. |
| [MQTT.js](https://github.com/mqttjs/MQTT.js) | 5.16.1 | Its client against MqttWire's server, in Node 24. |

Each at QoS 0, 1 and 2 in both directions; MqttWire's client is also run against MqttWire's
server. The tests are `tests/MqttWire.Interop.Tests` and `tests/interop-js`. Other clients and
brokers have not been tried.
