# mqttwire
A lean MQTT 5 connection server and client for .NET, over WebSockets. Your app decides who connects, what a subscription means, and where each message goes.

MqttWire is not yet published as a package; see
[Building a local package](#building-a-local-package).

## What it is, and what it is not

MqttWire is **a connection server, not a broker.** It speaks MQTT 5 over WebSockets with each
client: connections, the protocol, QoS 0, 1 and 2, limits, deadlines and shutdown. It does not
decide who may connect, what a subscription means or where a published message goes. Your
application decides all of that, through one small handler interface, and sends messages to
connections itself. An idle connection costs MqttWire almost nothing, and every connection is
bounded in bytes and in time, including one that has stopped reading.

What it does not do:

- **No routing between clients.** No subscription table, no wildcard matching, no fan-out. Your
  handler can do all of that on top.
- **No sessions that survive a reconnect.** Nothing is stored for a client that is away, and
  nothing is sent a second time. QoS 1 and 2 are complete exchanges inside one connection.
- No retained messages (the retain flag is passed to your handler), no Will messages, topic
  aliases, shared subscriptions, subscription identifiers, enhanced authentication or WebSocket
  compression.
- No raw TCP endpoint and no MQTT 3.x: MQTT 5 over WebSockets only.
- No metrics and no limits across connections. The log says why each connection ended.

## Server

The server runs inside ASP.NET Core. You implement `IMqttWireHandler`: one object for all
connections, with a state slot on each connection for your own data. This handler sends every
message a client publishes back to that client, if it subscribed to the topic:

```csharp
using MqttWire;
using MqttWire.Server;

public sealed class EchoHandler : IMqttWireHandler
{
    public ValueTask<MqttConnectResult> OnConnectAsync(MqttConnectRequest request, CancellationToken cancellationToken) =>
        ValueTask.FromResult(request.UserName is null
            ? MqttConnectResult.Refuse(MqttReasonCode.BadUserNameOrPassword, "A user name is required.")
            : MqttConnectResult.Accept(state: new Dictionary<string, MqttQoS>()));   // this connection's subscriptions

    public MqttAdmitResult OnAdmit(MqttWireConnection connection, MqttWireConnection? replaces) =>
        MqttAdmitResult.Accepted;   // a new connection with the same client id takes over the old one

    public MqttSubscribeResult OnSubscribe(MqttWireConnection connection, string topicFilter, MqttQoS requested)
    {
        Subscriptions(connection)[topicFilter] = requested;
        return MqttSubscribeResult.Granted(requested);
    }

    public void OnUnsubscribe(MqttWireConnection connection, string topicFilter) =>
        Subscriptions(connection).Remove(topicFilter);

    public MqttPublishResult OnPublish(MqttWireConnection connection, in MqttPublish publish)
    {
        string topic = publish.GetTopic();
        if (Subscriptions(connection).TryGetValue(topic, out MqttQoS granted))
        {
            // publish.Payload is valid only during this call, and TrySend keeps what it is given: send a copy.
            MqttQoS qos = publish.QoS < granted ? publish.QoS : granted;
            connection.TrySend(MqttTopic.Create(topic), publish.Payload.ToArray(), qos);
        }

        return MqttPublishResult.Accepted;
    }

    public void OnClosed(MqttWireConnection connection, MqttCloseReason reason) { }

    private static Dictionary<string, MqttQoS> Subscriptions(MqttWireConnection connection) =>
        (Dictionary<string, MqttQoS>)connection.State!;   // OnConnectAsync accepts only with this state.
}
```

- `OnConnectAsync` may take time and do I/O; its token fires after about 10 seconds, when the
  client goes away or when the server stops. The request carries the user name, the password, the
  client's user properties and the `HttpContext` of the WebSocket request (headers, cookies, the
  authenticated user).
- `OnAdmit` runs inside the server's admission step, which lets at most one connection per client
  id in: it must be quick and must not call back into the server.
- `OnSubscribe`, `OnUnsubscribe` and `OnPublish` are called one at a time for a connection, in the
  order the packets arrived, and must return quickly. `OnClosed` is called exactly once for every
  connection `OnAdmit` accepted, after the last other call for it. A handler method that throws
  closes its connection with reason code `0x83`.
- MqttWire keeps no record of subscriptions: remembering them, and sending at no higher QoS than
  you granted, is the handler's.
- `TrySend` returns at once, whatever the network does: `Taken`, `NotOpen`, `TooLarge` or
  `Evicted`. It does not copy the topic or the payload, so one payload can go to any number of
  connections; do not change it afterwards. Keep an `MqttTopic` you send to often.
- `MqttWireServer` (from dependency injection; your handler may take it in its constructor)
  finds a connection by client id with `TryGetConnection` and counts the connections with
  `ConnectionCount`. Both include a
  connection that is closing, until it has closed; `TrySend` on one that is closing answers
  `NotOpen`. `connection.Close(reasonCode)` ends a connection.

Register the server and map its endpoint:

```csharp
using MqttWire.Server;

WebApplicationBuilder builder = WebApplication.CreateBuilder(args);
builder.Services.AddMqttWireServer<EchoHandler>(options => options.MaxOutgoingBytes = 4 * 1_048_576);

WebApplication app = builder.Build();
app.UseWebSockets();
app.MapMqttWire("/mqtt");
app.Run();
```

`AddMqttWireServer<THandler>` registers your handler as a singleton (it must not depend on scoped
services; it is created when the host starts), the options and the server, and ties the server
to the host: when the host stops,
every connection is closed with `0x8B` (server shutting down), and the server waits for the
connections to end, every `OnClosed` included, before the web server stops. `MapMqttWire` maps
the endpoint and returns its `IEndpointConventionBuilder`, so you can add authorization and other
conventions to it. Clients must ask for the WebSocket sub-protocol `mqtt`. `UseWebSockets()` must
come before the endpoint.

| Option | Default | Meaning |
|---|---|---|
| `MaxQoS` | `ExactlyOnce` | The highest QoS accepted from clients and used towards them: `AtLeastOnce` or `ExactlyOnce`. |
| `MaxPublishPayloadBytes` | 65,536 | The largest payload a client may publish, from 1 to 16,777,216. |
| `MaxOutgoingMessages` | 1,000 | How many messages may wait for one client. A client that falls further behind is disconnected. |
| `MaxOutgoingBytes` | 1,048,576 | How many bytes may wait for one client, at least 65,536. A client that falls further behind is disconnected. |

An invalid option throws `OptionsValidationException` at start-up, and the host does not start.
The deadlines and the other bounds are fixed.

## Client

`MqttWireClient` is a general MQTT 5 client over WebSockets. One instance is one connection. It
never reconnects, never re-sends and keeps no session, so nothing of a broken connection
survives into the next one.

```csharp
await using MqttWireClient client = new(new MqttWireClientOptions
{
    Uri = new Uri("wss://example.com/mqtt"),
    ClientId = "my-client",
    UserName = "user",
    Password = token,
    KeepAlive = TimeSpan.FromSeconds(15),
    ConfigureWebSocket = ws => ws.SetRequestHeader("X-Thing", "value"),
    OnMessage = (in MqttPublish message) =>
    {
        // message.Payload is valid only during this call.
    },
    OnClosed = info => { },
});

MqttConnectAck ack = await client.ConnectAsync(cancellationToken);
if (!ack.IsConnected)
{
    // ack.Failure says why; ack.ReasonCode is the server's, when it refused.
    return;
}

MqttRequestResult subscribed = await client.SubscribeAsync("some/topic", MqttQoS.AtMostOnce, cancellationToken);
ValueTask<MqttRequestResult> done = client.PublishAsync("some/topic", payload, MqttQoS.AtLeastOnce);
```

- `ConnectAsync` returns an `MqttConnectAck` for every outcome: connected, refused by the server,
  or no valid CONNACK. It throws only for a caller bug or the caller's own cancellation.
- `PublishAsync`, `SubscribeAsync` and `UnsubscribeAsync` return an `MqttRequestResult`: a
  status and the server's reason code. `Completed` means the exchange ended; check `IsSuccess`
  for the reason code. `NotConnected`, `ReceiveMaximumReached`, `PacketTooLarge`,
  `QoSNotSupported`, `NoFreePacketIdentifier` and `SendQueueFull` mean the request was not sent.
  `SendQueueFull` means 1 MiB of publishes already waits for a slow link; trying again later may
  succeed.
  `ConnectionLost` means the connection ended first, so the server may or may not have received
  it. `PublishAsync` has copied the payload before it returns, and a caller that does not care
  about the acknowledgement does not await the result.
- An `MqttPublish` is valid only while `OnMessage` runs. Copy what must outlive the call. If
  `OnMessage` throws, the client ends the connection.
- `MaxIncomingPacketBytes` (default 1 MiB) is the largest packet the client accepts.

### Reconnecting client

`MqttWireReconnectingClient` is opt-in. It makes a new `MqttWireClient` for every attempt, calls
your `ClientOptions` callback before each one (so an expired token can be replaced), backs off
between attempts, and subscribes again to what you subscribed to through it. It stops when
retrying cannot help, such as a bad password.

It does not bring back messages missed while disconnected, does not queue publishes while
disconnected, and does not send anything a second time. `OnConnected` with `IsReconnect` is your
signal to catch up by your own means.

### Blazor WebAssembly

The client works in Blazor WebAssembly. There the browser supports only part of
`ClientWebSocketOptions`: `ConfigureWebSocket` cannot set request headers, TLS or proxy settings.

## Building a local package

The packages are not yet published. To try them, pack them locally:

```
dotnet pack src/MqttWire -c Release -o artifacts/packages -p:Version=0.1.0-dev.2
dotnet pack src/MqttWire.Server -c Release -o artifacts/packages -p:Version=0.1.0-dev.2
```

`MqttWire` holds the client; `MqttWire.Server` holds the server and depends on `MqttWire`, so use
the same version for both. Use a new `-dev.N` version for each pack, so that a consumer never
restores a stale copy from the NuGet cache. `artifacts/` is not committed.
