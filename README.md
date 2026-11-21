# mqttwire
A lean MQTT 5 connection server and client for .NET, over WebSockets. Your app decides who connects, what a subscription means, and where each message goes.

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
  `QoSNotSupported` and `NoFreePacketIdentifier` mean the request was not sent.
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

```
dotnet pack src/MqttWire -c Release -o artifacts/packages -p:Version=0.1.0-dev.1
```

Use a new `-dev.N` version for each pack, so that a consumer never restores a stale copy from the
NuGet cache. `artifacts/` is not committed.
