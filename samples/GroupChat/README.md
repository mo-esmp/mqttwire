# GroupChat

A group chat on MqttWire: a topic per group, every member subscribed. It shows shared topics
through the host (design 6.10): MqttWire does not route, and the few lines that do are the
sample's own.

| Project | What it is |
|---|---|
| `GroupChat.Server` | An ASP.NET Core host with MqttWire's server at `/mqtt`. `ChatHandler` is all of the chat's logic; `GroupTable` is its table of who is in which group. It also serves the client. |
| `GroupChat.Client` | A Blazor WebAssembly page. `ChatSession` wraps one `MqttWireReconnectingClient`. |

## Running it

```
dotnet run --project samples/GroupChat/GroupChat.Server
```

Open <http://localhost:5210> in two browser tabs, give each a name, join the same group in both
and send a message from each.

## What to look at

**`ChatHandler`** (the server's side):

- `OnConnectAsync` accepts a client that gives a name as its user name, and puts a `ChatMember`
  into the connection's state slot. A real host checks a password or a token here.
- `OnSubscribe` is "join a group": it grants a filter of the form `groups/<name>` and refuses
  every other, wildcards included.
- `OnPublish` is "send to a group". It refuses a topic the sender has not joined. Otherwise it
  shows the two rules a host most easily gets wrong:
  - **the payload is copied, once.** `publish.Payload` is valid only during the call, and
    `TrySend` keeps what it is given, so one copy is shared by every member;
  - **each member gets the message at no higher QoS than it was granted.** MqttWire keeps no
    record of subscriptions, so the handler remembers the QoS and uses the lower of it and the
    publish's.
- The members are copied out of the table before anything is sent, and sent to with no lock
  held: `TrySend` can disconnect a member that has fallen too far behind, and that member's
  `OnClosed` takes the table's lock on another thread.
- `OnUnsubscribe` and `OnClosed` take the connection out of the table.

**`ChatSession`** (the client's side):

- The options of every connection come from a callback, which is where a real application
  fetches a fresh token before each attempt.
- A granted subscription is remembered by the reconnecting client and made again on every later
  connection. Stop the server and start it again: both tabs show `reconnecting`, then
  `connected`, and the next message arrives in both without a reload.
- Nothing is brought back: a message sent while a tab was disconnected never reaches it, and a
  message sent from a disconnected tab is refused at once (`NotConnected`), not queued.
- In a browser the WebSocket is the browser's own, so the client sets no request header, no TLS
  or proxy setting and no WebSocket keep-alive option on it.

## Checked in a browser

Design 6.13 item 9 asks for the client to be run once in Blazor WebAssembly by hand.

| | |
|---|---|
| Date | 2026-11-23 |
| Browser | Chromium 156.0.8078.75 on Windows 11 (the browser pane of the Claude desktop app, driven by Claude) |
| Build | Release, .NET SDK 10.0.404, ASP.NET Core 10.0.15 |

1. **Two names join the group `demo`; a message from each appears in both tabs.** Passed: `ada`
   and `bob` joined, and `bob: hello from bob` and `ada: hello from ada` appeared in both.
2. **The server is stopped and started again.** Passed, with `bob` and a third tab, `cy`: both
   showed `reconnecting` within 3 seconds of the stop and `connected` again after the start,
   without a reload. `bob: back again`, sent after that, appeared in both, so the subscription
   had been made again. (The tool that restarts the server reloads the tab it opened first,
   which is why `ada`'s tab was not one of the two.)
3. **The browser's console.** No error and no warning while the server was up. While it was
   down, the browser itself logged `WebSocket connection to 'ws://localhost:5210/mqtt' failed`
   once for each attempt to reconnect, and the .NET runtime a `MONO_WASM: WebSocket error`
   warning beside it. A page cannot turn those lines off; the application logged nothing.

Not checked: any other browser, a phone, and `wss` behind TLS.
