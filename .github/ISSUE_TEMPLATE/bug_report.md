---
name: Bug report
about: Report a bug in MqttWire or MqttWire.Server
title: ''
labels: bug
assignees: ''
---

<!-- A security vulnerability does not belong here: see SECURITY.md and report it privately. -->

**Describe the bug**
What goes wrong, and which side it is on: the client, the reconnecting client or the server.

**To reproduce**
The smallest code or steps that show it:
1. ...
2. ...

**Expected behavior**
What you expected to happen.

**What happened on the wire**
If you have them: the packets, the reason codes, the WebSocket close status, and the server's log
line that says why the connection ended.

**Environment**
- MqttWire / MqttWire.Server version:
- .NET version:
- OS:
- Host: [ASP.NET Core / Blazor WebAssembly / console / other]
- The other side, if not MqttWire: [MQTTnet / MQTT.js / a broker / other, with its version]

**Additional context**
Anything else: stack traces, logs, screenshots.
