---
name: Feature request
about: Suggest a new feature or improvement
title: ''
labels: enhancement
assignees: ''
---

<!--
MqttWire is a connection server, not a broker. Some things are not planned: stored sessions,
Will messages, topic aliases, shared subscriptions, subscription identifiers, enhanced
authentication, raw TCP and MQTT 3.x. See docs/mqtt-support.md, section 8, and docs/roadmap.md
before asking for one of them.
-->

**What problem does this solve?**
The problem or use case.

**Proposed solution**
How you would like it to work, and which side it is for: the client, the server, or both.

**Could the host do it?**
Whether this could be built on `IMqttWireHandler` and `MqttWireConnection` instead of inside
MqttWire, and why that is not enough.

**Cost**
What it would cost per connection or per message, in memory or time, if you know.

**Alternatives considered**
Other approaches you have thought about.

**Additional context**
Anything else: references, the part of the MQTT 5.0 standard it touches.
