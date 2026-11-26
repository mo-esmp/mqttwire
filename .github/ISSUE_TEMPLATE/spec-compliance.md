---
name: Spec compliance
about: Report a difference between MqttWire and MQTT 5.0 or docs/mqtt-support.md
title: ''
labels: spec-compliance
assignees: ''
---

<!--
docs/mqtt-support.md states what MqttWire implements of MQTT 5.0. A difference between that
document and what MqttWire does on the wire is a bug in one of the two. What section 8 lists as
not implemented is not a compliance gap.
-->

**Standard reference**
The section of the [MQTT 5.0 standard](https://docs.oasis-open.org/mqtt/mqtt/v5.0/mqtt-v5.0.html)
and, where there is one, its normative statement (for example `[MQTT-3.1.2-3]`).

**docs/mqtt-support.md reference**
The section or table row of [docs/mqtt-support.md](https://github.com/mo-esmp/mqttwire/blob/main/docs/mqtt-support.md) this concerns, or
"not covered".

**Side**
[Server / client / reconnecting client / both]

**Current behavior**
What MqttWire does now, with the bytes on the wire if you have them.

**Required behavior**
What the standard requires, or what docs/mqtt-support.md says.

**Found with**
[Reading the code / MQTTnet / MQTT.js / another implementation, with its version]

**Affected files**
The files that would change, if you know.

**Acceptance criteria**
- [ ] MqttWire matches the standard, or the difference is stated in docs/mqtt-support.md
- [ ] A test against the known byte sequence, which fails without the fix
- [ ] The interop tests pass (`tests/MqttWire.Interop.Tests`, `tests/interop-js`)
- [ ] docs/mqtt-support.md updated
- [ ] The design in docs/specs amended, if it is changed
- [ ] CHANGELOG.md entry under `## Unreleased`
