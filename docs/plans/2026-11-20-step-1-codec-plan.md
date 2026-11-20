# Step 1: the repository and the codec — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task (the owner's choice, 2026-11-20). Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Set up the `mqttwire` repository and build MqttWire's internal MQTT 5 codec: every packet of design 6.4, decoded and encoded over spans, allocation-free, with malformed input as an error result, proven by known bytes, MQTTnet as an independent cross-check, every-byte cuts and a seeded random test.

**Architecture:** One project, `src/MqttWire`, holds the codec under `MqttWire.Codec` (internal, reached by tests and later by `MqttWire.Server` through `InternalsVisibleTo`). The codec has three layers: field primitives (variable byte integer, a span reader and writer), the fixed header and framing ("is a complete packet at the start of this buffer, and how long is it?"), and one static `…Codec` class per packet with `TryDecode` returning views and `GetSize`/`Write` encoding into a given span. Properties are validated once by a shared `PropertyBlock` against the standard's table, then read on demand.

**Tech Stack:** .NET 10, C# 14, xunit 2.9.3, MQTTnet 5.2.0.1603 (tests only), BenchmarkDotNet 0.15.8 (microbenchmarks only).

**Spec:** [docs/specs/2026-11-19-mqttwire-design.md](../specs/2026-11-19-mqttwire-design.md), sections 4, 6.1, 6.4, 6.7 and 6.13 (item 1). This is step 1 of its section 7, and step 1 of section 7 in [GrainWire on MqttWire](../specs/2026-11-19-grainwire-on-mqttwire-design.md).

## Global Constraints

- .NET 10 (`net10.0` only), C# 14, warnings are errors, MIT licence: the same conventions as GrainWire. Copy GrainWire's `Directory.Build.props` first property group and `global.json` (`10.0.100`, `latestFeature`).
- The shared `.editorconfig` is already in the repository and is binding: no `var` (error), braces always (error), file-scoped namespaces (error), static fields `s_camelCase`, private fields `_camelCase`, constants `PascalCase`.
- `MqttWire` has **no package dependencies**. MQTTnet is used in the tests only, at a pinned version (`5.2.0.1603`); BenchmarkDotNet in `bench/` only.
- The codec is **internal**. `MqttWire` gives `InternalsVisibleTo` to `MqttWire.Server`, `MqttWire.Tests` and `MqttWire.Microbench`.
- The codec **never allocates and never owns a buffer.** Decoding takes a `ReadOnlySpan<byte>` and returns views into it. Encoding writes into a `Span<byte>` the caller sized with `GetSize`.
- **Malformed input is an error result, not an exception.** Decoders return an `MqttReasonCode`: `Success`, or the reason code to close with. Exceptions are only for caller bugs: an encoder given a value MQTT cannot carry, or a decoder given something other than exactly one framed packet of its type (`PacketHeader.TryReadPacket` gives the length).
- Types carry the `MqttWire` prefix where MQTTnet has the same name (MQTTnet has `MqttUserProperty`, so ours is `MqttWireUserProperty`).
- Every step ends with every test green (design section 7).

## Decisions this plan makes

The design leaves these open or is silent on them. Each is the plan's ruling; the executor follows it, and the owner can overturn any of them at review.

1. **The codec checks form and the rules MQTT itself states; MqttWire's own refusals are applied above it.** The codec returns `MalformedPacket` (0x81), `ProtocolError` (0x82), `UnsupportedProtocolVersion` (0x84) and `TopicNameInvalid` (0x90). It *exposes*, without refusing, everything 6.6 and 6.7 refuse as policy: a Will (0x87), an authentication method (0x8C), an empty or long client id (0x85), a maximum packet size under 4 KiB (0x95), a topic alias (0x94), a subscription identifier in SUBSCRIBE (0xA1), and the validity of each topic filter (SUBACK 0x8F). The server (step 3) and the client (step 2) apply those. `Topics.IsValidFilter` is built and tested here so they can.
2. **Spans only.** No `ReadOnlySequence<byte>` overloads: the server's read buffer grows as one array (6.6), and the client can do the same.
3. **A wrong protocol name or level is `UnsupportedProtocolVersion`**, and nothing after the level is parsed (an MQTT 3.1.1 CONNECT has no properties).
4. **A non-minimal variable byte integer is malformed** (MQTT 1.5.5 requires the minimum number of bytes).
5. **A packet identifier of 0 is a protocol error** wherever an identifier is required.
6. **DUP = 1 on a QoS 0 PUBLISH is accepted and passed on.** The standard says a sender must not set it, but names no error; refusing it gains nothing.
7. **A CONNACK, SUBACK or UNSUBACK without a property length is malformed.** MQTT 5 always has one there; the short forms exist only for PUBACK, PUBREC, PUBREL, PUBCOMP and DISCONNECT, and those are accepted.
8. **Reason code values are not checked against the packet type.** A decoder hands on the byte; what an unexpected value means is decided by the caller.
9. **Strings:** valid UTF-8 and no U+0000, else malformed. The other code points MQTT says "should not" be used are accepted.
10. **Three public types appear now**, because the codec's signatures use them and steps 2 and 3 need them as they are: `MqttQoS`, `MqttReasonCode`, `MqttWireUserProperty`.
11. **MQTTnet's packet formatter is used as an independent check** of the codec in both directions (packets MQTTnet encodes are decoded by MqttWire and the other way round). Design section 4 names MQTTnet as one of the two independent implementations that check this one.
12. **A BenchmarkDotNet project, `bench/MqttWire.Microbench`, is added** beside the design's `bench/MqttWire.Bench` (which comes in step 3). It measures the codec per operation; the allocation-free rule itself is asserted by a test, not by the benchmark.
13. **Which documents are committed:** all of them, on 2026-11-20, before this step starts: the MqttWire documents moved from GrainWire (mo-esmp/mqttwire#2: the two designs, the v5 design they cite and its review), `.editorconfig`, the roadmap and this plan. The saved review conversations and the copies of GrainWire's own documents were removed by the owner.

Not in this step: the link (6.5), the client (step 2), the server (step 3), `docs/mqtt-support.md` (with the preview package, 6.15). Reading chapter 4 of the standard (design section 7) is due before the QoS exchanges are written, which is step 2.

## Review Focus

The five inputs most likely to bite a user that the design implies but its test list does not name. Each has a test in the task that owns the code.

1. **Lengths at their limits:** a 65,535-byte string, a remaining length of 268,435,455 (header length 5), a PUBLISH whose remaining length crosses 127 → 128 (header grows from 2 to 3 bytes). Expected: read and written exactly; no `int` overflow in `TotalLength`. Tests in Tasks 2, 3 and 7.
2. **Multi-byte and broken UTF-8:** a two-byte and a four-byte character in a topic and a client id decode to the same bytes; an overlong form (`C0 AF`), a surrogate (`ED A0 80`) and a cut sequence (`C3`) are malformed. Tests in Task 2.
3. **Repeated and empty user properties:** an empty name and an empty value are valid; several user properties keep their order; a once-only property twice is malformed. Tests in Task 4.
4. **Packet identifiers at the edges:** 1 and 65,535 round-trip in every packet that carries one; 0 is a protocol error. Tests in Tasks 7, 8 and 9.
5. **An encoder given something MQTT cannot carry:** a string over 65,535 UTF-8 bytes, a string containing U+0000, a packet larger than 268,435,455 bytes, a QoS 0 PUBLISH with an identifier. Expected: `ArgumentException` before anything is written. Tests in Tasks 2 and 7.

---

## File structure

```
mqttwire/
  .github/workflows/ci.yml                 build and test on push and pull request
  global.json  Directory.Build.props  Directory.Packages.props  MqttWire.slnx
  src/MqttWire/
    MqttWire.csproj
    MqttQoS.cs  MqttReasonCode.cs  MqttWireUserProperty.cs        public
    Codec/                                                        internal
      VariableByteInteger.cs  MqttReader.cs  MqttWriter.cs  FieldSize.cs
      PacketType.cs  PacketHeader.cs
      PropertyId.cs  PropertyContext.cs  PropertyBlock.cs  PropertyWriter.cs
      Topics.cs
      ConnectCodec.cs  ConnAckCodec.cs  PublishCodec.cs  AckCodec.cs
      PingCodec.cs  DisconnectCodec.cs
      SubscribeCodec.cs  SubAckCodec.cs  UnsubscribeCodec.cs  UnsubAckCodec.cs
  tests/MqttWire.Tests/
    MqttWire.Tests.csproj
    Codec/  one …Tests.cs per codec file, plus:
      Hex.cs  KnownPackets.cs  MqttNetFormatter.cs  PacketStream.cs
      StreamTests.cs  AllocationTests.cs
  bench/MqttWire.Microbench/
    MqttWire.Microbench.csproj  Program.cs  CodecBenchmarks.cs
```

Each `…Codec.cs` holds the packet's view type (a `readonly ref struct …Packet`), its encoder input where it has one (`readonly ref struct …Fields`), and the static `…Codec` class.

---

### Task 1: The repository, the public enums and the variable byte integer

**Files:**
- Create: `global.json`, `Directory.Build.props`, `Directory.Packages.props`, `MqttWire.slnx`, `.github/workflows/ci.yml`
- Create: `src/MqttWire/MqttWire.csproj`, `src/MqttWire/MqttQoS.cs`, `src/MqttWire/MqttReasonCode.cs`, `src/MqttWire/MqttWireUserProperty.cs`, `src/MqttWire/Codec/VariableByteInteger.cs`
- Create: `tests/MqttWire.Tests/MqttWire.Tests.csproj`, `tests/MqttWire.Tests/Codec/Hex.cs`, `tests/MqttWire.Tests/Codec/VariableByteIntegerTests.cs`

**Interfaces:**
- Produces:
  - `public enum MqttQoS : byte { AtMostOnce = 0, AtLeastOnce = 1, ExactlyOnce = 2 }`
  - `public enum MqttReasonCode : byte` with the MQTT 5 names and values, no duplicates (0x00 is `Success`; its other names, "normal disconnection" and "granted QoS 0", go in the XML comment): `Success = 0x00, GrantedQoS1 = 0x01, GrantedQoS2 = 0x02, DisconnectWithWillMessage = 0x04, NoMatchingSubscribers = 0x10, NoSubscriptionExisted = 0x11, ContinueAuthentication = 0x18, ReAuthenticate = 0x19, UnspecifiedError = 0x80, MalformedPacket = 0x81, ProtocolError = 0x82, ImplementationSpecificError = 0x83, UnsupportedProtocolVersion = 0x84, ClientIdentifierNotValid = 0x85, BadUserNameOrPassword = 0x86, NotAuthorized = 0x87, ServerUnavailable = 0x88, ServerBusy = 0x89, Banned = 0x8A, ServerShuttingDown = 0x8B, BadAuthenticationMethod = 0x8C, KeepAliveTimeout = 0x8D, SessionTakenOver = 0x8E, TopicFilterInvalid = 0x8F, TopicNameInvalid = 0x90, PacketIdentifierInUse = 0x91, PacketIdentifierNotFound = 0x92, ReceiveMaximumExceeded = 0x93, TopicAliasInvalid = 0x94, PacketTooLarge = 0x95, MessageRateTooHigh = 0x96, QuotaExceeded = 0x97, AdministrativeAction = 0x98, PayloadFormatInvalid = 0x99, RetainNotSupported = 0x9A, QoSNotSupported = 0x9B, UseAnotherServer = 0x9C, ServerMoved = 0x9D, SharedSubscriptionsNotSupported = 0x9E, ConnectionRateExceeded = 0x9F, MaximumConnectTime = 0xA0, SubscriptionIdentifiersNotSupported = 0xA1, WildcardSubscriptionsNotSupported = 0xA2`
  - `public readonly record struct MqttWireUserProperty(string Name, string Value)`
  - `internal static class VariableByteInteger` (namespace `MqttWire.Codec`): `const int MaxValue = 268_435_455`, `const int MaxLength = 4`, `OperationStatus TryRead(ReadOnlySpan<byte> source, out int value, out int length)` (`Done`, `NeedMoreData`, `InvalidData`), `int GetLength(int value)`, `int Write(Span<byte> destination, int value)` (throws `ArgumentOutOfRangeException` outside 0..`MaxValue`)
  - Test helper `internal static class Hex { byte[] Parse(string spaced); }`, e.g. `Hex.Parse("10 0E 00 04")`.

- [ ] **Step 1: Branch from `main`, and check the documents**

The documents are already committed (decision 13). Their links into GrainWire are GitHub URLs.

```bash
git switch main && git pull
git switch -c step-1-codec
grep -n "](\.\./" docs/specs/2026-11-19-*.md
```

Expected: `grep` finds nothing, and `git status --short` is empty.

- [ ] **Step 2: Create the build files, the two projects and CI**

- `Directory.Build.props`: GrainWire's first property group as is. Second group: `Version` `0.1.0-preview.1`, the same authors, copyright and licence, `PackageProjectUrl` and `RepositoryUrl` `https://github.com/mo-esmp/mqttwire`, `PackageTags` `mqtt;mqtt5;websocket;aspnetcore`, and the same symbol and source-link settings.
- `Directory.Packages.props`: central versions for `Microsoft.NET.Test.Sdk` 18.12.0, `xunit` 2.9.3, `xunit.runner.visualstudio` 4.0.1, `MQTTnet` 5.2.0.1603, `BenchmarkDotNet` 0.15.8.
- `src/MqttWire/MqttWire.csproj`: `IsPackable` true, `GenerateDocumentationFile` true, a one-line `Description` from the README, the README packed at the root, `InternalsVisibleTo` for `MqttWire.Server`, `MqttWire.Tests`, `MqttWire.Microbench`. No package references.
- `tests/MqttWire.Tests/MqttWire.Tests.csproj`: `Microsoft.NET.Test.Sdk`, `xunit`, `xunit.runner.visualstudio`, `MQTTnet`; a project reference to `src/MqttWire`; a global `<Using Include="Xunit" />`.
- `MqttWire.slnx` with folders `/src/`, `/tests/` (and `/bench/` in Task 11).
- `.github/workflows/ci.yml`: GrainWire's `dotnet` job (checkout, setup-dotnet `10.0.x`, restore, `build --no-restore -c Release`, `test --no-build -c Release`), without the pack and upload steps.

- [ ] **Step 3: Write the failing tests** in `tests/MqttWire.Tests/Codec/VariableByteIntegerTests.cs`

```csharp
[Theory]
[InlineData(0, "00")]
[InlineData(127, "7F")]
[InlineData(128, "80 01")]
[InlineData(16_383, "FF 7F")]
[InlineData(16_384, "80 80 01")]
[InlineData(2_097_151, "FF FF 7F")]
[InlineData(2_097_152, "80 80 80 01")]
[InlineData(268_435_455, "FF FF FF 7F")]
public void A_value_is_written_and_read_as_the_standard_shows(int value, string hex)
// Write gives exactly Hex.Parse(hex) and returns its length; GetLength(value) is that length;
// TryRead of the bytes followed by an extra 0xAA gives Done, value, and the length (the extra byte is not read).

[Theory] [InlineData("")] [InlineData("80")] [InlineData("FF FF FF")]
public void A_value_cut_short_needs_more_data(string hex)        // NeedMoreData

[Theory] [InlineData("FF FF FF FF")] [InlineData("80 00")] [InlineData("FF 80 00")]
public void Five_bytes_or_a_longer_form_than_needed_is_invalid(string hex)   // InvalidData

[Theory] [InlineData(-1)] [InlineData(268_435_456)]
public void A_value_outside_the_range_cannot_be_written(int value)   // ArgumentOutOfRangeException
```

- [ ] **Step 4: Run them to see them fail**

Run: `dotnet test tests/MqttWire.Tests --filter "FullyQualifiedName~VariableByteIntegerTests"`
Expected: build error, `VariableByteInteger` does not exist.

- [ ] **Step 5: Implement the enums, `MqttWireUserProperty` and `VariableByteInteger`**

`TryRead` stops at the first byte without the continuation bit; a fourth byte with it set is `InvalidData` without waiting for a fifth; a last byte of `0x00` after the first is `InvalidData` (decision 4).

- [ ] **Step 6: Run the tests and the build to see them pass**

Run: `dotnet build -c Release` then `dotnet test`
Expected: build with 0 warnings; all `VariableByteIntegerTests` pass.

- [ ] **Step 7: Commit**

```bash
git add global.json Directory.Build.props Directory.Packages.props MqttWire.slnx .github src tests
git commit -m "Set up the repository and add the variable byte integer to the codec."
```

---

### Task 2: Reading and writing fields

**Files:**
- Create: `src/MqttWire/Codec/MqttReader.cs`, `src/MqttWire/Codec/MqttWriter.cs`, `src/MqttWire/Codec/FieldSize.cs`
- Test: `tests/MqttWire.Tests/Codec/MqttReaderTests.cs`, `tests/MqttWire.Tests/Codec/MqttWriterTests.cs`

**Interfaces:**
- Consumes: `VariableByteInteger` (Task 1).
- Produces:
  - `internal ref struct MqttReader`, constructed with `MqttReader(ReadOnlySpan<byte> source)`: `int Remaining { get; }`, `bool TryReadByte(out byte value)`, `bool TryReadUInt16(out ushort value)`, `bool TryReadUInt32(out uint value)`, `bool TryReadVariableByteInteger(out int value)`, `bool TryReadUtf8String(out ReadOnlySpan<byte> value)`, `bool TryReadBinary(out ReadOnlySpan<byte> value)`, `bool TryReadBytes(int count, out ReadOnlySpan<byte> value)`, `ReadOnlySpan<byte> ReadToEnd()`. Big-endian. **`false` always means malformed** (a reader only ever runs over a complete packet). A failed read does not move the position.
  - `internal ref struct MqttWriter`, constructed with `MqttWriter(Span<byte> destination)`: `int Written { get; }`, `WriteByte(byte)`, `WriteUInt16(ushort)`, `WriteUInt32(uint)`, `WriteVariableByteInteger(int)`, `WriteUtf8String(string)`, `WriteUtf8String(ReadOnlySpan<byte> utf8)`, `WriteBinary(ReadOnlySpan<byte>)`, `WriteBytes(ReadOnlySpan<byte>)`.
  - `internal static class FieldSize`: `int Utf8String(string value)`, `int Utf8String(ReadOnlySpan<byte> utf8)`, `int Binary(int length)` — each the length prefix plus the bytes, and each throws `ArgumentException` above 65,535 bytes or (for `string`) for a `'\0'`.

- [ ] **Step 1: Write the failing tests**

```csharp
// MqttReaderTests
[Fact] public void Integers_are_big_endian()                       // "01 02" → 0x0102; "01 02 03 04" → 0x01020304
[Theory]
[InlineData("00 01 61", "61")]                  // "a"
[InlineData("00 02 C3 BC", "C3 BC")]            // "ü"
[InlineData("00 04 F0 9F 98 80", "F0 9F 98 80")] // a four-byte character (Review Focus 2)
[InlineData("00 00", "")]
public void A_string_is_read_as_its_utf8_bytes(string hex, string expected)
[Fact] public void A_string_of_65535_bytes_is_read()               // "FF FF" + 65,535 × 0x61 (Review Focus 1)
[Theory]
[InlineData("00 02 61")]          // length past the end
[InlineData("00 02 C3 28")]       // invalid UTF-8
[InlineData("00 02 C0 AF")]       // overlong form
[InlineData("00 03 ED A0 80")]    // a surrogate
[InlineData("00 01 C3")]          // a cut sequence
[InlineData("00 01 00")]          // U+0000
[InlineData("00")]                // the length itself cut
public void A_string_that_is_not_valid_is_malformed(string hex)    // TryReadUtf8String is false, Remaining unchanged
[Fact] public void Binary_data_is_not_checked_as_utf8()            // "00 02 C3 28" → C3 28
[Fact] public void Reading_past_the_end_is_false_and_moves_nothing() // TryReadUInt16 on "01"; TryReadBytes(3) on "01 02"

// MqttWriterTests
[Fact] public void A_string_is_written_with_its_length()            // "ü" → "00 02 C3 BC"; FieldSize.Utf8String("ü") == 4
[Fact] public void A_string_over_65535_bytes_cannot_be_written()    // FieldSize.Utf8String(new string('a', 65_536)) throws ArgumentException (Review Focus 5)
[Fact] public void A_string_with_a_null_character_cannot_be_written() // "a\0" throws ArgumentException (Review Focus 5)
[Fact] public void What_the_writer_writes_the_reader_reads()       // byte, UInt16, UInt32, VBI 16,384, string, binary in sequence
```

- [ ] **Step 2: Run them to see them fail**

Run: `dotnet test tests/MqttWire.Tests --filter "FullyQualifiedName~MqttReaderTests|FullyQualifiedName~MqttWriterTests"`
Expected: build error, `MqttReader` does not exist.

- [ ] **Step 3: Implement `MqttReader`, `MqttWriter` and `FieldSize`**

UTF-8 is checked with `System.Text.Unicode.Utf8.IsValid` plus "no `0x00` byte". Strings are written with `Encoding.UTF8.GetBytes(string, Span<byte>)`, which does not allocate.

- [ ] **Step 4: Run them to see them pass**

Run: the command of step 2. Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add src/MqttWire/Codec tests/MqttWire.Tests/Codec
git commit -m "Add the codec's field reader and writer."
```

---

### Task 3: The fixed header and framing

**Files:**
- Create: `src/MqttWire/Codec/PacketType.cs`, `src/MqttWire/Codec/PacketHeader.cs`
- Test: `tests/MqttWire.Tests/Codec/PacketHeaderTests.cs`

**Interfaces:**
- Consumes: `VariableByteInteger` (Task 1).
- Produces:
  - `internal enum PacketType : byte { Connect = 1, ConnAck, Publish, PubAck, PubRec, PubRel, PubComp, Subscribe, SubAck, Unsubscribe, UnsubAck, PingReq, PingResp, Disconnect, Auth }`
  - `internal readonly struct PacketHeader`: `PacketType Type`, `byte Flags` (low nibble), `int RemainingLength`, `int HeaderLength` (2 to 5), `int TotalLength` (`HeaderLength + RemainingLength`).
    - `static OperationStatus TryRead(ReadOnlySpan<byte> buffer, out PacketHeader header)`: the header only. `InvalidData` as soon as the first byte is invalid, even when it is the only byte.
    - `static OperationStatus TryReadPacket(ReadOnlySpan<byte> buffer, out PacketHeader header)`: `Done` only when the whole packet (`TotalLength` bytes) is in `buffer`.
    - `static int GetLength(int remainingLength)` and `static int Write(Span<byte> destination, PacketType type, byte flags, int remainingLength)`.
  - **Flag rules** (MQTT 2.1.3; a violation is `InvalidData`, which callers report as `MalformedPacket`): type 0 is invalid; PUBREL, SUBSCRIBE and UNSUBSCRIBE must have flags `0b0010`; PUBLISH may have any flags except QoS bits `11`; every other type must have flags `0`.

- [ ] **Step 1: Write the failing tests**

```csharp
[Theory]
[InlineData("10 0E", PacketType.Connect, 0x0, 14, 2)]
[InlineData("3D 07", PacketType.Publish, 0xD, 7, 2)]
[InlineData("62 02", PacketType.PubRel, 0x2, 2, 2)]
[InlineData("82 80 01", PacketType.Subscribe, 0x2, 128, 3)]
[InlineData("30 FF FF FF 7F", PacketType.Publish, 0x0, 268_435_455, 5)]   // Review Focus 1: TotalLength 268,435,460
[InlineData("F0 00", PacketType.Auth, 0x0, 0, 2)]
public void A_header_gives_type_flags_and_lengths(string hex, PacketType type, byte flags, int remaining, int headerLength)

[Theory]
[InlineData("00 00")]   // type 0
[InlineData("60 02")]   // PUBREL without 0b0010
[InlineData("80 02")]   // SUBSCRIBE without 0b0010
[InlineData("A0 02")]   // UNSUBSCRIBE without 0b0010
[InlineData("36 02")]   // PUBLISH at QoS 3
[InlineData("41 02")]   // PUBACK with a flag
[InlineData("C1")]      // invalid on its first byte alone
[InlineData("30 FF FF FF FF")]
[InlineData("30 80 00")]
public void A_header_with_wrong_flags_or_length_is_invalid(string hex)

[Theory] [InlineData("")] [InlineData("30")] [InlineData("30 80")] [InlineData("30 FF FF FF")]
public void A_header_cut_short_needs_more_data(string hex)

[Fact] public void A_packet_needs_more_data_until_its_last_byte()   // "32 0A 00 03 61 2F 62 00 0A 00 68 69": every prefix shorter than 12 bytes is NeedMoreData, 12 is Done, 13 with a trailing byte is still Done with TotalLength 12
[Fact] public void A_written_header_reads_back()                    // Write(Publish, 0x2, 128) gives "32 80 01" and GetLength(128) == 3
```

- [ ] **Step 2: Run them to see them fail**

Run: `dotnet test tests/MqttWire.Tests --filter "FullyQualifiedName~PacketHeaderTests"`
Expected: build error, `PacketHeader` does not exist.

- [ ] **Step 3: Implement `PacketType` and `PacketHeader`**

- [ ] **Step 4: Run them to see them pass** (same command). Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add src/MqttWire/Codec tests/MqttWire.Tests/Codec
git commit -m "Add the fixed header and packet framing to the codec."
```

---

### Task 4: Properties

**Files:**
- Create: `src/MqttWire/Codec/PropertyId.cs`, `src/MqttWire/Codec/PropertyContext.cs`, `src/MqttWire/Codec/PropertyBlock.cs`, `src/MqttWire/Codec/PropertyWriter.cs`
- Test: `tests/MqttWire.Tests/Codec/PropertyBlockTests.cs`

**Interfaces:**
- Consumes: `MqttReader`, `MqttWriter`, `FieldSize` (Task 2).
- Produces:
  - `internal enum PropertyId : byte` with the 27 MQTT 5 properties: `PayloadFormatIndicator = 0x01, MessageExpiryInterval = 0x02, ContentType = 0x03, ResponseTopic = 0x08, CorrelationData = 0x09, SubscriptionIdentifier = 0x0B, SessionExpiryInterval = 0x11, AssignedClientIdentifier = 0x12, ServerKeepAlive = 0x13, AuthenticationMethod = 0x15, AuthenticationData = 0x16, RequestProblemInformation = 0x17, WillDelayInterval = 0x18, RequestResponseInformation = 0x19, ResponseInformation = 0x1A, ServerReference = 0x1C, ReasonString = 0x1F, ReceiveMaximum = 0x21, TopicAliasMaximum = 0x22, TopicAlias = 0x23, MaximumQoS = 0x24, RetainAvailable = 0x25, UserProperty = 0x26, MaximumPacketSize = 0x27, WildcardSubscriptionAvailable = 0x28, SubscriptionIdentifierAvailable = 0x29, SharedSubscriptionAvailable = 0x2A`
  - `[Flags] internal enum PropertyContext : ushort { Connect = 1, Will = 2, ConnAck = 4, Publish = 8, Ack = 16 /* PUBACK, PUBREC, PUBREL, PUBCOMP */, Subscribe = 32, SubAck = 64, Unsubscribe = 128, UnsubAck = 256, Disconnect = 512, Auth = 1024 }`
  - `internal readonly ref struct PropertyBlock`:
    - `static MqttReasonCode TryRead(ref MqttReader reader, PropertyContext context, out PropertyBlock block)`: reads the length and the whole block, validates it, and moves the reader past it.
    - `bool Contains(PropertyId id)`, `bool TryGetInteger(PropertyId id, out uint value)` (byte, two-byte, four-byte and variable byte integer properties; the first occurrence), `bool TryGetBytes(PropertyId id, out ReadOnlySpan<byte> value)` (string and binary properties, without their length prefix), `UserPropertyEnumerator UserProperties { get; }`, `static PropertyBlock Empty { get; }`.
  - `internal ref struct UserPropertyEnumerator` (`GetEnumerator`, `MoveNext`, `Current`) over `internal readonly ref struct Utf8Pair { ReadOnlySpan<byte> Name; ReadOnlySpan<byte> Value; }`.
  - `internal static class PropertyWriter`: sizes `ByteSize = 2`, `UInt16Size = 3`, `UInt32Size = 5`, `int StringSize(string value)`, `int UserPropertiesSize(ReadOnlySpan<MqttWireUserProperty> properties)`; writers `WriteByte(ref MqttWriter w, PropertyId id, byte value)`, `WriteUInt16(…, ushort)`, `WriteUInt32(…, uint)`, `WriteString(…, string)`, `WriteUserProperties(ref MqttWriter w, ReadOnlySpan<MqttWireUserProperty> properties)`.
- **The table `TryRead` enforces** (MQTT 5, 2.2.2.2). Type, then the contexts where the property is allowed; anything else is `MalformedPacket`:

  | Id | Type | Allowed in |
  |---|---|---|
  | 0x01, 0x02, 0x03, 0x08, 0x09 | byte, four-byte, string, string, binary | Publish, Will |
  | 0x0B | variable byte integer | Publish (repeatable), Subscribe (once) |
  | 0x11 | four-byte | Connect, ConnAck, Disconnect |
  | 0x12, 0x1A | string | ConnAck |
  | 0x13 | two-byte | ConnAck |
  | 0x15 / 0x16 | string / binary | Connect, ConnAck, Auth |
  | 0x17, 0x19 | byte | Connect |
  | 0x18 | four-byte | Will |
  | 0x1C | string | ConnAck, Disconnect |
  | 0x1F | string | ConnAck, Ack, SubAck, UnsubAck, Disconnect, Auth |
  | 0x21, 0x22 | two-byte | Connect, ConnAck |
  | 0x23 | two-byte | Publish |
  | 0x24, 0x25, 0x28, 0x29, 0x2A | byte | ConnAck |
  | 0x26 | string pair, repeatable | every context |
  | 0x27 | four-byte | Connect, ConnAck |

  Also `MalformedPacket`: an unknown id, a block or value running past its end, invalid UTF-8, and a once-only property seen twice (tracked in a `ulong` bit mask). **`ProtocolError`**: a value of 0 for 0x0B, 0x21, 0x23 or 0x27; a value other than 0 or 1 for 0x01, 0x17, 0x19, 0x24, 0x25, 0x28, 0x29 or 0x2A.

- [ ] **Step 1: Write the failing tests**

```csharp
// The test keeps its own copy of the table above as data, and checks it against TryRead exhaustively.
[Theory, MemberData(nameof(EveryPropertyInEveryContext))]   // 27 ids × 11 contexts, each with a valid value of its type
public void A_property_is_accepted_exactly_where_the_standard_allows_it(PropertyId id, PropertyContext context, bool allowed)
// allowed → Success; not allowed → MalformedPacket

[Theory] [InlineData("02 05 00")] [InlineData("02 7F 00")]
public void An_unknown_property_is_malformed(string block)
[Theory] [InlineData("05 21 00")] [InlineData("02 21 00")] [InlineData("04 03 00 05 61")]
public void A_block_or_value_running_past_its_end_is_malformed(string block)   // block past the input; a two-byte value cut to one; a string past its block
[Fact] public void A_once_only_property_twice_is_malformed()               // "06 21 00 0A 21 00 0A" in Connect (Review Focus 3)
[Fact] public void User_properties_may_repeat_and_keep_their_order()        // "0B 26 00 01 61 00 00 26 00 00 00 00" → ("a",""), ("","") (Review Focus 3)
[Fact] public void A_subscription_identifier_may_repeat_in_publish_but_not_in_subscribe()  // "04 0B 01 0B 02": Publish Success, Subscribe MalformedPacket
[Theory]
[InlineData("03 21 00 00", PropertyContext.Connect)]          // receive maximum 0
[InlineData("05 27 00 00 00 00", PropertyContext.Connect)]    // maximum packet size 0
[InlineData("02 0B 00", PropertyContext.Publish)]             // subscription identifier 0
[InlineData("03 23 00 00", PropertyContext.Publish)]          // topic alias 0
[InlineData("02 17 02", PropertyContext.Connect)]             // request problem information 2
[InlineData("02 24 02", PropertyContext.ConnAck)]             // maximum QoS 2
[InlineData("02 01 02", PropertyContext.Publish)]             // payload format indicator 2
public void A_value_the_standard_forbids_is_a_protocol_error(string block, PropertyContext context)
[Fact] public void Invalid_utf8_in_a_user_property_name_is_malformed()      // "06 26 00 01 FF 00 00"
[Fact] public void Values_are_read_by_id()                                  // ReceiveMaximum 1,024, ReasonString "x", Contains(AuthenticationMethod) false
[Fact] public void What_the_property_writer_writes_the_block_reads()        // every PropertyWriter method into one block, then TryRead in ConnAck
```

- [ ] **Step 2: Run them to see them fail**

Run: `dotnet test tests/MqttWire.Tests --filter "FullyQualifiedName~PropertyBlockTests"`
Expected: build error, `PropertyBlock` does not exist.

- [ ] **Step 3: Implement `PropertyId`, `PropertyContext`, `PropertyBlock`, `UserPropertyEnumerator` and `PropertyWriter`**

`TryRead` validates in one pass; the accessors walk the already validated span again, so they need no error paths.

- [ ] **Step 4: Run them to see them pass** (same command). Expected: all pass, 297 cases for the table alone.

- [ ] **Step 5: Commit**

```bash
git add src/MqttWire/Codec tests/MqttWire.Tests/Codec
git commit -m "Validate and read MQTT 5 properties in the codec."
```

---

### Task 5: CONNECT, and the MQTTnet cross-check

**Files:**
- Create: `src/MqttWire/Codec/ConnectCodec.cs`
- Create: `tests/MqttWire.Tests/Codec/KnownPackets.cs`, `tests/MqttWire.Tests/Codec/MqttNetFormatter.cs`
- Test: `tests/MqttWire.Tests/Codec/ConnectCodecTests.cs`

**Interfaces:**
- Consumes: `MqttReader`, `MqttWriter`, `FieldSize`, `PacketHeader`, `PropertyBlock`, `PropertyWriter`.
- Produces:
  - `internal readonly ref struct ConnectPacket`: `ReadOnlySpan<byte> ClientId`, `bool HasUserName`, `ReadOnlySpan<byte> UserName`, `bool HasPassword`, `ReadOnlySpan<byte> Password`, `ushort KeepAliveSeconds`, `bool CleanStart`, `bool HasWill`, `PropertyBlock Properties`.
  - `internal readonly ref struct ConnectFields`: `string ClientId`, `string? UserName`, `byte[]? Password`, `ushort KeepAliveSeconds`, `ushort ReceiveMaximum` (the client sends 1,024, 6.9). The encoder always writes protocol 5, clean start, no Will, and only Receive Maximum as a property.
  - `internal static class ConnectCodec`: `MqttReasonCode TryDecode(ReadOnlySpan<byte> packet, out ConnectPacket connect)` (`packet` is one whole framed packet; another type throws `ArgumentException`), `int GetSize(in ConnectFields fields)`, `int Write(Span<byte> destination, in ConnectFields fields)`.
  - Test helpers used by every later task: `internal static class KnownPackets` with one `public static readonly byte[]` per known sequence (this task adds `Connect`, `ConnectMinimal`, `ConnectWithWill`); `internal static class MqttNetFormatter { byte[] Encode(MQTTnet.Packets.MqttPacket packet); MQTTnet.Packets.MqttPacket Decode(ReadOnlySpan<byte> packet); }` over `new MqttPacketFormatterAdapter(MqttProtocolVersion.V500, new MqttBufferWriter(4096, 65536))`. `Encode` returns `adapter.Encode(packet).ToArray()`. `Decode` reads our `PacketHeader` and calls `adapter.Decode(new ReceivedMqttPacket(packet[0], new ArraySegment<byte>(body), packet.Length))`, where `body` is the packet after its header.
- **Decoding, in order:** protocol name (not `MQTT` → `UnsupportedProtocolVersion`); level (not 5 → `UnsupportedProtocolVersion`); flags (bit 0 set, or Will QoS or Will retain set without the Will flag, or Will QoS 3 → `MalformedPacket`); keep-alive; properties (`Connect`); Authentication Data without Authentication Method → `ProtocolError`; client id; if Will: Will properties (`Will`), topic, payload; user name if flagged; password if flagged; anything left over → `MalformedPacket`; last, a Will topic that is empty or holds a wildcard → `TopicNameInvalid`.

- [ ] **Step 1: Write the failing tests**

```csharp
// KnownPackets
Connect        = "10 18 00 04 4D 51 54 54 05 C2 00 0F 03 21 04 00 00 02 63 31 00 01 75 00 01 70"
                 // client id "c1", user "u", password 0x70, keep-alive 15, clean start, receive maximum 1,024
ConnectMinimal = "10 0E 00 04 4D 51 54 54 05 02 00 00 00 00 01 63"            // client id "c", nothing else
ConnectWithWill= "10 15 00 04 4D 51 54 54 05 06 00 00 00 00 01 63 00 00 01 77 00 01 78"   // Will topic "w", payload "x"

[Fact] public void The_known_connect_decodes()          // ClientId "c1", UserName "u", Password [0x70], KeepAlive 15, CleanStart, !HasWill, ReceiveMaximum 1,024
[Fact] public void The_known_connect_is_what_the_encoder_writes()  // Write(new ConnectFields { "c1", "u", [0x70], 15, 1024 }) == KnownPackets.Connect; GetSize == 26
[Fact] public void A_connect_without_user_name_or_password_decodes()   // ConnectMinimal: !HasUserName, !HasPassword
[Fact] public void A_password_without_a_user_name_decodes()            // "10 11 00 04 4D 51 54 54 05 42 00 00 00 00 01 63 00 01 70": MQTT 5 allows it
[Fact] public void An_empty_client_id_decodes()                        // "10 0D 00 04 4D 51 54 54 05 02 00 00 00 00 00": Success, ClientId empty (the server refuses it, decision 1)
[Fact] public void A_will_is_read_and_reported()                       // ConnectWithWill: HasWill
[Fact] public void An_authentication_method_is_reported_not_refused()  // "10 12 00 04 4D 51 54 54 05 02 00 00 04 15 00 01 61 00 01 63": Properties.Contains(AuthenticationMethod)
[Theory]
[InlineData("10 0E 00 04 4D 51 54 54 05 03 00 00 00 00 01 63")]      // reserved flag
[InlineData("10 0E 00 04 4D 51 54 54 05 0A 00 00 00 00 01 63")]      // Will QoS without Will
[InlineData("10 0E 00 04 4D 51 54 54 05 22 00 00 00 00 01 63")]      // Will retain without Will
[InlineData("10 15 00 04 4D 51 54 54 05 1E 00 00 00 00 01 63 00 00 01 77 00 01 78")]   // Will QoS 3
[InlineData("10 0E 00 04 4D 51 54 54 05 42 00 00 00 00 01 63")]      // password flagged but missing
[InlineData("10 0E 00 04 4D 51 54 54 05 82 00 00 00 00 01 63")]      // user name flagged but missing
[InlineData("10 0F 00 04 4D 51 54 54 05 02 00 00 00 00 01 63 00")]   // a left-over byte
[InlineData("10 03 00 04 4D")]                                       // protocol name cut
public void A_broken_connect_is_malformed(string hex)
[Theory]
[InlineData("10 0F 00 06 4D 51 49 73 64 70 03 02 00 3C 00 01 63")]   // "MQIsdp", level 3
[InlineData("10 0E 00 04 4D 51 54 54 04 02 00 00 00 00 01 63")]      // "MQTT", level 4
public void Another_protocol_or_version_is_unsupported(string hex)     // UnsupportedProtocolVersion
[Fact] public void Authentication_data_without_a_method_is_a_protocol_error()   // "10 11 00 04 4D 51 54 54 05 02 00 00 03 16 00 00 00 01 63"

[Fact] public void A_connect_mqttnet_encodes_decodes()
// MqttConnectPacket { ClientId = "c1", Username = "u", Password = [0x70], KeepAlivePeriod = 15, CleanSession = true,
//   ReceiveMaximum = 1024, MaximumPacketSize = 65536, RequestProblemInformation = false, UserProperties = [new("k", "v")] }
// → Success; same client id, user name, password, keep-alive; MaximumPacketSize 65,536; RequestProblemInformation 0; one user property ("k","v")
[Fact] public void A_connect_this_codec_encodes_mqttnet_decodes()   // our Connect → MqttConnectPacket with the same five values
```


- [ ] **Step 2: Run them to see them fail**

Run: `dotnet test tests/MqttWire.Tests --filter "FullyQualifiedName~ConnectCodecTests"`
Expected: build error, `ConnectCodec` does not exist.

- [ ] **Step 3: Implement `ConnectPacket`, `ConnectFields`, `ConnectCodec`, `KnownPackets` and `MqttNetFormatter`**

- [ ] **Step 4: Run them to see them pass** (same command). Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add src/MqttWire/Codec tests/MqttWire.Tests/Codec
git commit -m "Decode and encode CONNECT, checked against MQTTnet."
```

---

### Task 6: CONNACK

**Files:**
- Create: `src/MqttWire/Codec/ConnAckCodec.cs`
- Modify: `tests/MqttWire.Tests/Codec/KnownPackets.cs`
- Test: `tests/MqttWire.Tests/Codec/ConnAckCodecTests.cs`

**Interfaces:**
- Consumes: as Task 5, and `MqttWireUserProperty`.
- Produces:
  - `internal readonly ref struct ConnAckPacket`: `bool SessionPresent`, `MqttReasonCode ReasonCode`, `PropertyBlock Properties`.
  - `internal readonly ref struct ConnAckFields`: `MqttReasonCode ReasonCode`, `uint? SessionExpiryInterval`, `ushort? ReceiveMaximum`, `byte? MaximumQoS`, `bool? SubscriptionIdentifiersAvailable`, `ushort? ServerKeepAlive`, `string? ReasonString`, `ReadOnlySpan<MqttWireUserProperty> UserProperties`. Session present is always written as 0 (6.6). **Properties are written in this order:** 0x11, 0x21, 0x24, 0x29, 0x13, 0x1F, 0x26.
  - `internal static class ConnAckCodec`: `TryDecode(ReadOnlySpan<byte> packet, out ConnAckPacket connAck)`, `GetSize(in ConnAckFields)`, `Write(Span<byte>, in ConnAckFields)`.
- **Decoding:** flags byte (bits 1 to 7 set → `MalformedPacket`), reason code, properties (`ConnAck`, required: decision 7), nothing left over.

- [ ] **Step 1: Write the failing tests**

```csharp
KnownPackets.ConnAck = "20 08 00 00 05 21 01 00 29 00"   // success, receive maximum 256, subscription identifiers not available

[Fact] public void The_known_connack_is_what_the_encoder_writes()   // { Success, ReceiveMaximum = 256, SubscriptionIdentifiersAvailable = false }; GetSize == 10
[Fact] public void The_known_connack_decodes()
[Fact] public void A_refusal_carries_its_reason_string_and_user_properties()
// { NotAuthorized, ReasonString = "no", UserProperties = [("k","v")] } round-trips
[Fact] public void All_properties_are_written_in_the_fixed_order()  // every field set; the written ids are 11 21 24 29 13 1F 26 in order
[Fact] public void Session_present_is_reported()                     // "20 03 01 00 00" → SessionPresent (the client closes on it, step 2)
[Theory] [InlineData("20 03 02 00 00")] [InlineData("20 02 00 00")] [InlineData("20 04 00 00 00 00")]
public void A_broken_connack_is_malformed(string hex)                // reserved flag; no property length; left-over byte
[Fact] public void A_connack_mqttnet_encodes_decodes()
// MqttConnAckPacket { ReasonCode = Success, ReceiveMaximum = 256, MaximumQoS = AtLeastOnce, ServerKeepAlive = 60,
//   ReasonString = "r", SubscriptionIdentifiersAvailable = false, UserProperties = [new("k","v")] } → the same values read back
[Fact] public void A_connack_this_codec_encodes_mqttnet_decodes()
```

- [ ] **Step 2: Run them to see them fail**

Run: `dotnet test tests/MqttWire.Tests --filter "FullyQualifiedName~ConnAckCodecTests"`
Expected: build error, `ConnAckCodec` does not exist.

- [ ] **Step 3: Implement `ConnAckPacket`, `ConnAckFields` and `ConnAckCodec`**

- [ ] **Step 4: Run them to see them pass** (same command). Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add src/MqttWire/Codec tests/MqttWire.Tests/Codec
git commit -m "Decode and encode CONNACK."
```

---

### Task 7: PUBLISH and topic names

**Files:**
- Create: `src/MqttWire/Codec/PublishCodec.cs`, `src/MqttWire/Codec/Topics.cs`
- Modify: `tests/MqttWire.Tests/Codec/KnownPackets.cs`
- Test: `tests/MqttWire.Tests/Codec/PublishCodecTests.cs`, `tests/MqttWire.Tests/Codec/TopicsTests.cs`

**Interfaces:**
- Consumes: as Task 5.
- Produces:
  - `internal enum PacketSource { Client, Server }`: who sent the packet being decoded.
  - `internal static class Topics`: `bool IsValidName(ReadOnlySpan<byte> utf8)` (not empty, no `+` or `#`), `bool IsValidFilter(ReadOnlySpan<byte> utf8)` (Task 9 tests it fully; this task adds it with the name rule's tests).
  - `internal readonly ref struct PublishPacket`: `MqttQoS QoS`, `bool Dup`, `bool Retain`, `ReadOnlySpan<byte> Topic`, `ushort PacketIdentifier` (0 at QoS 0), `PropertyBlock Properties`, `ReadOnlySpan<byte> Payload`, `bool HasTopicAlias`.
  - `internal readonly ref struct PublishFields`: `ReadOnlySpan<byte> Topic` (UTF-8, encoded once by the caller), `MqttQoS QoS`, `bool Retain`, `bool Dup`, `ushort PacketIdentifier`. MqttWire never writes PUBLISH properties: the property length is always `00`.
  - `internal static class PublishCodec`:
    - `MqttReasonCode TryDecode(ReadOnlySpan<byte> packet, PacketSource source, out PublishPacket publish)`
    - `int GetHeaderSize(in PublishFields fields, int payloadLength)`: fixed header, topic, identifier and property length; `int WriteHeader(Span<byte> destination, in PublishFields fields, int payloadLength)`. For large payloads the server writes the header from a small buffer and the payload from the host's memory (6.6).
    - `int GetSize(in PublishFields fields, int payloadLength)` and `int Write(Span<byte> destination, in PublishFields fields, ReadOnlySpan<byte> payload)`: header and a copy of the payload.
    - Caller bugs throw `ArgumentException`: QoS 0 with an identifier, QoS 1 or 2 with identifier 0, a topic that is not `Topics.IsValidName`, a packet over `VariableByteInteger.MaxValue` remaining length.
- **Decoding:** QoS, DUP and retain from the flags (decision 6); topic; identifier at QoS 1 and 2 (0 → `ProtocolError`); properties (`Publish`); a Subscription Identifier when `source` is `Client` → `ProtocolError`; an empty topic without a Topic Alias → `ProtocolError`; a topic with `+` or `#` → `TopicNameInvalid`; the rest is the payload.

- [ ] **Step 1: Write the failing tests**

```csharp
KnownPackets.PublishQoS0 = "30 04 00 01 74 00"                       // topic "t", empty payload
KnownPackets.PublishQoS1 = "32 0A 00 03 61 2F 62 00 0A 00 68 69"     // "a/b", id 10, "hi"
KnownPackets.PublishQoS2 = "3D 07 00 01 74 00 01 00 78"              // DUP, QoS 2, retain, "t", id 1, "x"

[Theory, MemberData(nameof(KnownPublishes))]
public void A_known_publish_decodes_and_is_what_the_encoder_writes(byte[] packet, MqttQoS qos, bool dup, bool retain, string topic, ushort id, string payload)
[Fact] public void Header_and_payload_written_apart_equal_the_whole()    // WriteHeader + payload bytes == Write, for payloads of 0, 1 and 70,000 bytes
[Theory] [InlineData(119)] [InlineData(120)] [InlineData(121)]           // "a/b" at QoS 1: remaining 127 / 128 / 129 (Review Focus 1)
public void The_header_grows_with_the_remaining_length(int payloadLength) // GetHeaderSize matches PacketHeader.GetLength; decode gives the payload back
[Theory] [InlineData((ushort)1)] [InlineData((ushort)65535)]             // Review Focus 4
public void Identifiers_at_the_edges_round_trip(ushort id)
[Fact] public void Identifier_zero_at_qos_1_is_a_protocol_error()        // "32 06 00 01 74 00 00 00" (Review Focus 4)
[Fact] public void A_subscription_identifier_is_refused_from_a_client_and_read_from_a_server()   // property "02 0B 05": Client → ProtocolError, Server → Success
[Fact] public void A_topic_alias_is_reported_not_refused()                // HasTopicAlias (the server refuses it, decision 1)
[Fact] public void An_empty_topic_without_an_alias_is_a_protocol_error()
[Theory] [InlineData("a/#")] [InlineData("+")] [InlineData("a+b")]
public void A_topic_name_with_a_wildcard_is_invalid(string topic)         // TopicNameInvalid
[Fact] public void A_topic_running_past_the_packet_is_malformed()         // "30 03 00 05 74"
[Theory] [InlineData("ü/x")] [InlineData("😀")]                            // Review Focus 2
public void A_multi_byte_topic_round_trips(string topic)
[Theory]   // Review Focus 5
[InlineData(MqttQoS.AtMostOnce, 7)] [InlineData(MqttQoS.AtLeastOnce, 0)]
public void A_publish_with_a_wrong_identifier_cannot_be_written(MqttQoS qos, ushort id)
[Fact] public void A_publish_over_the_largest_packet_cannot_be_written()  // GetSize with payloadLength 268_435_455 throws ArgumentException
[Fact] public void A_publish_mqttnet_encodes_decodes()
// QoS 1, topic "a/b", id 10, payload "hi", retain, with ContentType, ResponseTopic, CorrelationData, MessageExpiryInterval, PayloadFormatIndicator and a user property
// → our view: QoS, retain, topic, id and payload equal; the properties are present and ignored
[Fact] public void A_publish_this_codec_encodes_mqttnet_decodes()        // QoS 2, DUP, retain, "t", id 1, "x"

// TopicsTests (names)
[Theory] [InlineData("a", true)] [InlineData("/", true)] [InlineData("ü", true)] [InlineData("", false)] [InlineData("a/+", false)] [InlineData("#", false)]
public void A_topic_name_is_valid_without_wildcards(string name, bool valid)
```

- [ ] **Step 2: Run them to see them fail**

Run: `dotnet test tests/MqttWire.Tests --filter "FullyQualifiedName~PublishCodecTests|FullyQualifiedName~TopicsTests"`
Expected: build error, `PublishCodec` does not exist.

- [ ] **Step 3: Implement `PacketSource`, `Topics.IsValidName`, `PublishPacket`, `PublishFields` and `PublishCodec`**

- [ ] **Step 4: Run them to see them pass** (same command). Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add src/MqttWire/Codec tests/MqttWire.Tests/Codec
git commit -m "Decode and encode PUBLISH at every QoS."
```

---

### Task 8: PUBACK, PUBREC, PUBREL, PUBCOMP, the pings and DISCONNECT

**Files:**
- Create: `src/MqttWire/Codec/AckCodec.cs`, `src/MqttWire/Codec/PingCodec.cs`, `src/MqttWire/Codec/DisconnectCodec.cs`
- Modify: `tests/MqttWire.Tests/Codec/KnownPackets.cs`
- Test: `tests/MqttWire.Tests/Codec/AckCodecTests.cs`, `tests/MqttWire.Tests/Codec/PingCodecTests.cs`, `tests/MqttWire.Tests/Codec/DisconnectCodecTests.cs`

**Interfaces:**
- Consumes: as Task 5.
- Produces:
  - `internal readonly ref struct AckPacket`: `PacketType Type` (one of the four), `ushort PacketIdentifier`, `MqttReasonCode ReasonCode`, `PropertyBlock Properties`.
  - `internal static class AckCodec`: `TryDecode(ReadOnlySpan<byte> packet, out AckPacket ack)`; `int GetSize(MqttReasonCode reasonCode, string? reasonString)`; `int Write(Span<byte> destination, PacketType type, ushort packetIdentifier, MqttReasonCode reasonCode, string? reasonString)` (PUBREL gets flags `0b0010`).
    - **Encoded form:** `Success` and no reason string → remaining length 2. Otherwise the reason code; and only with a reason string, a property block holding it.
    - **Decoded forms:** remaining 2 → `Success`; 3 → the reason code; 4 or more → reason code and properties (`Ack`); identifier 0 → `ProtocolError`; anything left over → `MalformedPacket`.
  - `internal static class PingCodec`: `static ReadOnlySpan<byte> PingReq` (`C0 00`), `static ReadOnlySpan<byte> PingResp` (`D0 00`), `MqttReasonCode TryDecode(ReadOnlySpan<byte> packet)` (a remaining length other than 0 → `MalformedPacket`).
  - `internal readonly ref struct DisconnectPacket`: `MqttReasonCode ReasonCode`, `PropertyBlock Properties`.
  - `internal static class DisconnectCodec`: `TryDecode(ReadOnlySpan<byte> packet, out DisconnectPacket disconnect)` (remaining 0 → `Success`; 1 → the reason code; 2 or more → reason code and properties (`Disconnect`)); `GetSize(MqttReasonCode, string? reasonString)`; `Write(Span<byte>, MqttReasonCode, string? reasonString)` (`Success` without a reason string → `E0 00`).

- [ ] **Step 1: Write the failing tests**

```csharp
KnownPackets.PubAck          = "40 02 00 0A"
KnownPackets.PubRecRefused   = "50 03 00 0A 97"
KnownPackets.PubRel          = "62 02 00 0A"
KnownPackets.PubCompNotFound = "70 03 00 0A 92"
KnownPackets.PubAckWithReason= "40 08 00 0A 97 04 1F 00 01 78"     // quota exceeded, reason string "x"
KnownPackets.PingReq = "C0 00"   KnownPackets.PingResp = "D0 00"
KnownPackets.Disconnect = "E0 00"   KnownPackets.DisconnectTakenOver = "E0 01 8E"
KnownPackets.DisconnectWithReason = "E0 06 8E 04 1F 00 01 78"

[Theory, MemberData(nameof(KnownAcks))]
public void A_known_ack_decodes_and_is_what_the_encoder_writes(byte[] packet, PacketType type, ushort id, MqttReasonCode code, string? reasonString)
[Fact] public void Every_short_form_is_read()            // "40 02 …", "40 03 00 0A 00", "40 04 00 0A 00 00" all decode to Success
[Theory, MemberData(nameof(EveryAckTypeAtBothEdges))]    // four types × ids 1 and 65,535 (Review Focus 4)
public void Identifiers_at_the_edges_round_trip(PacketType type, ushort id)
[Fact] public void Identifier_zero_is_a_protocol_error()  // "40 02 00 00" (Review Focus 4)
[Theory] [InlineData("40 05 00 0A 00 05 1F")] [InlineData("40 04 00 0A 00 01")] [InlineData("40 05 00 0A 00 00 00")]
public void A_broken_ack_is_malformed(string hex)         // a property past its block; a block past the packet; a left-over byte
[Fact] public void A_ping_with_a_body_is_malformed()      // "C0 01 00"
[Theory, MemberData(nameof(KnownDisconnects))]
public void A_known_disconnect_decodes_and_is_what_the_encoder_writes(byte[] packet, MqttReasonCode code, string? reasonString)
[Fact] public void A_disconnect_with_session_expiry_and_server_reference_decodes()   // properties allowed in Disconnect are read
[Fact] public void Acks_and_disconnects_mqttnet_encodes_decode()   // MqttPubAckPacket, MqttPubRecPacket (failure), MqttPubRelPacket, MqttPubCompPacket, MqttDisconnectPacket with ReasonString
[Fact] public void Acks_and_disconnects_this_codec_encodes_mqttnet_decodes()
```

- [ ] **Step 2: Run them to see them fail**

Run: `dotnet test tests/MqttWire.Tests --filter "FullyQualifiedName~AckCodecTests|FullyQualifiedName~PingCodecTests|FullyQualifiedName~DisconnectCodecTests"`
Expected: build error, `AckCodec` does not exist.

- [ ] **Step 3: Implement `AckPacket`, `AckCodec`, `PingCodec`, `DisconnectPacket` and `DisconnectCodec`**

- [ ] **Step 4: Run them to see them pass** (same command). Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add src/MqttWire/Codec tests/MqttWire.Tests/Codec
git commit -m "Decode and encode the acknowledgements, the pings and DISCONNECT."
```

---

### Task 9: SUBSCRIBE, SUBACK, UNSUBSCRIBE, UNSUBACK and topic filters

**Files:**
- Create: `src/MqttWire/Codec/SubscribeCodec.cs`, `src/MqttWire/Codec/SubAckCodec.cs`, `src/MqttWire/Codec/UnsubscribeCodec.cs`, `src/MqttWire/Codec/UnsubAckCodec.cs`
- Modify: `src/MqttWire/Codec/Topics.cs`, `tests/MqttWire.Tests/Codec/KnownPackets.cs`, `tests/MqttWire.Tests/Codec/TopicsTests.cs`
- Test: `tests/MqttWire.Tests/Codec/SubscribeCodecTests.cs`, `tests/MqttWire.Tests/Codec/UnsubscribeCodecTests.cs`

**Interfaces:**
- Consumes: as Task 5, and `Topics` (Task 7).
- Produces:
  - `internal readonly ref struct SubscribePacket`: `ushort PacketIdentifier`, `PropertyBlock Properties`, `int Count`, `SubscriptionEnumerator GetSubscriptions()` over `internal readonly ref struct Subscription { ReadOnlySpan<byte> TopicFilter; MqttQoS MaximumQoS; bool NoLocal; bool RetainAsPublished; byte RetainHandling; }`.
  - `internal static class SubscribeCodec`: `TryDecode(ReadOnlySpan<byte> packet, out SubscribePacket subscribe)`; `int GetSize(ReadOnlySpan<byte> topicFilter)`; `int Write(Span<byte> destination, ushort packetIdentifier, ReadOnlySpan<byte> topicFilter, MqttQoS maximumQoS)` (one filter per SUBSCRIBE, options = the QoS only, no properties).
    - **Decoding:** identifier (0 → `ProtocolError`); properties (`Subscribe`); at least one entry (none → `ProtocolError`). For each entry: a filter string, then an options byte. Bits 6 and 7 set → `MalformedPacket`. QoS 3 → `ProtocolError`. Retain handling 3 → `ProtocolError`. No Local on a filter starting `$share/` → `ProtocolError`. **Filter validity is not checked here** (decision 1).
  - `internal readonly ref struct SubAckPacket`: `ushort PacketIdentifier`, `PropertyBlock Properties`, `ReadOnlySpan<byte> ReasonCodes`.
  - `internal static class SubAckCodec`: `TryDecode(ReadOnlySpan<byte> packet, out SubAckPacket subAck)` (identifier 0 → `ProtocolError`; properties (`SubAck`) required; at least one reason code); `int GetSize(int count, string? reasonString)`; `int Write(Span<byte> destination, ushort packetIdentifier, ReadOnlySpan<MqttReasonCode> reasonCodes, string? reasonString)`.
  - `internal readonly ref struct UnsubscribePacket`: `ushort PacketIdentifier`, `PropertyBlock Properties`, `int Count`, `TopicFilterEnumerator GetTopicFilters()` (yields `ReadOnlySpan<byte>`).
  - `internal static class UnsubscribeCodec`: `TryDecode` (identifier, properties (`Unsubscribe`), at least one filter); `GetSize(ReadOnlySpan<byte> topicFilter)`; `Write(Span<byte>, ushort packetIdentifier, ReadOnlySpan<byte> topicFilter)`.
  - `internal readonly ref struct UnsubAckPacket` and `internal static class UnsubAckCodec`: as SUBACK, with context `UnsubAck`.
  - `Topics.IsValidFilter`:
    - Not empty.
    - `#` only as the whole last level.
    - `+` only as a whole level.
    - `$share/{name}/{filter}`: the name is not empty and has no `+`, `#` or `/`, and the filter after it is valid and not empty.

- [ ] **Step 1: Write the failing tests**

```csharp
KnownPackets.Subscribe   = "82 09 00 01 00 00 03 61 2F 2B 01"     // id 1, "a/+", QoS 1
KnownPackets.SubAck      = "90 04 00 01 00 01"                    // id 1, granted QoS 1
KnownPackets.Unsubscribe = "A2 08 00 02 00 00 03 61 2F 2B"        // id 2, "a/+"
KnownPackets.UnsubAck    = "B0 04 00 02 00 00"                    // id 2, success
KnownPackets.SubscribeTwo= "82 0D 00 01 00 00 03 61 2F 2B 01 00 01 78 2D"   // second filter "x", options 0x2D

[Fact] public void The_known_subscribe_decodes_and_is_what_the_encoder_writes()     // Write(dest, 1, "a/+", AtLeastOnce)
[Fact] public void The_known_suback_decodes_and_is_what_the_encoder_writes()        // Write(dest, 1, [GrantedQoS1], null)
[Fact] public void The_known_unsubscribe_decodes_and_is_what_the_encoder_writes()   // Write(dest, 2, "a/+")
[Fact] public void The_known_unsuback_decodes_and_is_what_the_encoder_writes()      // Write(dest, 2, [Success], null)
[Fact] public void Every_subscription_and_its_options_are_read()
// SubscribeTwo: Count 2; ("a/+", AtLeastOnce, false, false, 0), ("x", AtLeastOnce, NoLocal, RetainAsPublished, RetainHandling 2)
[Theory]
[InlineData("82 07 00 01 00 00 01 78 40", MqttReasonCode.MalformedPacket)]   // reserved option bit
[InlineData("82 07 00 01 00 00 01 78 03", MqttReasonCode.ProtocolError)]     // QoS 3
[InlineData("82 07 00 01 00 00 01 78 30", MqttReasonCode.ProtocolError)]     // retain handling 3
[InlineData("82 03 00 01 00", MqttReasonCode.ProtocolError)]                 // no filters
[InlineData("82 10 00 01 00 00 0A 24 73 68 61 72 65 2F 67 2F 61 04", MqttReasonCode.ProtocolError)]   // No Local on "$share/g/a"
[InlineData("82 07 00 00 00 00 01 78 01", MqttReasonCode.ProtocolError)]     // identifier 0
public void A_wrong_subscribe_is_refused(string hex, MqttReasonCode expected)
[Fact] public void An_invalid_filter_is_still_decoded()       // "a#" decodes (the server answers 0x8F per filter, decision 1)
[Fact] public void A_subscription_identifier_is_reported()    // Properties.Contains(SubscriptionIdentifier) (the server refuses it with 0xA1)
[Fact] public void An_unsubscribe_without_filters_is_a_protocol_error()       // "A2 03 00 02 00"
[Fact] public void A_suback_and_unsuback_without_reason_codes_are_protocol_errors()
[Theory, MemberData(nameof(EveryPacketAtBothEdges))]          // the four types × ids 1 and 65,535 (Review Focus 4)
public void Identifiers_at_the_edges_round_trip(PacketType type, ushort id)
[Fact] public void Subscribe_and_unsubscribe_packets_mqttnet_encodes_decode()
// MqttSubscribePacket with two MqttTopicFilters (one with NoLocal, RetainAsPublished, RetainHandling.DoNotSendOnSubscribe),
// MqttSubAckPacket with two reason codes and a ReasonString, MqttUnsubscribePacket, MqttUnsubAckPacket
[Fact] public void Subscribe_and_unsubscribe_packets_this_codec_encodes_mqttnet_decodes()

// TopicsTests (filters)
[Theory]
[InlineData("a", true)] [InlineData("a/b", true)] [InlineData("+", true)] [InlineData("#", true)]
[InlineData("a/+/c", true)] [InlineData("a/#", true)] [InlineData("/", true)] [InlineData("+/+", true)]
[InlineData("$share/g/a/#", true)]
[InlineData("", false)] [InlineData("a#", false)] [InlineData("a/#/b", false)] [InlineData("a+", false)]
[InlineData("+a", false)] [InlineData("$share/", false)] [InlineData("$share/g", false)] [InlineData("$share//a", false)]
[InlineData("$share/g+/a", false)] [InlineData("$share/g/", false)]
public void A_topic_filter_is_valid_by_the_standards_rules(string filter, bool valid)
```


- [ ] **Step 2: Run them to see them fail**

Run: `dotnet test tests/MqttWire.Tests --filter "FullyQualifiedName~SubscribeCodecTests|FullyQualifiedName~UnsubscribeCodecTests|FullyQualifiedName~TopicsTests"`
Expected: build error, `SubscribeCodec` does not exist.

- [ ] **Step 3: Implement the four codecs, their views and enumerators, and `Topics.IsValidFilter`**

- [ ] **Step 4: Run them to see them pass** (same command). Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add src/MqttWire/Codec tests/MqttWire.Tests/Codec
git commit -m "Decode and encode SUBSCRIBE, UNSUBSCRIBE and their acknowledgements."
```

---

### Task 10: Streams cut at every byte, random input, and no allocations

Design 6.13, item 1, the last three bullets.

**Files:**
- Create: `tests/MqttWire.Tests/Codec/PacketStream.cs`, `tests/MqttWire.Tests/Codec/StreamTests.cs`, `tests/MqttWire.Tests/Codec/AllocationTests.cs`

**Interfaces:**
- Consumes: every codec, `KnownPackets` (Tasks 5 to 9).
- Produces (tests only):
  - `internal enum Reader { Server, Client }`.
  - `internal sealed record DecodedEntry(PacketType? Type, MqttReasonCode Status, string Hex)`. For a framing error, `Type` is null and `Status` is `MalformedPacket`, and it is the last entry.
  - `internal static class PacketStream`:
    - `List<DecodedEntry> DecodeAll(ReadOnlySpan<byte> stream, Reader reader)` and `List<DecodedEntry> DecodeInPieces(byte[] stream, IReadOnlyList<int> cuts, Reader reader)`.
    - The second accumulates the pieces in one growing buffer, as a read loop does, and on each piece frames and decodes every complete packet with `PacketHeader.TryReadPacket`.
    - Decoding dispatches by type as `reader` would. The server reads CONNECT, PUBLISH (`PacketSource.Client`), the four acks, SUBSCRIBE, UNSUBSCRIBE, PINGREQ and DISCONNECT. The client reads CONNACK, PUBLISH (`PacketSource.Server`), the four acks, SUBACK, UNSUBACK, PINGRESP and DISCONNECT. A packet the reader never reads, AUTH included, is `ProtocolError` (6.6).
    - It asserts progress on every call: either a packet was consumed or `NeedMoreData` was returned. It also asserts that framing calls never exceed the stream's length plus the number of pieces plus one.

- [ ] **Step 1: Write the tests**

```csharp
// StreamTests
static byte[] ClientStream  // Connect, PublishQoS0, PublishQoS1, PublishQoS2, PubAck, PubRecRefused, PubRel, PubCompNotFound, Subscribe, SubscribeTwo, Unsubscribe, PingReq, DisconnectWithReason
static byte[] ServerStream  // ConnAck, PublishQoS1, PublishQoS2, PubAckWithReason, PubRel, SubAck, UnsubAck, PingResp, DisconnectTakenOver

[Theory] [InlineData(Reader.Server)] [InlineData(Reader.Client)]
public void A_stream_cut_at_every_position_decodes_like_the_whole(Reader reader)
// for every k in 1..length-1: DecodeInPieces(stream, [k]) equals DecodeAll(stream); also with every piece one byte long;
// and DecodeAll gives Success for every packet of the reader's own stream

[Theory, MemberData(nameof(Seeds), 200)]
public void Random_joins_and_splits_decode_to_the_same_packets(int seed)
// Random(seed): 1–20 known packets of one reader's stream, joined; 0–10 random cut positions; DecodeInPieces == DecodeAll

[Theory, MemberData(nameof(Seeds), 500)]
public void Random_and_mutated_bytes_end_in_a_packet_or_an_error(int seed)
// Random(seed): either 0–64 random bytes, or a known stream with 1–3 bytes flipped, inserted or removed;
// random cuts; for both readers: no exception, progress asserted by PacketStream, DecodeInPieces == DecodeAll

[Theory]
[InlineData("30 FF FF FF 7F 00")]   // a header that promises 268 MB: NeedMoreData for ever, never an exception
public void Fixed_cases(string hex)  // a seed that ever fails is added here as its bytes, with "seed N" in a comment

// AllocationTests
[Fact] public void Decoding_every_known_packet_allocates_nothing()
// warm up once; then, between two GC.GetAllocatedBytesForCurrentThread() readings: TryReadPacket and TryDecode
// every KnownPackets entry for its reader, read every property, enumerate every user property, subscription and filter.
// Assert the difference is 0.
[Fact] public void Encoding_every_known_packet_allocates_nothing()
// the same around GetSize and Write for every packet the encoders write, into one preallocated 4 KiB buffer
// (the fields, strings and arrays are built before the first reading)
```

- [ ] **Step 2: Run them**

Run: `dotnet test tests/MqttWire.Tests --filter "FullyQualifiedName~StreamTests|FullyQualifiedName~AllocationTests"`
Expected: all pass. These tests check code that already exists, so a failure is a real fault in Tasks 2 to 9. Find it with superpowers:systematic-debugging, fix it test-first in the owning codec's test file, add the failing input to `Fixed_cases` if a seed found it, and run again.

- [ ] **Step 3: Check that the tests can fail**

Make `PacketHeader.TryReadPacket` report `Done` one byte early, and run the command of step 2.
Expected: `A_stream_cut_at_every_position_decodes_like_the_whole` fails. Undo the change.

Then add `_ = new byte[1];` to `PublishCodec.TryDecode` and run again.
Expected: `Decoding_every_known_packet_allocates_nothing` fails. Undo the change.

- [ ] **Step 4: Run the whole suite**

Run: `dotnet test`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add tests/MqttWire.Tests/Codec
git commit -m "Test the codec on streams cut at every byte, on random input, and for allocations."
```

---

### Task 11: Microbenchmarks

**Files:**
- Create: `bench/MqttWire.Microbench/MqttWire.Microbench.csproj`, `bench/MqttWire.Microbench/Program.cs`, `bench/MqttWire.Microbench/CodecBenchmarks.cs`
- Modify: `MqttWire.slnx` (folder `/bench/`)

**Interfaces:**
- Consumes: the codec through `InternalsVisibleTo` (Task 1).
- Produces: `[MemoryDiagnoser] public class CodecBenchmarks` with the benchmarks below. `Program` is `BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);`.

| Benchmark | What it does |
|---|---|
| `DecodePublishQoS1` | `PublishCodec.TryDecode` of a QoS 1 PUBLISH with a 1 KiB payload |
| `EncodePublishQoS1` | `PublishCodec.Write` of the same into a preallocated buffer |
| `DecodeConnect` | `ConnectCodec.TryDecode` of Task 5's 26-byte CONNECT, written out in the benchmark (`KnownPackets` is in the test project), plus reading `ReceiveMaximum` |
| `FrameStream` | `PacketHeader.TryReadPacket` across 100 small packets in one buffer |
| `DecodeSubscribe` | `SubscribeCodec.TryDecode` of a two-filter SUBSCRIBE and enumerating it |

The project is an `Exe`, `IsPackable` false, references `BenchmarkDotNet` and `src/MqttWire`. Its inputs are built in `[GlobalSetup]`, so they are not measured.

- [ ] **Step 1: Write the project and the benchmarks**

- [ ] **Step 2: Build it with the solution**

Run: `dotnet build -c Release`
Expected: 0 warnings, 0 errors.

- [ ] **Step 3: Run it once**

Run: `dotnet run -c Release --project bench/MqttWire.Microbench -- --filter "*" --job short`
Expected: five rows; the `Allocated` column shows `-` (nothing) for every row. Put the table in the pull request's description. The results stay out of the repository: `BenchmarkDotNet.Artifacts/` is already ignored.

- [ ] **Step 4: Commit**

```bash
git add bench MqttWire.slnx
git commit -m "Add codec microbenchmarks with BenchmarkDotNet."
```

---

## Done when

- `dotnet build -c Release` has 0 warnings, and `dotnet test` passes, locally and in CI.
- Every packet of design 6.4 has a known byte sequence, decoded and encoded, and a cross-check with MQTTnet in both directions.
- Every form-level row of 6.7 has a test: allowed in its packet, at most once, of the right type, with the forbidden values.
- The policy rows (decision 1) are exposed and tested as exposed. They are listed below for steps 2 and 3, which apply them.
- The stream, random and allocation tests of 6.13 pass, and each has been seen to fail on a planted fault.

**Handed to steps 2 and 3** (decision 1): refuse a Will with 0x87, an authentication method with 0x8C, an empty or over-256-byte client id with 0x85, a maximum packet size under 4,096 with 0x95, a topic alias with 0x94, and a subscription identifier in SUBSCRIBE with 0xA1. Answer an invalid topic filter (`Topics.IsValidFilter`) with 0x8F in SUBACK. Close the client on a CONNACK with session present. Give `PublishCodec` only topic bytes that are valid MQTT UTF-8: the encoder checks only that the topic is not empty and holds no wildcard, no U+0000 and at most 65,535 bytes. Validate and cap host user properties and reason strings at the API boundary. When `TryReadPacket` returns `NeedMoreData`, have the read loop check `header.TotalLength` so an oversized packet is refused before it is buffered.
