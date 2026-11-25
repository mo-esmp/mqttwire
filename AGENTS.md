# Agent Guidelines

MqttWire is a lean MQTT 5 connection server and client for .NET 10 (C# 14), over WebSockets. It is
a connection server, not a broker: the host decides who connects, what a subscription means and
where a message goes. [The design](docs/specs/2026-11-19-mqttwire-design.md) is the authority.

## Map

| Path | What it holds |
|---|---|
| `src/MqttWire` | Package `MqttWire`: the client, the reconnecting client, the internal codec (`Codec/`) and the link (`Link/`). No dependencies; runs in Blazor WebAssembly. |
| `src/MqttWire.Server` | Package `MqttWire.Server`: the connection server for ASP.NET Core. Uses `MqttWire`'s internals and depends on exactly its own version of it. |
| `tests/MqttWire.Tests` | Unit tests over the in-memory link. No network. |
| `tests/MqttWire.Interop.Tests` | Loopback tests against MQTTnet, and of the server's WebSocket endpoint. |
| `tests/interop-js`, `tests/MqttWire.TestHost` | The MQTT.js interop and the host it runs against. |
| `bench/MqttWire.Microbench` | BenchmarkDotNet: the codec, the client, the server's delivery encoding, and comparisons with MQTTnet. |
| `bench/MqttWire.Bench` | The idle-memory benchmark of design 6.14, with a server role and a client role in two processes. |
| `bench/MqttWire.Probe` | The interleaved before/after probe; its `Program.cs` remarks give the build commands. |
| `samples/GroupChat` | Shared topics through the host, with a Blazor WebAssembly client. |
| `docs/specs`, `docs/plans`, `docs/bench` | Designs and their reviews, implementation plans, measurement reports. |

Large types are partial classes split by concern: `MqttWireClient.ReadLoop.cs`,
`MqttWireConnection.Close.cs` and so on. Put new code in the part it belongs to.

## Commands

```
dotnet build -c Release
dotnet test -c Release
dotnet test tests/MqttWire.Tests -c Release --filter "FullyQualifiedName~WebSocketLinkTests"
```

The build treats warnings as errors and enforces code style (`EnforceCodeStyleInBuild`), with the
Meziantou, Sonar, Roslynator and xUnit analyzers, so a style slip fails the build. For the MQTT.js
interop, build `tests/MqttWire.TestHost`, then run `npm ci` and `npm test` in `tests/interop-js`.

## Design

* Follow the .NET Framework Design Guidelines and library-design best practices.
* Performance and memory come first: prefer the faster, lower-allocation design when it conflicts with convenience.
* `internal` by default; a type becomes public only when a consumer needs it. Tests, benchmarks and `MqttWire.Server` reach internals through `InternalsVisibleTo`.
* Classes are `sealed` unless designed for inheritance. A change to a shipped public API is a breaking change.
* No new package dependency in `MqttWire` or `MqttWire.Server` without the owner's approval; versions are pinned centrally in `Directory.Packages.props`.
* **The client and the server share `src/MqttWire/Link` and `src/MqttWire/Codec`.** A change made for one side changes the other: say in the design which side it is for, keep the other side's behaviour unless the design changes it, and run the interop tests.
* An internal change can break `MqttWire.Server`, so both packages always ship together at one version.

## Hot Paths

The codec, framing, the link and the read/write loops:

* Allocate nothing: no LINQ, closures, boxing, string concatenation, `params` arrays or `ToArray()`.
* `Span<T>`/`ReadOnlySpan<T>`, `stackalloc` for small fixed buffers, `ArrayPool<byte>` for large ones. Allocation tests (`*AllocationTests`) enforce this.
* An async method that often suspends uses `PoolingAsyncValueTaskMethodBuilder`, and one that usually completes at once returns the inner `ValueTask` without a state machine.
* A performance claim needs a BenchmarkDotNet run with `[MemoryDiagnoser]`, before and after.

## Errors

* Malformed network input is an error result, never an exception. Exceptions are only for caller bugs.
* Argument checks use the throw helpers: `ArgumentNullException.ThrowIfNull`, `ArgumentOutOfRangeException.ThrowIfNegative` and the like.

## Code Style

Follow `.editorconfig`; these supplement it:

* File-scoped namespaces; a file's namespace is its full folder path (`MqttWire.Codec`). Project-wide `global using`s only in the project's `GlobalUsings.cs`.
* Primary constructors for services; constructor injection only.
* Name the type on the left, infer it on the right: never `var`, and target-typed `new` (`MqttWriter writer = new(destination);`). Both are build errors. Where the left names no type, spell it out (`return new ConnAckPacket();`, `List<string> filters = [];`).
* Private static fields take `s_`. Expression-bodied members for one-liners; braces on every control statement; guard clauses and early return; a blank line after each property when a class has more than one.
* Async: methods end in `Async`; `ValueTask` on hot paths; `ConfigureAwait(false)` in library code; `CancellationToken` is the last parameter; never `async void`, `.Result` or `.Wait()`.
* Nullable reference types are on. No `!` without a comment saying why it is safe.
* All code is documented: every type and every member has XML documentation, whatever its accessibility (public, internal, private) and whichever project it is in (`src`, `tests`, `bench`). A test's summary says in one sentence what it proves. Say what the signature cannot: what a `false` or an error result means, what throws and why, whether a failed call changes state. Avoid one-line XML doc comments; use the multi-line form:

  ```csharp
  /// <summary>
  /// This is a sample
  /// </summary>
  ```

## Testing

* xUnit + Shouldly, with hand-written fakes such as the in-memory `MemoryLink` and `ScriptedSendWebSocket`; no mocking library. AAA comments in every test.
* Test names follow `MethodName_StateUnderTest_ExpectedBehavior`, e.g. `TryRead_ValueCutShort_ReturnsNeedMoreData`.
* Protocol code is tested against known byte sequences from the standard. MQTTnet is used only in tests and benchmarks, as an independent cross-check.
* Unit tests (`MqttWire.Tests`) use no network, sockets or real broker. Loopback sockets belong in `MqttWire.Interop.Tests`.
* Every wait in a test is bounded (`.Within("what")` from `BoundedWaits`, or `WaitAsync`), so a broken run fails instead of hanging the suite. No outcome may depend on the bound.
* Allocation tests measure with `GC.GetAllocatedBytesForCurrentThread()`. The client's and the server's run in a collection with parallelization disabled, so that other tests do not disturb them.
* A fix for a bug comes with a test that fails without it.

## Benchmarks

* Before and after are measured on the same machine in the same session; the probe interleaves them, and BenchmarkDotNet runs are minutes apart on a drifting VM.
* To compare with an older client, build it from a worktree under the assembly name `MqttWire.Before` and reference it under the extern alias `before`. Put the `using` aliases inside the file-scoped namespace: outside it, `MqttWireClient` resolves to the current client in the enclosing `MqttWire` namespace. Check that the built before contestant really holds `MqttWire.Before` types.
* A report in `docs/bench/YYYY-MM-topic.md` names the commits measured, the machine, the method and the run counts, and keeps every number it relies on.

## Build, CI & Docs

* Before every commit: `dotnet build -c Release` with 0 warnings, and `dotnet test` green.
* CI runs on Linux (`.github/workflows/ci.yml`): the build, both test projects, both packs and the MQTT.js interop. A green run on Windows is not enough, since socket timing differs; check CI after every push.
* `docs/specs` is the authority. Code that has to diverge from a spec updates the spec in the same pull request. A change to an approved design is written as a dated amendment design that names the section it amends.
* Keep these in step with the code in the same pull request:
  * `CHANGELOG.md`: an entry under `## Unreleased`, by package, for every change a user can see.
  * `docs/mqtt-support.md`: packets, properties, limits and deadlines, whenever protocol behaviour changes.
  * `README.md`: whenever usage changes.

## Branches, Commits & Pull Requests

* Branch: `feature/<issue-number>-<brief-issue-title>`, kebab-case, e.g. `feature/233-attachment-entity-options-switch`.
* Commit message: one imperative sentence ending with a period, e.g. `Add the codec's field reader and writer.` No `Co-Authored-By` trailer.
* Commit only the files of the change; leave unrelated local files untracked.
* Pull request title: `#<IssueNumber> - <IssueTitle>.` The squash commit on `main` takes it, with the PR number appended. The description links the issue with `Closes #<IssueNumber>`.
* Answer each review comment in its thread. Resolve the thread when the change is pushed; leave it open when the answer is a disagreement or a deferral to another issue.
* Tags, GitHub releases and publishing to nuget.org are the owner's call. Ask first.
