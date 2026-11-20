# Agent Guidelines

## Design

* Follow the .NET Framework Design Guidelines and library-design best practices.
* Performance and memory come first: prefer the faster, lower-allocation design when it conflicts with convenience.
* `internal` by default; a type becomes public only when a consumer needs it. Tests and sibling packages reach internals through `InternalsVisibleTo`.
* Classes are `sealed` unless designed for inheritance. A change to a shipped public API is a breaking change.
* No new package dependency in `MqttWire` without the owner's approval; versions are pinned centrally in `Directory.Packages.props`.

## Hot Paths

The codec, framing and the read/write loops:

* Allocate nothing: no LINQ, closures, boxing, string concatenation, `params` arrays or `ToArray()`.
* `Span<T>`/`ReadOnlySpan<T>`, `stackalloc` for small fixed buffers, `ArrayPool<byte>` for large ones. Allocation tests enforce this.
* A performance claim needs a BenchmarkDotNet run with `[MemoryDiagnoser]`, before and after.

## Errors

* Malformed network input is an error result, never an exception. Exceptions are only for caller bugs.
* Argument checks use the throw helpers: `ArgumentNullException.ThrowIfNull`, `ArgumentOutOfRangeException.ThrowIfNegative` and the like.

## Code Style

Follow `.editorconfig`; these supplement it:

* File-scoped namespaces; a file's namespace is its full folder path (`MqttWire.Codec`). Project-wide `global using`s only in the project's `GlobalUsings.cs`.
* Primary constructors for services; constructor injection only.
* Name the type on the left, infer it on the right: never `var`, and target-typed `new` (`MqttWriter writer = new(destination);`) — both are build errors. Where the left names no type, spell it out (`return new ConnAckPacket();`, `List<string> filters = [];`).
* Private static fields take `s_`. Expression-bodied members for one-liners; braces on every control statement; guard clauses and early return; a blank line after each property when a class has more than one.
* Async: methods end in `Async`; `ValueTask` on hot paths; `ConfigureAwait(false)` in library code; `CancellationToken` is the last parameter; never `async void`, `.Result` or `.Wait()`.
* Nullable reference types are on. No `!` without a comment saying why it is safe.
* Public APIs must have XML documentation. Avoid one-line XML doc comments; use the multi-line form:

  ```csharp
  /// <summary>
  /// This is a sample
  /// </summary>
  ```

## Testing

* xUnit + Shouldly + NSubstitute; AAA comments in every test.
* Test names follow `MethodName_StateUnderTest_ExpectedBehavior`, e.g. `TryRead_ValueCutShort_ReturnsNeedMoreData`.
* Protocol code is tested against known byte sequences from the standard. MQTTnet is used only in tests, as an independent cross-check.
* Unit tests use no network, sockets or real broker.

## Build & Docs

* Before every commit: `dotnet build -c Release` with 0 warnings, and `dotnet test` green.
* `docs/specs` is the authority. Code that has to diverge from a spec updates the spec in the same pull request.

## Branches, Commits & Pull Requests

* Branch: `feature/<issue-number>-<brief-issue-title>`, kebab-case, e.g. `feature/233-attachment-entity-options-switch`.
* Commit message: one imperative sentence ending with a period, e.g. `Add the codec's field reader and writer.`
* Pull request title: `#<IssueNumber> - <IssueTitle>.` — the squash commit on `main` takes it, with the PR number appended. The description links the issue with `Closes #<IssueNumber>`.
