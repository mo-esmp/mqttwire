# Two target frameworks: .NET 10 and .NET 11

A change to how MqttWire is built and tested, for issue #47. It amends section 4
("Constraints", its first item) and section 10, item 12 ("Target framework `net10.0` only, for
now") of [the design](2026-11-19-mqttwire-design.md), which stays the authority for everything
else.

This is for both sides. `src/MqttWire/Link` and `src/MqttWire/Codec` are shared by the client
and the server, and both packages get the second target. No source file of either package
changes: the protocol, the public API and every limit and deadline are the same on both
targets.

Status: written on 2026-11-27 with the change itself; the owner has not approved it yet.

## 1. Intent

Both packages targeted `net10.0` only. A .NET 11 application could already use them, since a
`net10.0` assembly runs on the .NET 11 runtime. But nothing was built against .NET 11 or tested
on it: the allocation tests, the interop tests and the Blazor WebAssembly client had only ever
run on .NET 10, `MqttWire.Server` was compiled against ASP.NET Core 10 only, and neither package
could use an API of .NET 11 where it would be faster or allocate less.

So each package now carries a build for each framework, and everything that is tested is tested
on both runtimes. `net10.0` stays: .NET 10 is the LTS release, and dropping it would be a
breaking change.

## 2. What changes

**The targets.** `Directory.Build.props` says `<TargetFrameworks>net10.0;net11.0</TargetFrameworks>`
for every project: the two packages, both test projects, the test host, the sample and the
benchmarks. One rule for all is simpler than a list of exceptions, and each of them has a use
for both: the tests and the test host must run on both, the sample is the only Blazor
WebAssembly build of the client that CI makes, and the benchmarks are how the two runtimes are
compared (section 5).

**The SDK.** `global.json` asks for the .NET 11 SDK (`11.0.100`, `latestFeature`), which builds
both targets. The .NET 10 runtime and its ASP.NET Core are still needed to run the `net10.0`
tests and test host.

**The packages.** `MqttWire` and `MqttWire.Server` each have `lib/net10.0` and `lib/net11.0`. A
.NET 10 application gets the first and a .NET 11 application the second, chosen by NuGet.
`MqttWire.Server` depends on exactly its own version of `MqttWire` in the dependency group of
each target, and the check that reads the packed manifest back (`CheckMqttWireIsPinned`) now
fails when any group lacks the dependency or has another version.

**The versions of what follows the framework.** `Directory.Packages.props` has one version per
target for `Microsoft.Extensions.TimeProvider.Testing` (the unit tests' clock) and for
`Microsoft.AspNetCore.Components.WebAssembly` and its `.Server` (the sample). Every other
package has one version for both. Neither package of MqttWire gains a dependency.

## 3. Decisions

1. **The language stays C# 14 for both targets.** C# 15 is the default for `net11.0`, but it is
   supported on .NET 11 and later only, and one source is compiled for both targets. Nothing in
   this change needs a C# 15 feature. `LangVersion` moves when `net10.0` goes.
2. **Runtime-async stays off.** .NET 11 can have the runtime, instead of the compiler, make the
   state machine of an `async` method (`<Features>runtime-async=on</Features>`); it is opt-in,
   and the `net11.0` builds were checked to still hold the compiler's state machines (21 in
   `MqttWire`, 13 in `MqttWire.Server`, as in the `net10.0` builds). It is not turned on here,
   for three reasons. It does not work in WebAssembly, where the client must run (design 4). The
   methods of the hot paths that often suspend use `PoolingAsyncValueTaskMethodBuilder`, and
   the .NET 11 runtime notes say that pooled methods are left out of runtime-async anyway. And it would be the first code path that
   differs between the targets, which needs a measurement of its own (item 3). Turning it on
   for `MqttWire.Server` alone is a possible later change.
3. **No code path for one target only, yet.** There is no `#if NET11_0_OR_GREATER` in either
   package: both builds come from the same source and the same IL, compiled against two sets of
   reference assemblies. A later change that adds one says so in its design, is measured before
   and after with BenchmarkDotNet on both runtimes (AGENTS.md, "Hot Paths"), and keeps the
   `net10.0` path tested.
4. **The .NET 11 runtime and ASP.NET Core are used as they come.** Their own libraries are
   compiled with runtime-async, which changes how the WebSocket and Kestrel code beneath
   MqttWire suspends. That is the framework's business; what it does to MqttWire's numbers is
   measured (section 5), not configured.

## 4. Tests and CI

Both test projects run on both targets: `dotnet test -c Release` runs each of them twice. The
allocation tests (`*AllocationTests`) must hold on each runtime, because a framework change can
add or remove an allocation on a hot path; they hold on both without a change.

CI installs the .NET 11 SDK and, for its runtime, 10.0.x. The `dotnet` job builds, tests and
packs for both targets. The `interop-js` job is a matrix over the two targets: it builds the
test host for one and runs MQTT.js against it, the host's build being chosen by the environment
variable `MQTTWIRE_TEST_FRAMEWORK` (`host.mjs` runs the `net11.0` build when it is not set). The
`publish` job needs every leg of both.

`dotnet run` does not choose between two targets by itself, so the commands that run the sample,
the test host and the benchmarks name one with `-f`.

## 5. Measurement

Issue #47 asks for one BenchmarkDotNet run of the client and the codec on both runtimes, and
AGENTS.md for a report that keeps every number. The report is
[.NET 11 against .NET 10](../bench/2026-11-net11.md): the codec, the client and the server's
delivery encoding in one BenchmarkDotNet run with a job for each runtime, the end-to-end client
over loopback with the probe, and the idle-memory benchmark of design 6.14 on each runtime.

`bench/MqttWire.Microbench` takes `--both-runtimes` for that run. BenchmarkDotNet 0.15.8 does
not know .NET 11, so the program makes the two jobs itself, each with a toolchain, and has to
be started on .NET 10 (`-f net10.0`) with or without that option; started on .NET 11 it says
so and ends. This goes when a BenchmarkDotNet that knows `net11.0` is taken.

## 6. Cost

- Build and CI time: the build makes every assembly twice and every test runs twice. Locally a
  Release build of the whole solution takes 20 to 30 seconds and the tests about a minute.
- Each package grows by one more assembly and its documentation file, about twice the size.
- Nothing changes per connection or per operation in MqttWire itself. What the .NET 11 runtime
  changes is in the report; the largest is that ASP.NET Core 11 holds about 2,000 bytes more for
  an idle WebSocket connection, beneath MqttWire.
- The .NET 11 SDK's compiler now builds the `net10.0` assemblies too, and emits slightly
  different IL for 12 methods of `MqttWire` and 4 of `MqttWire.Server`. The report measures the
  `net10.0` build before and after: nothing is slower, and nothing allocates more.

## 7. Not in this change

- A .NET Standard 2.1 build of the client: issue #42.
- Any use of an API that only .NET 11 has, and runtime-async for the server (section 3).
- Native AOT and trimming annotations: not planned (roadmap).
