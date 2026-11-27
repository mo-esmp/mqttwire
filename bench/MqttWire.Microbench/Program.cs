﻿using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Running;
using BenchmarkDotNet.Toolchains.CsProj;
using BenchmarkDotNet.Toolchains.DotNetCli;
using MqttWire.Microbench;

// Runs the benchmarks BenchmarkDotNet's own arguments choose, on .NET 10:
//
//   dotnet run -c Release -f net10.0 --project bench/MqttWire.Microbench -- --filter "*CodecBenchmarks*"
//
// With --both-runtimes as well, every chosen benchmark runs on .NET 10 and on .NET 11 in the one run, so that the two
// are measured minutes apart at most and in the same session. BenchmarkDotNet builds the benchmarks once for each
// target, and so takes the build of MqttWire for that target: the comparison is of the two builds the packages carry,
// each on its own runtime.
//
// BenchmarkDotNet 0.15.8 does not know .NET 11: it has no name for it in its own --runtimes, and its check of the
// installed SDKs throws for a runtime it cannot name. That is why the two jobs are made here, each with a toolchain
// instead of a runtime, and why the program itself has to run on .NET 10 (-f net10.0) in either case: a job without a
// runtime is checked against the program's own. The project still builds for net11.0, because the benchmarks that
// BenchmarkDotNet builds for .NET 11 reference it.
const string BothRuntimes = "--both-runtimes";

if (Environment.Version.Major != 10)
{
    await Console.Error.WriteLineAsync(
        $"BenchmarkDotNet 0.15.8 cannot run from .NET {Environment.Version.Major}: start the program with -f net10.0, and add {BothRuntimes} to measure on .NET 11 as well.").ConfigureAwait(false);
    return 1;
}

IConfig? config = null;
if (args.Contains(BothRuntimes, StringComparer.Ordinal))
{
    args = [.. args.Where(argument => !string.Equals(argument, BothRuntimes, StringComparison.Ordinal))];
    config = DefaultConfig.Instance
        .AddJob(Job.Default.WithToolchain(CsProjCoreToolchain.NetCoreApp10_0).WithId(".NET 10"))
        .AddJob(Job.Default
            .WithToolchain(CsProjCoreToolchain.From(new NetCoreAppSettings("net11.0", null, ".NET 11.0")))
            .WithId(".NET 11"));
}

BenchmarkSwitcher.FromAssembly(typeof(CodecBenchmarks).Assembly).Run(args, config);
return 0;
