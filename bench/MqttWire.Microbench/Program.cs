﻿using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Reports;
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
//
// The exit code says whether the run measured what was asked for, so that a script that collects numbers cannot take
// a run without them for a comparison. It is 1 when the run made no report at all (a filter that matches nothing,
// for one), when a benchmark has no result (its project could not be generated or built, or it failed), and, with
// --both-runtimes, when either runtime has no result. BenchmarkDotNet itself ends such a run with 0. Asking for
// information only (--list, --info, --help, --version) measures nothing and ends with 0.
const string BothRuntimes = "--both-runtimes";
const string Net10Job = ".NET 10";
const string Net11Job = ".NET 11";

if (Environment.Version.Major != 10)
{
    await Console.Error.WriteLineAsync(
        $"BenchmarkDotNet 0.15.8 cannot run from .NET {Environment.Version.Major}: start the program with -f net10.0, and add {BothRuntimes} to measure on .NET 11 as well.").ConfigureAwait(false);
    return 1;
}

bool bothRuntimes = args.Contains(BothRuntimes, StringComparer.Ordinal);
IConfig? config = null;
if (bothRuntimes)
{
    args = [.. args.Where(argument => !string.Equals(argument, BothRuntimes, StringComparison.Ordinal))];
    config = DefaultConfig.Instance
        .AddJob(Job.Default.WithToolchain(CsProjCoreToolchain.NetCoreApp10_0).WithId(Net10Job))
        .AddJob(Job.Default
            .WithToolchain(CsProjCoreToolchain.From(new NetCoreAppSettings("net11.0", null, ".NET 11.0")))
            .WithId(Net11Job));
}

Summary[] summaries = [.. BenchmarkSwitcher.FromAssembly(typeof(CodecBenchmarks).Assembly).Run(args, config)];

string[] informational = ["--list", "--info", "--help", "-h", "-?", "--version"];
if (args.Any(argument => informational.Contains(argument, StringComparer.OrdinalIgnoreCase)))
{
    return 0;
}

BenchmarkReport[] reports = [.. summaries.SelectMany(summary => summary.Reports)];
List<string> problems = [];
if (reports.Length == 0)
{
    problems.Add("the run made no report: no benchmark was chosen, or none could be run");
}

foreach (Summary summary in summaries.Where(summary => summary.HasCriticalValidationErrors))
{
    problems.Add($"{summary.Title} has critical validation errors");
}

foreach (BenchmarkReport report in reports.Where(report => !report.Success || report.ResultStatistics is null))
{
    problems.Add($"{report.BenchmarkCase.DisplayInfo} has no result");
}

if (bothRuntimes)
{
    foreach (string job in (string[])[Net10Job, Net11Job])
    {
        if (!reports.Any(report => report.Success
            && report.ResultStatistics is not null
            && string.Equals(report.BenchmarkCase.Job.Id, job, StringComparison.Ordinal)))
        {
            problems.Add($"no benchmark has a result on {job}");
        }
    }
}

if (problems.Count == 0)
{
    return 0;
}

await Console.Error.WriteLineAsync("The run did not measure everything that was asked for:").ConfigureAwait(false);
foreach (string problem in problems)
{
    await Console.Error.WriteLineAsync($"  {problem}").ConfigureAwait(false);
}

return 1;
