using BenchmarkDotNet.Running;
using MqttWire.Microbench;

BenchmarkSwitcher.FromAssembly(typeof(CodecBenchmarks).Assembly).Run(args);
