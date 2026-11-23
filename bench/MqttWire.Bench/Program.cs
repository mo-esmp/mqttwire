using System.Globalization;

namespace MqttWire.Bench;

/// <summary>
/// The idle-memory benchmark of design 6.14: what an idle connection costs on a bare ASP.NET Core WebSocket
/// endpoint (the floor) and on MqttWire with a handler that does nothing. It is one program with two roles in two
/// processes, so that the memory of the clients is never measured with the server's:
/// <c>server</c> holds the connections and measures itself, and <c>run</c> starts servers, opens the connections
/// and reads the figures.
/// </summary>
internal static class Program
{
    /// <summary>
    /// Runs the role the first argument names.
    /// </summary>
    /// <param name="args">
    /// <c>server [--port &lt;p&gt;] [--tls]</c>, or
    /// <c>run [--small &lt;n&gt;] [--large &lt;n&gt;] [--tls] [--dumps &lt;folder&gt;]</c>.
    /// </param>
    /// <returns>
    /// 0 when the role ran to its end; 1 when the arguments name no role, or the run found something that makes
    /// its figures untrustworthy.
    /// </returns>
    public static async Task<int> Main(string[] args)
    {
        string role = args.Length > 0 ? args[0] : string.Empty;
        switch (role)
        {
            case "server":
                await ServerRole.RunAsync(GetInt32(args, "--port", fallback: 0), HasFlag(args, "--tls"));
                return 0;
            case "run":
                return await RunRole.RunAsync(
                    GetInt32(args, "--small", fallback: 3_000),
                    GetInt32(args, "--large", fallback: 12_000),
                    HasFlag(args, "--tls"),
                    GetString(args, "--dumps"));
            default:
                Console.Error.WriteLine("Usage: MqttWire.Bench server [--port <p>] [--tls]");
                Console.Error.WriteLine("       MqttWire.Bench run [--small <n>] [--large <n>] [--tls] [--dumps <folder>]");
                return 1;
        }
    }

    /// <summary>
    /// Tells whether a flag is among the arguments.
    /// </summary>
    /// <param name="args">
    /// The command line.
    /// </param>
    /// <param name="name">
    /// The flag, such as <c>--tls</c>.
    /// </param>
    /// <returns>
    /// <see langword="true"/> when it is.
    /// </returns>
    private static bool HasFlag(string[] args, string name) => Array.IndexOf(args, name) >= 0;

    /// <summary>
    /// Reads the text that follows a named argument.
    /// </summary>
    /// <param name="args">
    /// The command line.
    /// </param>
    /// <param name="name">
    /// The argument's name, such as <c>--dumps</c>.
    /// </param>
    /// <returns>
    /// The text, or <see langword="null"/> when the argument is not given.
    /// </returns>
    /// <exception cref="FormatException">
    /// The argument is given with nothing behind it.
    /// </exception>
    private static string? GetString(string[] args, string name)
    {
        int index = Array.IndexOf(args, name);
        if (index < 0)
        {
            return null;
        }

        return index + 1 < args.Length ? args[index + 1] : throw new FormatException($"{name} needs a value.");
    }

    /// <summary>
    /// Reads the whole number that follows a named argument.
    /// </summary>
    /// <param name="args">
    /// The command line.
    /// </param>
    /// <param name="name">
    /// The argument's name, such as <c>--port</c>.
    /// </param>
    /// <param name="fallback">
    /// The value when the argument is not given.
    /// </param>
    /// <returns>
    /// The number, or <paramref name="fallback"/>.
    /// </returns>
    /// <exception cref="FormatException">
    /// The argument is given without a whole number behind it.
    /// </exception>
    private static int GetInt32(string[] args, string name, int fallback)
    {
        int index = Array.IndexOf(args, name);
        if (index < 0)
        {
            return fallback;
        }

        if (index + 1 >= args.Length)
        {
            throw new FormatException($"{name} needs a whole number.");
        }

        return int.Parse(args[index + 1], CultureInfo.InvariantCulture);
    }
}
