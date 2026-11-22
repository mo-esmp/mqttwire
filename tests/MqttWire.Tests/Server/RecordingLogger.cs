using Microsoft.Extensions.Logging;
using MqttWire.Server;

namespace MqttWire.Tests.Server;

/// <summary>
/// A logger for the server that keeps every entry it is given, so a test can assert what was logged. Every level
/// is enabled. Safe to log to from several threads at once.
/// </summary>
internal sealed class RecordingLogger : ILogger<MqttWireServer>
{
    /// <summary>
    /// Guards <see cref="Entries"/> while an entry is added.
    /// </summary>
    private readonly Lock _gate = new();

    /// <summary>
    /// Gets every entry logged, oldest first, with its message formatted.
    /// </summary>
    public List<(LogLevel Level, EventId Id, string Message, Exception? Exception)> Entries { get; } = [];

    /// <summary>
    /// Starts no scope: the logger does not record scopes.
    /// </summary>
    /// <typeparam name="TState">
    /// The type of the scope's state.
    /// </typeparam>
    /// <param name="state">
    /// The scope's state, ignored.
    /// </param>
    /// <returns>
    /// Always <see langword="null"/>.
    /// </returns>
    public IDisposable? BeginScope<TState>(TState state)
        where TState : notnull => null;

    /// <summary>
    /// Tells that every level is logged.
    /// </summary>
    /// <param name="logLevel">
    /// The level asked about.
    /// </param>
    /// <returns>
    /// Always <see langword="true"/>.
    /// </returns>
    public bool IsEnabled(LogLevel logLevel) => true;

    /// <summary>
    /// Formats the entry and keeps it.
    /// </summary>
    /// <typeparam name="TState">
    /// The type of the entry's state.
    /// </typeparam>
    /// <param name="logLevel">
    /// The entry's level.
    /// </param>
    /// <param name="eventId">
    /// The entry's event id.
    /// </param>
    /// <param name="state">
    /// The entry's state.
    /// </param>
    /// <param name="exception">
    /// The exception the entry carries, or <see langword="null"/>.
    /// </param>
    /// <param name="formatter">
    /// Formats the state and the exception into the message.
    /// </param>
    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
        Func<TState, Exception?, string> formatter)
    {
        ArgumentNullException.ThrowIfNull(formatter);
        string message = formatter(state, exception);
        lock (_gate)
        {
            Entries.Add((logLevel, eventId, message, exception));
        }
    }
}
