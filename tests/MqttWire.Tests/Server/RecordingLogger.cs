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
    /// Completes when an entry with <see cref="_awaitedEventId"/> is logged, or <see langword="null"/> when no one
    /// waits.
    /// </summary>
    private TaskCompletionSource? _awaited;

    /// <summary>
    /// The event id <see cref="_awaited"/> waits for.
    /// </summary>
    private int _awaitedEventId;

    /// <summary>
    /// Gets or sets an event id whose entries are recorded and then answered with an exception, as a logging provider
    /// that fails does; 0 for none.
    /// </summary>
    public int ThrowForEventId { get; set; }

    /// <summary>
    /// Gets every entry logged, oldest first, with its message formatted.
    /// </summary>
    public List<(LogLevel Level, EventId Id, string Message, Exception? Exception)> Entries { get; } = [];

    /// <summary>
    /// Waits until an entry with an event id is logged, for entries written on another thread. One waiter at a time.
    /// </summary>
    /// <param name="eventId">
    /// The event id to wait for.
    /// </param>
    /// <returns>
    /// A task that completes, never inline, once such an entry is in <see cref="Entries"/>; at once when one is
    /// there already. It fails with <see cref="TimeoutException"/> when none is logged within
    /// <see cref="BoundedWaits.Limit"/>.
    /// </returns>
    public Task WhenLoggedAsync(int eventId)
    {
        lock (_gate)
        {
            if (Entries.Exists(entry => entry.Id.Id == eventId))
            {
                return Task.CompletedTask;
            }

            _awaitedEventId = eventId;
            _awaited = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            return _awaited.Task.Within($"The log entry with event {eventId}");
        }
    }

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
    /// <exception cref="InvalidOperationException">
    /// The entry's event id is <see cref="ThrowForEventId"/>; the entry is recorded first.
    /// </exception>
    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
        Func<TState, Exception?, string> formatter)
    {
        ArgumentNullException.ThrowIfNull(formatter);
        string message = formatter(state, exception);
        lock (_gate)
        {
            Entries.Add((logLevel, eventId, message, exception));
            if (_awaited is not null && eventId.Id == _awaitedEventId)
            {
                _awaited.SetResult();
                _awaited = null;
            }
        }

        if (ThrowForEventId != 0 && eventId.Id == ThrowForEventId)
        {
            throw new InvalidOperationException("The logging provider failed.");
        }
    }
}
