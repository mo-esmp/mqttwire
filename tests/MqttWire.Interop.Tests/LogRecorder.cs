using Microsoft.Extensions.Logging;

namespace MqttWire.Interop.Tests;

/// <summary>
/// A logger provider that keeps every line written through it, so a test can wait for a line without polling.
/// </summary>
internal sealed class LogRecorder : ILoggerProvider
{
    /// <summary>
    /// Guards <see cref="_entries"/> and <see cref="_waiters"/>.
    /// </summary>
    private readonly Lock _gate = new();

    /// <summary>
    /// Every line written so far, in order.
    /// </summary>
    private readonly List<LogEntry> _entries = [];

    /// <summary>
    /// The tests waiting for a line that has not been written yet.
    /// </summary>
    private readonly List<(Func<LogEntry, bool> Match, TaskCompletionSource<LogEntry> Found)> _waiters = [];

    /// <summary>
    /// Gets or sets a callback that runs after each line is recorded, on the thread that logs it, so a test can act
    /// at the very moment the server writes a line; or <see langword="null"/>.
    /// </summary>
    public Action<LogEntry>? Recorded { get; set; }

    /// <summary>
    /// Gets a copy of every line written so far, in order.
    /// </summary>
    public IReadOnlyList<LogEntry> Entries
    {
        get
        {
            lock (_gate)
            {
                return [.. _entries];
            }
        }
    }

    /// <summary>
    /// Waits for the first line that matches, written before or after this call.
    /// </summary>
    /// <param name="match">
    /// Decides whether a line is the one waited for.
    /// </param>
    /// <returns>
    /// A task that completes with the line; it never fails on its own, so the caller bounds it.
    /// </returns>
    public Task<LogEntry> WhenLoggedAsync(Func<LogEntry, bool> match)
    {
        lock (_gate)
        {
            foreach (LogEntry entry in _entries)
            {
                if (match(entry))
                {
                    return Task.FromResult(entry);
                }
            }

            TaskCompletionSource<LogEntry> found = new(TaskCreationOptions.RunContinuationsAsynchronously);
            _waiters.Add((match, found));
            return found.Task;
        }
    }

    /// <summary>
    /// Creates the logger of a category.
    /// </summary>
    /// <param name="categoryName">
    /// The category.
    /// </param>
    /// <returns>
    /// A logger that records into this provider.
    /// </returns>
    public ILogger CreateLogger(string categoryName) => new Recorder(this, categoryName);

    /// <summary>
    /// Does nothing: the provider holds nothing to release.
    /// </summary>
    public void Dispose()
    {
    }

    /// <summary>
    /// Records a line, completes the waiters it matches, and runs <see cref="Recorded"/>.
    /// </summary>
    /// <param name="entry">
    /// The line.
    /// </param>
    private void Add(LogEntry entry)
    {
        lock (_gate)
        {
            _entries.Add(entry);
            for (int i = _waiters.Count - 1; i >= 0; i--)
            {
                if (_waiters[i].Match(entry))
                {
                    _waiters[i].Found.TrySetResult(entry);
                    _waiters.RemoveAt(i);
                }
            }
        }

        Recorded?.Invoke(entry);
    }

    /// <summary>
    /// One line that was written.
    /// </summary>
    /// <param name="Category">
    /// The logger's category.
    /// </param>
    /// <param name="Level">
    /// The level.
    /// </param>
    /// <param name="EventId">
    /// The event id.
    /// </param>
    /// <param name="Message">
    /// The formatted message.
    /// </param>
    /// <param name="Exception">
    /// The exception written with it, or <see langword="null"/>.
    /// </param>
    public sealed record LogEntry(string Category, LogLevel Level, EventId EventId, string Message, Exception? Exception);

    /// <summary>
    /// The logger of one category.
    /// </summary>
    /// <param name="owner">
    /// The provider it records into.
    /// </param>
    /// <param name="category">
    /// The category.
    /// </param>
    private sealed class Recorder(LogRecorder owner, string category) : ILogger
    {
        /// <summary>
        /// Begins no scope: scopes are not recorded.
        /// </summary>
        /// <typeparam name="TState">
        /// The type of the scope's state.
        /// </typeparam>
        /// <param name="state">
        /// The scope's state.
        /// </param>
        /// <returns>
        /// <see langword="null"/>.
        /// </returns>
        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull => null;

        /// <summary>
        /// Tells that every level is recorded; the host's filters decide what reaches this logger.
        /// </summary>
        /// <param name="logLevel">
        /// The level.
        /// </param>
        /// <returns>
        /// <see langword="true"/> unless the level is <see cref="LogLevel.None"/>.
        /// </returns>
        public bool IsEnabled(LogLevel logLevel) => logLevel != LogLevel.None;

        /// <summary>
        /// Records the line.
        /// </summary>
        /// <typeparam name="TState">
        /// The type of the line's state.
        /// </typeparam>
        /// <param name="logLevel">
        /// The level.
        /// </param>
        /// <param name="eventId">
        /// The event id.
        /// </param>
        /// <param name="state">
        /// The line's state.
        /// </param>
        /// <param name="exception">
        /// The exception, or <see langword="null"/>.
        /// </param>
        /// <param name="formatter">
        /// Formats the message.
        /// </param>
        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter) =>
            owner.Add(new LogEntry(category, logLevel, eventId, formatter(state, exception), exception));
    }
}
