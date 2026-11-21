using MqttWire.Link;

namespace MqttWire.Tests.Link;

/// <summary>
/// An <see cref="IMqttLinkConnector"/> for tests: each call hands out a new <see cref="MemoryLink"/>, or fails with
/// the next scripted network error. It completes at once and is safe to call from several threads.
/// </summary>
internal sealed class MemoryLinkConnector : IMqttLinkConnector
{
    private readonly Lock _gate = new();
    private readonly List<MemoryLink> _links = [];
    private readonly Queue<Exception> _failures = new();

    /// <summary>
    /// Gets the links handed out so far, oldest first.
    /// </summary>
    public IReadOnlyList<MemoryLink> Links
    {
        get
        {
            lock (_gate)
            {
                return [.. _links];
            }
        }
    }

    /// <summary>
    /// Makes the next call that has no failure scripted before it return the error instead of a link.
    /// </summary>
    /// <param name="error">
    /// The network error that call returns.
    /// </param>
    public void FailNext(Exception error)
    {
        ArgumentNullException.ThrowIfNull(error);
        lock (_gate)
        {
            _failures.Enqueue(error);
        }
    }

    /// <summary>
    /// Hands out a new link, or the next scripted error.
    /// </summary>
    /// <param name="options">
    /// Not used.
    /// </param>
    /// <param name="cancellationToken">
    /// An already cancelled token throws <see cref="OperationCanceledException"/>.
    /// </param>
    /// <returns>
    /// A completed task with the new link, or with the error and no link.
    /// </returns>
    public ValueTask<LinkConnectResult> ConnectAsync(MqttWireClientOptions options, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_gate)
        {
            if (_failures.TryDequeue(out Exception? error))
            {
                return new ValueTask<LinkConnectResult>(new LinkConnectResult(null, error));
            }

            MemoryLink link = new();
            _links.Add(link);
            return new ValueTask<LinkConnectResult>(new LinkConnectResult(link, null));
        }
    }
}
