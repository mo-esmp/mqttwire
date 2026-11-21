using MqttWire.Link;

namespace MqttWire.Tests.Link;

/// <summary>
/// An <see cref="IMqttLinkConnector"/> for tests: each call hands out a new <see cref="MemoryLink"/>, or fails with
/// the next scripted network error, at once; or, after <see cref="HoldConnects"/>, stays pending until its token is
/// cancelled. It is safe to call from several threads.
/// </summary>
internal sealed class MemoryLinkConnector : IMqttLinkConnector
{
    private readonly Lock _gate = new();
    private readonly List<MemoryLink> _links = [];
    private readonly Queue<Exception> _failures = new();
    private bool _holdConnects;
    private int _pendingConnects;

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
    /// Gets the number of held calls still pending.
    /// </summary>
    public int PendingConnects
    {
        get
        {
            lock (_gate)
            {
                return _pendingConnects;
            }
        }
    }

    /// <summary>
    /// Makes every later call stay pending, as a dial to a server that does not answer, until its token is
    /// cancelled; it then ends with <see cref="OperationCanceledException"/> and hands out no link.
    /// </summary>
    public void HoldConnects()
    {
        lock (_gate)
        {
            _holdConnects = true;
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
    /// A completed task with the new link, or with the error and no link; while connects are held, a task that
    /// ends with <see cref="OperationCanceledException"/> when the token is cancelled.
    /// </returns>
    public ValueTask<LinkConnectResult> ConnectAsync(MqttWireClientOptions options, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_gate)
        {
            if (_holdConnects)
            {
                return HoldAsync(cancellationToken);
            }

            if (_failures.TryDequeue(out Exception? error))
            {
                return new ValueTask<LinkConnectResult>(new LinkConnectResult(null, error));
            }

            MemoryLink link = new();
            _links.Add(link);
            return new ValueTask<LinkConnectResult>(new LinkConnectResult(link, null));
        }
    }

    /// <summary>
    /// Waits, counted in <see cref="PendingConnects"/>, until the token is cancelled.
    /// </summary>
    /// <param name="cancellationToken">
    /// Ends the wait.
    /// </param>
    /// <returns>
    /// A task that ends with <see cref="OperationCanceledException"/> when the token is cancelled.
    /// </returns>
    private async ValueTask<LinkConnectResult> HoldAsync(CancellationToken cancellationToken)
    {
        _pendingConnects++;
        try
        {
            await Task.Delay(Timeout.Infinite, cancellationToken).ConfigureAwait(false);
            throw new InvalidOperationException("An infinite delay ended without being cancelled.");
        }
        finally
        {
            lock (_gate)
            {
                _pendingConnects--;
            }
        }
    }
}
