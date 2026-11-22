using Microsoft.AspNetCore.Http;
using MqttWire.Server;
using MqttWire.Tests.Link;

namespace MqttWire.Tests.Server;

/// <summary>
/// One connection the harness opened: its in-memory link, the task of its run, and the source of its request's
/// cancellation.
/// </summary>
/// <param name="harness">
/// The harness that opened it.
/// </param>
/// <param name="link">
/// The link the server reads from and writes to.
/// </param>
/// <param name="run">
/// The task of the server's run of the connection.
/// </param>
/// <param name="requestAborted">
/// The source of the HTTP request's <see cref="HttpContext.RequestAborted"/>.
/// </param>
/// <param name="httpContext">
/// The HTTP context the server was given.
/// </param>
internal sealed class TestConnection(
    ServerHarness harness,
    MemoryLink link,
    Task run,
    CancellationTokenSource requestAborted,
    HttpContext httpContext)
{
    /// <summary>
    /// Gets the link the server reads from and writes to.
    /// </summary>
    public MemoryLink Link { get; } = link;

    /// <summary>
    /// Gets the task of the server's run of the connection; it completes when the connection is Closed.
    /// </summary>
    public Task Run { get; } = run;

    /// <summary>
    /// Gets the source of the HTTP request's cancellation, for a test that ends the request.
    /// </summary>
    public CancellationTokenSource RequestAborted { get; } = requestAborted;

    /// <summary>
    /// Gets the HTTP context the server was given.
    /// </summary>
    public HttpContext HttpContext { get; } = httpContext;

    /// <summary>
    /// Gets the server's connection once the handler's <c>OnAdmit</c> has seen it; <see langword="null"/> before.
    /// </summary>
    public MqttWireConnection? Connection => harness.Handler.FindAdmitted(HttpContext);

    /// <summary>
    /// Delivers bytes from the client, then asserts the harness's bounds. A whole CONNECT at the start of the bytes
    /// gives the harness the client's window first (<see cref="ServerHarness.NoteConnect"/>).
    /// </summary>
    /// <param name="bytes">
    /// The bytes the server reads, as one chunk.
    /// </param>
    public void Deliver(ReadOnlySpan<byte> bytes)
    {
        harness.NoteConnect(HttpContext, bytes);
        Link.Deliver(bytes);
        harness.AssertBounds();
    }

    /// <summary>
    /// Completes every write the link holds, oldest first, and stops holding later ones, with the server carrying on
    /// from each completion on this thread, before this returns, as it does on the thread on which a socket completes
    /// a write. The test runner's synchronization context is taken off the thread meanwhile: with it, .NET would
    /// queue what follows a completed write to the thread pool, and a test could not tell when it has run.
    /// </summary>
    /// <param name="exception">
    /// The exception the held writes fail with, or <see langword="null"/> to complete them successfully.
    /// </param>
    public void CompleteHeldWrites(Exception? exception = null)
    {
        SynchronizationContext? context = SynchronizationContext.Current;
        SynchronizationContext.SetSynchronizationContext(syncContext: null);
        try
        {
            Link.CompleteHeldWrites(exception);
        }
        finally
        {
            SynchronizationContext.SetSynchronizationContext(context);
        }

        harness.AssertBounds();
    }

    /// <summary>
    /// Waits until the server's send path for this connection has been given up, for a test whose next step or
    /// assertion holds only once the holder of the path has finished: a write the link took (which
    /// <see cref="MemoryLink.WhenWrittenAsync"/> reports) is not yet a path given up. It waits on the server's own
    /// completion of the give-up, never in real time.
    /// </summary>
    /// <returns>
    /// A task that completes once the path is idle; at once when it is idle now.
    /// </returns>
    public Task WhenSendPathIdleAsync() =>
        Connection.ShouldNotBeNull().WhenSendPathIdleAsync().Within("The give-up of the send path");

    /// <summary>
    /// Waits until the server's run of the connection has ended, at most <see cref="BoundedWaits.Limit"/>, so that a
    /// run that never ends fails the test instead of hanging it.
    /// </summary>
    /// <returns>
    /// A task that completes when the run has; it fails with <see cref="TimeoutException"/> when the run is stuck.
    /// </returns>
    public Task RunEnded() => Run.Within("The connection's run");

    /// <summary>
    /// Takes everything the server wrote since the last call.
    /// </summary>
    /// <returns>
    /// The written bytes, concatenated; empty when nothing was written.
    /// </returns>
    public byte[] TakeWritten() => Link.TakeWritten();
}
