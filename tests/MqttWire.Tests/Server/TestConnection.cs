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
    public MqttWireConnection? Connection =>
        harness.Handler.Admitted.Find(connection => connection.HttpContext == HttpContext);

    /// <summary>
    /// Delivers bytes from the client, then asserts the harness's bounds.
    /// </summary>
    /// <param name="bytes">
    /// The bytes the server reads, as one chunk.
    /// </param>
    public void Deliver(ReadOnlySpan<byte> bytes)
    {
        Link.Deliver(bytes);
        harness.AssertBounds();
    }

    /// <summary>
    /// Takes everything the server wrote since the last call.
    /// </summary>
    /// <returns>
    /// The written bytes, concatenated; empty when nothing was written.
    /// </returns>
    public byte[] TakeWritten() => Link.TakeWritten();
}
