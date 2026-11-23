using Microsoft.Extensions.Hosting;

namespace MqttWire.Server.Internal;

/// <summary>
/// Ties the server to the host's lifetime (plan decision 16): starts the sweep in <see cref="StartingAsync"/>, which
/// the host calls on every lifecycle service before it starts any hosted service, so the sweep exists before the web
/// server (whose hosted service is registered before the host's own) accepts a connection; and stops the server in
/// <see cref="StoppingAsync"/>, which the host calls on every lifecycle service before it stops any hosted service,
/// so the web server is never left waiting on open WebSockets.
/// </summary>
/// <param name="server">
/// The server.
/// </param>
internal sealed class MqttWireServerLifetime(MqttWireServer server) : IHostedLifecycleService
{
    /// <summary>
    /// Starts the server's sweep, before any hosted service, the web server's included, is started.
    /// </summary>
    /// <param name="cancellationToken">
    /// Not used: the start completes at once.
    /// </param>
    /// <returns>
    /// A completed task.
    /// </returns>
    /// <exception cref="InvalidOperationException">
    /// The server was already started, or has been stopped.
    /// </exception>
    public Task StartingAsync(CancellationToken cancellationToken)
    {
        server.Start();
        return Task.CompletedTask;
    }

    /// <summary>
    /// Does nothing: the server was started in <see cref="StartingAsync"/>.
    /// </summary>
    /// <param name="cancellationToken">
    /// Not used.
    /// </param>
    /// <returns>
    /// A completed task.
    /// </returns>
    public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    /// <summary>
    /// Does nothing.
    /// </summary>
    /// <param name="cancellationToken">
    /// Not used.
    /// </param>
    /// <returns>
    /// A completed task.
    /// </returns>
    public Task StartedAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    /// <summary>
    /// Stops the server and waits for its stop to end (design 6.6, "When the host stops"). When the host's token
    /// fires first, the wait returns early and quietly: the server keeps stopping on its own, bounded by its close
    /// deadline, and the host goes on with its shutdown.
    /// </summary>
    /// <param name="cancellationToken">
    /// Fires when the host no longer waits for the shutdown.
    /// </param>
    /// <returns>
    /// A task that completes when the server has stopped, or when <paramref name="cancellationToken"/> fires; it
    /// does not fail.
    /// </returns>
    public async Task StoppingAsync(CancellationToken cancellationToken)
    {
        try
        {
            await server.StopAsync().WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // The host gave up waiting; the stop goes on, and its own task never fails.
        }
    }

    /// <summary>
    /// Does nothing: the server was stopped in <see cref="StoppingAsync"/>.
    /// </summary>
    /// <param name="cancellationToken">
    /// Not used.
    /// </param>
    /// <returns>
    /// A completed task.
    /// </returns>
    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    /// <summary>
    /// Does nothing.
    /// </summary>
    /// <param name="cancellationToken">
    /// Not used.
    /// </param>
    /// <returns>
    /// A completed task.
    /// </returns>
    public Task StoppedAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
