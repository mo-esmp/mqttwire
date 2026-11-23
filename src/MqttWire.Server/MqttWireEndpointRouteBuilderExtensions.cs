using System.Diagnostics.CodeAnalysis;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Features;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;

namespace MqttWire.Server;

/// <summary>
/// Maps the endpoint of the MQTT server.
/// </summary>
public static class MqttWireEndpointRouteBuilderExtensions
{
    /// <summary>
    /// Maps the endpoint that accepts MQTT connections over WebSockets (design 6.5). A request that is not a
    /// WebSocket request, or does not ask for the sub-protocol <c>mqtt</c>, is answered with HTTP 400; once the
    /// server is stopping, a new request is answered with HTTP 503. The application must call
    /// <c>app.UseWebSockets()</c> before the endpoint is reached; a request that arrives without it fails with
    /// <see cref="InvalidOperationException"/>.
    /// </summary>
    /// <param name="endpoints">
    /// The endpoint route builder; its services must include the server, registered with
    /// <see cref="MqttWireServiceCollectionExtensions.AddMqttWireServer{THandler}"/>.
    /// </param>
    /// <param name="pattern">
    /// The route pattern, for example <c>"/mqtt"</c>.
    /// </param>
    /// <returns>
    /// The builder of the mapped endpoint, so the host can add conventions such as authorization to it.
    /// </returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="endpoints"/> or <paramref name="pattern"/> is <see langword="null"/>.
    /// </exception>
    /// <exception cref="InvalidOperationException">
    /// The server was not registered with
    /// <see cref="MqttWireServiceCollectionExtensions.AddMqttWireServer{THandler}"/>; nothing is mapped.
    /// </exception>
    public static IEndpointConventionBuilder MapMqttWire(this IEndpointRouteBuilder endpoints,
        [StringSyntax("Route")] string pattern)
    {
        ArgumentNullException.ThrowIfNull(endpoints);
        ArgumentNullException.ThrowIfNull(pattern);
        MqttWireServer server = endpoints.ServiceProvider.GetService<MqttWireServer>()
                                ?? throw new InvalidOperationException(
                                    "The MQTT server is not registered: call services.AddMqttWireServer<THandler>() before MapMqttWire.");

        // Not async (plan decision 3): the request's task is the connection's run, the one async method whose state
        // lives as long as the connection.

        return endpoints.Map(pattern, Handle);

        Task Handle(HttpContext httpContext) => HandleRequest(server, httpContext);
    }

    /// <summary>
    /// Handles one request to the endpoint, in order: a request without the WebSocket feature throws; one that is
    /// not a WebSocket request, or does not ask for <c>mqtt</c>, is answered with HTTP 400; one that arrives while the
    /// server is stopping is answered with HTTP 503; any other becomes a connection, whose run accepts the WebSocket.
    /// </summary>
    /// <param name="server">
    /// The server.
    /// </param>
    /// <param name="httpContext">
    /// The request's context.
    /// </param>
    /// <returns>
    /// A completed task when the request was answered with a status; otherwise the task of the connection's run,
    /// which completes when the connection is Closed and fails only when the WebSocket cannot be accepted.
    /// </returns>
    /// <exception cref="InvalidOperationException">
    /// The request has no <see cref="IHttpWebSocketFeature"/>: the application did not call
    /// <c>app.UseWebSockets()</c>. Nothing is written.
    /// </exception>
    internal static Task HandleRequest(MqttWireServer server, HttpContext httpContext)
    {
        if (httpContext.Features.Get<IHttpWebSocketFeature>() is null)
        {
            throw new InvalidOperationException(
                "The MQTT endpoint needs WebSockets: call app.UseWebSockets() before the endpoint is reached.");
        }

        WebSocketManager webSockets = httpContext.WebSockets;
        if (!webSockets.IsWebSocketRequest || !AsksForMqtt(webSockets.WebSocketRequestedProtocols))
        {
            httpContext.Response.StatusCode = StatusCodes.Status400BadRequest;
            return Task.CompletedTask;
        }

        if (server.IsStopping)
        {
            httpContext.Response.StatusCode = StatusCodes.Status503ServiceUnavailable;
            return Task.CompletedTask;
        }

        return server.RunConnectionAsync(httpContext, link: null);
    }

    /// <summary>
    /// Tells whether the client asked for the sub-protocol <c>mqtt</c>, compared exactly.
    /// </summary>
    /// <param name="protocols">
    /// The sub-protocols the client asked for.
    /// </param>
    /// <returns>
    /// <see langword="true"/> when <c>mqtt</c> is among them; <see langword="false"/> otherwise.
    /// </returns>
    private static bool AsksForMqtt(IList<string> protocols)
    {
        for (int i = 0; i < protocols.Count; i++)
        {
            if (string.Equals(protocols[i], MqttWireConnection.SubProtocol, StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }
}
