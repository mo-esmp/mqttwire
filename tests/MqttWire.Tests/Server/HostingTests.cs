using System.Buffers;
using System.Net.WebSockets;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Features;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using MqttWire.Server;
using MqttWire.Server.Internal;
using MqttWire.Tests.Client;
using MqttWire.Tests.Link;

namespace MqttWire.Tests.Server;

/// <summary>
/// Tests of the glue a host uses: the registration, the start and stop through the generic host (no web server, no
/// network), and the synchronous branches of the endpoint's request delegate, run on a <see cref="DefaultHttpContext"/>.
/// </summary>
public sealed class HostingTests
{
    /// <summary>
    /// Proves that an invalid option makes the host's start throw <see cref="OptionsValidationException"/> and the
    /// host never reaches the started state.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task StartAsync_InvalidOption_ThrowsAndTheHostDoesNotStart()
    {
        // Arrange
        HostApplicationBuilder builder = Host.CreateEmptyApplicationBuilder(settings: null);
        builder.Services.AddMqttWireServer<RecordingHandler>(static options => options.MaxOutgoingBytes = 1);
        using IHost host = builder.Build();
        IHostApplicationLifetime lifetime = host.Services.GetRequiredService<IHostApplicationLifetime>();

        // Act
        OptionsValidationException thrown =
            await Should.ThrowAsync<OptionsValidationException>(() => host.StartAsync(CancellationToken.None));

        // Assert
        thrown.Failures.ShouldBe(["MaxOutgoingBytes must be at least 65,536."]);
        lifetime.ApplicationStarted.IsCancellationRequested.ShouldBeFalse();
    }

    /// <summary>
    /// Proves that the registration gives one server for every resolution, built over the registered handler, the
    /// configured options, the system clock and the shared pool, and one lifetime service.
    /// </summary>
    [Fact]
    public void AddMqttWireServer_Resolved_GivesOneServerAndTheHandler()
    {
        // Arrange
        HostApplicationBuilder builder = Host.CreateEmptyApplicationBuilder(settings: null);
        builder.Services.AddMqttWireServer<RecordingHandler>(static options => options.MaxOutgoingMessages = 7);
        using IHost host = builder.Build();

        // Act
        MqttWireServer first = host.Services.GetRequiredService<MqttWireServer>();
        MqttWireServer second = host.Services.GetRequiredService<MqttWireServer>();
        IMqttWireHandler handler = host.Services.GetRequiredService<IMqttWireHandler>();

        // Assert
        second.ShouldBeSameAs(first);
        handler.ShouldBeOfType<RecordingHandler>();
        first.Handler.ShouldBeSameAs(handler);
        first.Options.MaxOutgoingMessages.ShouldBe(7);
        first.Time.ShouldBeSameAs(TimeProvider.System);
        first.Pool.ShouldBeSameAs(ArrayPool<byte>.Shared);
        host.Services.GetServices<IHostedService>().OfType<MqttWireServerLifetime>().ShouldHaveSingleItem();
    }

    /// <summary>
    /// Proves that a handler that takes <see cref="MqttWireServer"/> in its constructor, as a host does that looks
    /// connections up, resolves: the host starts, the handler was given the one server, and the server calls that
    /// handler.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task StartAsync_HandlerTakesTheServerInItsConstructor_StartsWithTheSameServer()
    {
        // Arrange
        HostApplicationBuilder builder = Host.CreateEmptyApplicationBuilder(settings: null);
        builder.Services.AddMqttWireServer<ServerTakingHandler>();
        using IHost host = builder.Build();

        // Act: on another thread, so that a resolution that never returns fails the test instead of hanging it.
        await Task.Run(() => host.StartAsync(CancellationToken.None)).Within("The host's start");
        MqttWireServer server = host.Services.GetRequiredService<MqttWireServer>();
        IMqttWireHandler handler = host.Services.GetRequiredService<IMqttWireHandler>();
        await host.StopAsync(CancellationToken.None).Within("The host's stop");

        // Assert
        handler.ShouldBeOfType<ServerTakingHandler>().Server.ShouldBeSameAs(server);
        server.Handler.ShouldBeSameAs(handler);
    }

    /// <summary>
    /// Proves that a handler whose constructor throws makes the host's start fail with that exception, and the host
    /// never reaches the started state.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task StartAsync_HandlerConstructorThrows_FailsTheStartWithThatException()
    {
        // Arrange
        HostApplicationBuilder builder = Host.CreateEmptyApplicationBuilder(settings: null);
        builder.Services.AddMqttWireServer<FailingHandler>();
        using IHost host = builder.Build();
        IHostApplicationLifetime lifetime = host.Services.GetRequiredService<IHostApplicationLifetime>();
        MqttWireServer resolvedBeforeTheStart = host.Services.GetRequiredService<MqttWireServer>();

        // Act
        InvalidOperationException thrown =
            await Should.ThrowAsync<InvalidOperationException>(() => host.StartAsync(CancellationToken.None));

        // Assert
        thrown.Message.ShouldBe(FailingHandler.Message);
        resolvedBeforeTheStart.ShouldNotBeNull();
        lifetime.ApplicationStarted.IsCancellationRequested.ShouldBeFalse();
    }

    /// <summary>
    /// Proves that a connection that reaches a server the host has not started yet still finds the handler: it is
    /// resolved then, once, and called.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task RunConnectionAsync_BeforeTheHostStarted_ResolvesTheHandlerAndCallsIt()
    {
        // Arrange
        HostApplicationBuilder builder = Host.CreateEmptyApplicationBuilder(settings: null);
        builder.Services.AddMqttWireServer<RecordingHandler>();
        using IHost host = builder.Build();
        MqttWireServer server = host.Services.GetRequiredService<MqttWireServer>();
        MemoryLink link = new();

        // Act
        Task run = server.RunConnectionAsync(new DefaultHttpContext(), link);
        link.Deliver(ServerPackets.Connect);
        byte[] written = link.TakeWritten();
        link.EndFromPeer();
        await run.Within("The connection's run");

        // Assert
        written.ShouldBe(ServerPackets.ConnAck);
        RecordingHandler handler = host.Services.GetRequiredService<IMqttWireHandler>().ShouldBeOfType<RecordingHandler>();
        server.Handler.ShouldBeSameAs(handler);
        handler.Calls.ShouldBe(["OnConnectAsync c1", "OnAdmit c1 replaces=-", "OnClosed c1 ConnectionLost"]);
    }

    /// <summary>
    /// Proves that registering the server a second time on the same service collection throws.
    /// </summary>
    [Fact]
    public void AddMqttWireServer_CalledTwice_Throws()
    {
        // Arrange
        ServiceCollection services = new();
        services.AddMqttWireServer<RecordingHandler>();

        // Act
        InvalidOperationException thrown =
            Should.Throw<InvalidOperationException>(() => services.AddMqttWireServer<RecordingHandler>());

        // Assert
        thrown.Message.ShouldContain("AddMqttWireServer");
    }

    /// <summary>
    /// Proves that the host stops the server in the phase before any hosted service is stopped: a hosted service that
    /// the host stops first already sees the server stopping.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task StopAsync_Host_StopsTheServerBeforeAnyHostedServiceIsStopped()
    {
        // Arrange
        HostApplicationBuilder builder = Host.CreateEmptyApplicationBuilder(settings: null);
        builder.Services.AddMqttWireServer<RecordingHandler>();
        builder.Services.AddSingleton<StopRecorder>();
        builder.Services.AddHostedService(static provider => provider.GetRequiredService<StopRecorder>());
        using IHost host = builder.Build();
        await host.StartAsync(CancellationToken.None).Within("The host's start");
        StopRecorder recorder = host.Services.GetRequiredService<StopRecorder>();
        bool stoppingWhenStarted = host.Services.GetRequiredService<MqttWireServer>().IsStopping;

        // Act
        await host.StopAsync(CancellationToken.None).Within("The host's stop");

        // Assert
        stoppingWhenStarted.ShouldBeFalse();
        recorder.WasServerStopping.ShouldBe(true);
    }

    /// <summary>
    /// Proves that the lifetime service creates the sweep's timer in <c>StartingAsync</c>, which the host calls before
    /// it starts any hosted service, and that <c>StartAsync</c> creates no other.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task StartingAsync_Lifetime_CreatesTheSweepTimer()
    {
        // Arrange
        CountingTimeProvider timers = new(new FakeTimeProvider());
        MqttWireServer server = new(new RecordingHandler(), new MqttWireServerOptions(), new RecordingLogger(), timers,
            ArrayPool<byte>.Shared);
        MqttWireServerLifetime lifetime = new(server);

        // Act
        await lifetime.StartingAsync(CancellationToken.None).Within("The lifetime's starting");
        int timersAfterStarting = timers.PeriodicTimers().Count;
        await lifetime.StartAsync(CancellationToken.None).Within("The lifetime's start");

        // Assert
        timersAfterStarting.ShouldBe(1);
        timers.PeriodicTimers().ShouldHaveSingleItem().Period.ShouldBe(ServerLimits.SweepInterval);
    }

    /// <summary>
    /// Proves that mapping the endpoint without the registration throws an error that names the missing call.
    /// </summary>
    [Fact]
    public void MapMqttWire_WithoutAddMqttWireServer_Throws()
    {
        // Arrange
        using ServiceProvider provider = new ServiceCollection().BuildServiceProvider();
        RouteBuilderStub endpoints = new(provider);

        // Act
        InvalidOperationException thrown = Should.Throw<InvalidOperationException>(() => endpoints.MapMqttWire("/mqtt"));

        // Assert
        thrown.Message.ShouldContain("AddMqttWireServer");
        endpoints.DataSources.ShouldBeEmpty();
    }

    /// <summary>
    /// Proves that a request without the WebSocket feature, which <c>app.UseWebSockets()</c> adds, throws an error
    /// that names that call.
    /// </summary>
    [Fact]
    public void HandleRequest_NoWebSocketFeature_ThrowsNamingUseWebSockets()
    {
        // Arrange
        MqttWireServer server = CreateServer();
        DefaultHttpContext httpContext = new();

        // Act
        InvalidOperationException thrown = Should.Throw<InvalidOperationException>(
            () => MqttWireEndpointRouteBuilderExtensions.HandleRequest(server, httpContext));

        // Assert
        thrown.Message.ShouldContain("UseWebSockets()");
    }

    /// <summary>
    /// Proves that a request that is not a WebSocket request, or does not ask for the sub-protocol <c>mqtt</c>, is
    /// answered with HTTP 400 at once, and no connection is started.
    /// </summary>
    /// <param name="isWebSocketRequest">
    /// Whether the request is a WebSocket request.
    /// </param>
    /// <param name="protocols">
    /// The value of the request's <c>Sec-WebSocket-Protocol</c> header, or <see langword="null"/> for none.
    /// </param>
    [Theory]
    [InlineData(false, "mqtt")]
    [InlineData(true, null)]
    [InlineData(true, "mqttv3.1, MQTT")]
    public void HandleRequest_MqttNotRequested_Answers400(bool isWebSocketRequest, string? protocols)
    {
        // Arrange
        MqttWireServer server = CreateServer();
        DefaultHttpContext httpContext = CreateRequest(new WebSocketFeatureStub(isWebSocketRequest), protocols);

        // Act
        Task handled = MqttWireEndpointRouteBuilderExtensions.HandleRequest(server, httpContext);

        // Assert
        handled.IsCompletedSuccessfully.ShouldBeTrue();
        httpContext.Response.StatusCode.ShouldBe(StatusCodes.Status400BadRequest);
        server.Registry.Slots.ShouldAllBe(slot => slot == null);
    }

    /// <summary>
    /// Proves that a WebSocket request for <c>mqtt</c> that arrives once the server is stopping is answered with HTTP
    /// 503 at once, and its WebSocket is not accepted.
    /// </summary>
    [Fact]
    public void HandleRequest_ServerStopping_Answers503()
    {
        // Arrange
        MqttWireServer server = CreateServer();
        server.StopAsync().IsCompletedSuccessfully.ShouldBeTrue();
        WebSocketFeatureStub feature = new(isWebSocketRequest: true);
        DefaultHttpContext httpContext = CreateRequest(feature, "mqtt");

        // Act
        Task handled = MqttWireEndpointRouteBuilderExtensions.HandleRequest(server, httpContext);

        // Assert
        handled.IsCompletedSuccessfully.ShouldBeTrue();
        httpContext.Response.StatusCode.ShouldBe(StatusCodes.Status503ServiceUnavailable);
        feature.Accepted.ShouldBeNull();
    }

    /// <summary>
    /// Proves that a WebSocket request that asks for <c>mqtt</c> among other sub-protocols is accepted with the
    /// sub-protocol <c>mqtt</c>, no WebSocket keep-alive and no compression; an accept that fails fails the request's
    /// task before the connection takes a slot.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task HandleRequest_MqttRequested_AcceptsTheWebSocket()
    {
        // Arrange
        MqttWireServer server = CreateServer();
        WebSocketFeatureStub feature = new(isWebSocketRequest: true);
        DefaultHttpContext httpContext = CreateRequest(feature, "mqttv3.1, mqtt");

        // Act
        Task handled = MqttWireEndpointRouteBuilderExtensions.HandleRequest(server, httpContext);

        // Assert
        await Should.ThrowAsync<WebSocketException>(handled);
        WebSocketAcceptContext accepted = feature.Accepted.ShouldNotBeNull();
        accepted.SubProtocol.ShouldBe("mqtt");
        accepted.KeepAliveInterval.ShouldBe(TimeSpan.Zero);
        accepted.DangerousEnableCompression.ShouldBeFalse();
        httpContext.Response.StatusCode.ShouldBe(StatusCodes.Status200OK);
        server.Registry.Slots.ShouldAllBe(slot => slot == null);
    }

    /// <summary>
    /// Creates a server that is not started, over a recording handler and logger and a fake clock.
    /// </summary>
    /// <returns>
    /// The server.
    /// </returns>
    private static MqttWireServer CreateServer() =>
        new(new RecordingHandler(), new MqttWireServerOptions(), new RecordingLogger(), new FakeTimeProvider(),
            ArrayPool<byte>.Shared);

    /// <summary>
    /// Creates the context of a request that has a WebSocket feature and, optionally, asks for sub-protocols.
    /// </summary>
    /// <param name="feature">
    /// The request's WebSocket feature.
    /// </param>
    /// <param name="protocols">
    /// The value of the <c>Sec-WebSocket-Protocol</c> header, or <see langword="null"/> for none.
    /// </param>
    /// <returns>
    /// The context.
    /// </returns>
    private static DefaultHttpContext CreateRequest(IHttpWebSocketFeature feature, string? protocols)
    {
        DefaultHttpContext httpContext = new();
        httpContext.Features.Set(feature);
        if (protocols is not null)
        {
            httpContext.Request.Headers.SecWebSocketProtocol = protocols;
        }

        return httpContext;
    }

    /// <summary>
    /// A WebSocket feature as <c>app.UseWebSockets()</c> adds it, whose accept records its context and fails.
    /// </summary>
    /// <param name="isWebSocketRequest">
    /// Whether the request is a WebSocket request.
    /// </param>
    private sealed class WebSocketFeatureStub(bool isWebSocketRequest) : IHttpWebSocketFeature
    {
        /// <summary>
        /// Gets a value indicating whether the request is a WebSocket request.
        /// </summary>
        public bool IsWebSocketRequest { get; } = isWebSocketRequest;

        /// <summary>
        /// Gets the context the accept was asked with, or <see langword="null"/> when it was not asked.
        /// </summary>
        public WebSocketAcceptContext? Accepted { get; private set; }

        /// <summary>
        /// Records the context and fails, as an accept on a connection that is gone does.
        /// </summary>
        /// <param name="context">
        /// The accept's context.
        /// </param>
        /// <returns>
        /// A task that fails with <see cref="WebSocketException"/>.
        /// </returns>
        public Task<WebSocket> AcceptAsync(WebSocketAcceptContext context)
        {
            Accepted = context;
            return Task.FromException<WebSocket>(new WebSocketException("The connection is gone."));
        }
    }

    /// <summary>
    /// An endpoint route builder over a service provider, without a web server.
    /// </summary>
    /// <param name="serviceProvider">
    /// The services the mapping resolves from.
    /// </param>
    private sealed class RouteBuilderStub(IServiceProvider serviceProvider) : IEndpointRouteBuilder
    {
        /// <summary>
        /// Gets the services the mapping resolves from.
        /// </summary>
        public IServiceProvider ServiceProvider { get; } = serviceProvider;

        /// <summary>
        /// Gets the data sources the mapping adds to.
        /// </summary>
        public ICollection<EndpointDataSource> DataSources { get; } = [];

        /// <summary>
        /// Creates an application builder over <see cref="ServiceProvider"/>.
        /// </summary>
        /// <returns>
        /// The application builder.
        /// </returns>
        public IApplicationBuilder CreateApplicationBuilder() => new ApplicationBuilder(ServiceProvider);
    }

    /// <summary>
    /// A hosted service that records, when the host stops it, whether the server was already stopping.
    /// </summary>
    /// <param name="server">
    /// The server.
    /// </param>
    private sealed class StopRecorder(MqttWireServer server) : IHostedService
    {
        /// <summary>
        /// Gets whether the server was stopping when the host stopped this service; <see langword="null"/> before.
        /// </summary>
        public bool? WasServerStopping { get; private set; }

        /// <summary>
        /// Does nothing.
        /// </summary>
        /// <param name="cancellationToken">
        /// Not used.
        /// </param>
        /// <returns>
        /// A completed task.
        /// </returns>
        public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;

        /// <summary>
        /// Records whether the server is stopping.
        /// </summary>
        /// <param name="cancellationToken">
        /// Not used.
        /// </param>
        /// <returns>
        /// A completed task.
        /// </returns>
        public Task StopAsync(CancellationToken cancellationToken)
        {
            WasServerStopping = server.IsStopping;
            return Task.CompletedTask;
        }
    }

    /// <summary>
    /// A handler that takes the server in its constructor, as a host does that looks connections up.
    /// </summary>
    /// <param name="server">
    /// The server, from dependency injection.
    /// </param>
    private sealed class ServerTakingHandler(MqttWireServer server) : AcceptingHandler
    {
        /// <summary>
        /// Gets the server the handler was given.
        /// </summary>
        public MqttWireServer Server { get; } = server;
    }

    /// <summary>
    /// A handler whose constructor throws.
    /// </summary>
    private sealed class FailingHandler : AcceptingHandler
    {
        /// <summary>
        /// The message of the exception the constructor throws.
        /// </summary>
        public const string Message = "The handler cannot be created.";

        /// <summary>
        /// Throws, as a handler does whose own dependencies are missing.
        /// </summary>
        /// <exception cref="InvalidOperationException">
        /// Always.
        /// </exception>
        public FailingHandler() => throw new InvalidOperationException(Message);
    }

    /// <summary>
    /// A handler that accepts everything and keeps nothing, the base of the handlers of these tests, which differ
    /// only in their constructors.
    /// </summary>
    private abstract class AcceptingHandler : IMqttWireHandler
    {
        /// <summary>
        /// Accepts the connection with no state.
        /// </summary>
        /// <param name="request">
        /// The CONNECT.
        /// </param>
        /// <param name="cancellationToken">
        /// Not used.
        /// </param>
        /// <returns>
        /// An acceptance.
        /// </returns>
        public ValueTask<MqttConnectResult> OnConnectAsync(MqttConnectRequest request,
            CancellationToken cancellationToken) => ValueTask.FromResult(MqttConnectResult.Accept());

        /// <summary>
        /// Admits the connection.
        /// </summary>
        /// <param name="connection">
        /// The new connection.
        /// </param>
        /// <param name="replaces">
        /// The connection that has the client id now, or <see langword="null"/>.
        /// </param>
        /// <returns>
        /// An acceptance.
        /// </returns>
        public MqttAdmitResult OnAdmit(MqttWireConnection connection, MqttWireConnection? replaces) =>
            MqttAdmitResult.Accepted;

        /// <summary>
        /// Grants the subscription at the QoS asked for.
        /// </summary>
        /// <param name="connection">
        /// The subscribing connection.
        /// </param>
        /// <param name="topicFilter">
        /// The topic filter.
        /// </param>
        /// <param name="requested">
        /// The QoS asked for.
        /// </param>
        /// <returns>
        /// A grant of <paramref name="requested"/>.
        /// </returns>
        public MqttSubscribeResult OnSubscribe(MqttWireConnection connection, string topicFilter, MqttQoS requested) =>
            MqttSubscribeResult.Granted(requested);

        /// <summary>
        /// Does nothing.
        /// </summary>
        /// <param name="connection">
        /// The connection that unsubscribed.
        /// </param>
        /// <param name="topicFilter">
        /// The topic filter removed.
        /// </param>
        public void OnUnsubscribe(MqttWireConnection connection, string topicFilter)
        {
        }

        /// <summary>
        /// Accepts the publish.
        /// </summary>
        /// <param name="connection">
        /// The publishing connection.
        /// </param>
        /// <param name="publish">
        /// The message.
        /// </param>
        /// <returns>
        /// An acceptance.
        /// </returns>
        public MqttPublishResult OnPublish(MqttWireConnection connection, in MqttPublish publish) =>
            MqttPublishResult.Accepted;

        /// <summary>
        /// Does nothing.
        /// </summary>
        /// <param name="connection">
        /// The connection that ended.
        /// </param>
        /// <param name="reason">
        /// Why it ended.
        /// </param>
        public void OnClosed(MqttWireConnection connection, MqttCloseReason reason)
        {
        }
    }
}
