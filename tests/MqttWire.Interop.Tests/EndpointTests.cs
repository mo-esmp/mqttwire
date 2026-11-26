using System.Net;
using System.Net.Http;
using System.Net.Quic;
using System.Net.Sockets;
using System.Net.WebSockets;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using MQTTnet;
using MqttWire.Server;
using MqttWire.Tests.Codec;
using MqttWire.Tests.Server;
using Xunit.Abstractions;

namespace MqttWire.Interop.Tests;

/// <summary>
/// Proves the endpoint and the WebSocket rules of design 6.5, the end of a connection and the host's stop, over
/// Kestrel on loopback.
/// </summary>
/// <param name="output">
/// Where a test writes what it could not exercise on this machine.
/// </param>
public sealed class EndpointTests(ITestOutputHelper output)
{
    /// <summary>
    /// The fail-safe wait for something that is expected to happen; a passing test never waits this long.
    /// </summary>
    private static readonly TimeSpan s_failSafe = ServerHost.FailSafe;

    /// <summary>
    /// A PINGREQ.
    /// </summary>
    private static readonly byte[] s_pingReq = Hex.Parse("C0 00");

    /// <summary>
    /// A PINGRESP.
    /// </summary>
    private static readonly byte[] s_pingResp = Hex.Parse("D0 00");

    /// <summary>
    /// Proves a WebSocket request that does not offer the sub-protocol <c>mqtt</c> is refused with HTTP 400.
    /// </summary>
    [Fact]
    public async Task Request_WithoutTheMqttSubProtocol_IsAnsweredWith400()
    {
        // Arrange
        await using ServerHost host = await ServerHost.StartAsync();
        using ClientWebSocket webSocket = new();
        webSocket.Options.CollectHttpResponseDetails = true;
        webSocket.Options.AddSubProtocol("other");

        // Act
        await Should.ThrowAsync<WebSocketException>(() => webSocket.ConnectAsync(host.WebSocketUri, CancellationToken.None).WaitAsync(s_failSafe));

        // Assert
        webSocket.HttpStatusCode.ShouldBe(HttpStatusCode.BadRequest);
    }

    /// <summary>
    /// Proves a plain HTTP/1.1 GET to the endpoint, in an application that calls <c>UseWebSockets()</c>, is answered
    /// with HTTP 400 and not with an error.
    /// </summary>
    [Fact]
    public async Task Request_NotAWebSocket_IsAnsweredWith400()
    {
        // Arrange
        await using ServerHost host = await ServerHost.StartAsync();

        // Act
        HttpResponseMessage response = await GetAsync(host, HttpVersion.Version11);

        // Assert
        response.Version.ShouldBe(HttpVersion.Version11);
        response.StatusCode.ShouldBe(HttpStatusCode.BadRequest);
        host.Logs.Entries.ShouldNotContain(entry => entry.Exception != null);
    }

    /// <summary>
    /// Proves a plain HTTP/2 GET to the endpoint, in an application that calls <c>UseWebSockets()</c>, is answered
    /// with HTTP 400 and not with an error: Kestrel's HTTP/2 stream offers the extended CONNECT feature, so the
    /// WebSocket middleware gives it the WebSocket feature.
    /// </summary>
    [Fact]
    public async Task Request_NotAWebSocketOverHttp2_IsAnsweredWith400()
    {
        // Arrange
        await using ServerHost host = await ServerHost.StartAsync(new ServerHost.Setup { Protocols = HttpProtocols.Http2 });

        // Act
        HttpResponseMessage response = await GetAsync(host, HttpVersion.Version20);

        // Assert
        response.Version.ShouldBe(HttpVersion.Version20);
        response.StatusCode.ShouldBe(HttpStatusCode.BadRequest);
        host.Logs.Entries.ShouldNotContain(entry => entry.Exception != null);
    }

    /// <summary>
    /// Proves a plain HTTP/3 GET to the endpoint, in an application that calls <c>UseWebSockets()</c>, is answered
    /// with HTTP 400 and not with an error: Kestrel's HTTP/3 stream offers the upgrade feature, so the WebSocket
    /// middleware gives it the WebSocket feature. HTTP/3 needs QUIC, which Windows 11 and later always have: there the
    /// test must run. Elsewhere (for example Linux without the MsQuic library) the request cannot be made; the test then
    /// says in its output that HTTP/3 was not exercised.
    /// </summary>
    [Fact]
    public async Task Request_NotAWebSocketOverHttp3_IsAnsweredWith400()
    {
        if (OperatingSystem.IsWindowsVersionAtLeast(10, 0, 22000))
        {
            QuicListener.IsSupported.ShouldBeTrue("Windows 11 and later have QUIC, so HTTP/3 must be exercised here.");
        }

        if (!QuicListener.IsSupported)
        {
            // Arrange, Act, Assert: no QUIC on this machine, so Kestrel cannot listen for HTTP/3.
            output.WriteLine("HTTP/3 not exercised: QUIC is not supported on this machine.");
            return;
        }

        // Arrange
        using X509Certificate2 certificate = CreateLoopbackCertificate();
        await using ServerHost host = await ServerHost.StartAsync(
            new ServerHost.Setup { Protocols = HttpProtocols.Http3, Certificate = certificate });

        // Act
        HttpResponseMessage response = await GetAsync(host, HttpVersion.Version30);

        // Assert
        response.Version.ShouldBe(HttpVersion.Version30);
        response.StatusCode.ShouldBe(HttpStatusCode.BadRequest);
        host.Logs.Entries.ShouldNotContain(entry => entry.Exception != null);
    }

    /// <summary>
    /// Proves a text message from an Active connection closes the WebSocket with status 1003.
    /// </summary>
    [Fact]
    public async Task TextMessage_Always_ClosesTheWebSocketWith1003()
    {
        // Arrange
        await using ServerHost host = await ServerHost.StartAsync();
        using ClientWebSocket webSocket = await ConnectClientWebSocketAsync(host);
        await webSocket.SendAsync(ServerPackets.Connect, WebSocketMessageType.Binary, endOfMessage: true, CancellationToken.None);
        (await ReceiveMessageAsync(webSocket)).ShouldBe(ServerPackets.ConnAck);

        // Act
        await webSocket.SendAsync("hello"u8.ToArray(), WebSocketMessageType.Text, endOfMessage: true, CancellationToken.None);
        WebSocketReceiveResult result = await webSocket.ReceiveAsync(new byte[16], CancellationToken.None).WaitAsync(s_failSafe);

        // Assert
        result.MessageType.ShouldBe(WebSocketMessageType.Close);
        webSocket.CloseStatus.ShouldBe(WebSocketCloseStatus.InvalidMessageType);
    }

    /// <summary>
    /// Proves a text message sent in place of the CONNECT closes the WebSocket with status 1003 as well.
    /// </summary>
    [Fact]
    public async Task TextMessage_BeforeConnect_ClosesTheWebSocketWith1003()
    {
        // Arrange
        await using ServerHost host = await ServerHost.StartAsync();
        using ClientWebSocket webSocket = await ConnectClientWebSocketAsync(host);

        // Act
        await webSocket.SendAsync("hello"u8.ToArray(), WebSocketMessageType.Text, endOfMessage: true, CancellationToken.None);
        WebSocketReceiveResult result = await webSocket.ReceiveAsync(new byte[16], CancellationToken.None).WaitAsync(s_failSafe);

        // Assert
        result.MessageType.ShouldBe(WebSocketMessageType.Close);
        webSocket.CloseStatus.ShouldBe(WebSocketCloseStatus.InvalidMessageType);
    }

    /// <summary>
    /// Proves the link's own close with 1003, which it sends outside the server's one-write-at-a-time send path, never
    /// corrupts the stream when a write of the server's is pending: .NET's WebSocket serialises every frame it sends,
    /// the close included, through one send lock, so the client, which read nothing until it sent the text message,
    /// receives only whole frames, each a whole piece of a delivery, and then the end. The end is the close with 1003,
    /// or the abort of the link when the server, finding its write in progress, aborts it (design 6.6, "Closing,
    /// bounded", step 2) before the client has read the close (see <see cref="IsAbortSeenByTheClient"/>). Any other
    /// end, a close with another status among them, fails the test.
    /// </summary>
    [Fact]
    public async Task TextMessage_WhileAServerWriteIsPending_NeverCorruptsTheStream()
    {
        // Arrange
        await using ServerHost host = await ServerHost.StartAsync(options => options.MaxOutgoingBytes = 64 << 20);

        // The system's receive buffer, not a small one: the client reads what the server queued, and with a receive
        // buffer of 1 KB Linux can advertise a window smaller than the server's segment (351 bytes against 576),
        // after which the server sends only on its persist timer, a few hundred bytes every 200 ms, and a frame of
        // 60 KB takes half a minute. The 12 MB below keep the server's write pending without it, since the client
        // reads nothing until it sends the text message.
        await using RawWebSocket client = await host.ConnectRawAsync();
        host.Server.TryGetConnection("c1", out MqttWireConnection? connection).ShouldBeTrue();
        byte[] payload = new byte[60_000];
        MqttTopic topic = MqttTopic.Create("a/b");
        for (int i = 0; i < 200; i++)
        {
            // 12 MB, far more than loopback's socket buffers hold (up to 4 MB of send buffer on Linux), so the
            // server's write is pending.
            connection.TrySend(topic, payload, MqttQoS.AtMostOnce).ShouldBe(MqttSendResult.Taken);
        }

        // Act
        await client.SendAsync(RawWebSocket.Text, "hello"u8.ToArray());
        List<RawWebSocket.Frame> frames = [];
        RawWebSocket.Frame? last = null;
        try
        {
            do
            {
                last = await client.ReceiveAsync().WaitAsync(s_failSafe);
                frames.Add(last);
            }
            while (last.Opcode == RawWebSocket.Binary);
        }
        catch (IOException exception) when (IsAbortSeenByTheClient(exception))
        {
            // The abort; what came before it was read whole.
            last = null;
        }

        // Assert
        if (last is not null)
        {
            last.Opcode.ShouldBe(RawWebSocket.Close);
            last.CloseStatus.ShouldBe(1003);
            frames.RemoveAt(frames.Count - 1);
        }

        frames.Count.ShouldBeLessThan(400, "The end came while deliveries were still waiting.");
        for (int i = 0; i < frames.Count; i++)
        {
            // Each delivery is its header (PUBLISH, QoS 0, remaining length 60,006, topic "a/b", no properties), then
            // its payload.
            frames[i].Opcode.ShouldBe(RawWebSocket.Binary);
            if (i % 2 == 0)
            {
                Convert.ToHexString(frames[i].Payload).ShouldBe("30E6D4030003612F6200");
            }
            else
            {
                frames[i].Payload.Length.ShouldBe(60_000);
            }
        }
    }

    /// <summary>
    /// Proves the server accepts a WebSocket that offers <c>mqtt</c> among other sub-protocols and answers with
    /// <c>mqtt</c>.
    /// </summary>
    [Fact]
    public async Task Accept_Always_AnswersWithTheMqttSubProtocol()
    {
        // Arrange
        await using ServerHost host = await ServerHost.StartAsync();
        using ClientWebSocket webSocket = new();
        webSocket.Options.AddSubProtocol("other");
        webSocket.Options.AddSubProtocol("mqtt");

        // Act
        await webSocket.ConnectAsync(host.WebSocketUri, CancellationToken.None).WaitAsync(s_failSafe);

        // Assert
        webSocket.State.ShouldBe(WebSocketState.Open);
        webSocket.SubProtocol.ShouldBe("mqtt");
    }

    /// <summary>
    /// Proves the server turns the WebSocket's own keep-alive off for its endpoint, even when the application sets one:
    /// with a keep-alive of 50 ms for every other WebSocket, a control WebSocket receives three unsolicited PONG frames
    /// while the MQTT WebSocket, opened before it, receives none, and its next frame is the answer to a PINGREQ.
    /// </summary>
    [Fact]
    public async Task Accept_Always_TurnsTheWebSocketKeepAliveOff()
    {
        // Arrange
        await using ServerHost host = await ServerHost.StartAsync(new ServerHost.Setup
        {
            ConfigureBuilder = builder =>
                builder.Services.Configure<WebSocketOptions>(options => options.KeepAliveInterval = TimeSpan.FromMilliseconds(50)),
            ConfigureApp = app => app.Map("/control", HoldWebSocketAsync),
        });
        await using RawWebSocket mqtt = await host.ConnectRawAsync();
        await using RawWebSocket control = await RawWebSocket.ConnectAsync(new Uri(host.BaseUri, "/control"));
        for (int i = 0; i < 3; i++)
        {
            (await control.ReceiveAsync().WaitAsync(s_failSafe)).Opcode.ShouldBe(RawWebSocket.Pong);
        }

        // Act
        await mqtt.SendAsync(RawWebSocket.Binary, s_pingReq);
        RawWebSocket.Frame next = await mqtt.ReceiveAsync().WaitAsync(s_failSafe);

        // Assert
        next.Opcode.ShouldBe(RawWebSocket.Binary);
        next.Payload.ShouldBe(s_pingResp);
    }

    /// <summary>
    /// Proves a connection over a real socket that sends no CONNECT is aborted at the connect deadline, and that the
    /// line of the log (event 5) names its remote address and port.
    /// </summary>
    [Fact]
    public async Task ConnectDeadline_NoConnect_LogsTheRemoteEndPoint()
    {
        // Arrange
        await using ServerHost host = await ServerHost.StartAsync();
        await using RawWebSocket silent = await RawWebSocket.ConnectAsync(host.WebSocketUri);
        string remote = silent.LocalEndPoint.ToString();

        // Act: the deadline is 10 s, acted on at the first sweep after it, so within 12 s and some.
        LogRecorder.LogEntry line = await host.Logs.WhenLoggedAsync(entry => entry.EventId.Id == 5).WaitAsync(TimeSpan.FromSeconds(20));
        Exception aborted = await Should.ThrowAsync<Exception>(() => silent.ReceiveAsync().WaitAsync(s_failSafe));

        // Assert
        line.Message.ShouldContain(remote);
        remote.ShouldStartWith("127.0.0.1:");
        aborted.ShouldBeAssignableTo<IOException>();
    }

    /// <summary>
    /// Proves an application that maps the endpoint but never calls <c>UseWebSockets()</c> fails a WebSocket request
    /// with HTTP 500, and the error logged names the missing call.
    /// </summary>
    [Fact]
    public async Task MapMqttWire_WithoutUseWebSockets_FailsTheRequestWithAClearError()
    {
        // Arrange
        await using ServerHost host = await ServerHost.StartAsync(new ServerHost.Setup { UseWebSockets = false });
        using ClientWebSocket webSocket = new();
        webSocket.Options.CollectHttpResponseDetails = true;
        webSocket.Options.AddSubProtocol("mqtt");

        // Act
        await Should.ThrowAsync<WebSocketException>(() => webSocket.ConnectAsync(host.WebSocketUri, CancellationToken.None).WaitAsync(s_failSafe));
        LogRecorder.LogEntry error = await host.Logs.WhenLoggedAsync(entry => entry.Exception is not null).WaitAsync(s_failSafe);

        // Assert
        webSocket.HttpStatusCode.ShouldBe(HttpStatusCode.InternalServerError);
        InvalidOperationException exception = error.Exception.ShouldBeOfType<InvalidOperationException>();
        exception.Message.ShouldContain("app.UseWebSockets()");
    }

    /// <summary>
    /// Proves that when the sweep aborts a link at the close deadline (the client does not answer the server's
    /// close), the host's <see cref="IMqttWireHandler.OnClosed"/> does not run inside that abort, on the sweep's
    /// thread. The sweep's timer thread cannot be wrapped by a test, so the abort itself is: the application wraps
    /// every WebSocket it accepts (<see cref="AbortWatch"/>), which marks whichever thread aborts for as long as the
    /// abort lasts, and <c>OnClosed</c> records whether its own thread carries that mark.
    /// </summary>
    [Fact]
    public async Task OnClosed_LinkAbortedAtTheCloseDeadline_DoesNotRunInsideTheSweepsAbort()
    {
        // Arrange
        AbortWatch watch = new();
        await using ServerHost host = await ServerHost.StartAsync<ClosedRecordingHandler>(new ServerHost.Setup
        {
            ConfigureBuilder = builder => builder.Services.AddSingleton(watch), ConfigureApp = watch.Use,
        });
        ClosedRecordingHandler handler = (ClosedRecordingHandler)host.Services.GetRequiredService<IMqttWireHandler>();
        await using RawWebSocket client = await host.ConnectRawAsync();
        host.Server.TryGetConnection("c1", out MqttWireConnection? connection).ShouldBeTrue();
        connection.Close(MqttReasonCode.AdministrativeAction);
        (await client.ReceiveAsync().WaitAsync(s_failSafe)).Payload
            .ShouldBe(ServerPackets.Disconnect(MqttReasonCode.AdministrativeAction));
        (await client.ReceiveAsync().WaitAsync(s_failSafe)).Opcode.ShouldBe(RawWebSocket.Close);

        // Act: the client does not answer the close, so the sweep aborts the link 2 to 4 seconds later.
        (MqttCloseReason Reason, bool InsideAnAbort) closed = await handler.Closed.WaitAsync(s_failSafe);

        // Assert
        closed.Reason.ShouldBe(MqttCloseReason.ClosedByHost);
        watch.Aborts.ShouldBeGreaterThan(0);
        closed.InsideAnAbort.ShouldBeFalse();
    }

    /// <summary>
    /// Proves that when the host's stop aborts a link whose write is pending, the host's
    /// <see cref="IMqttWireHandler.OnClosed"/> does not run inside that abort, on the thread that stops the server:
    /// the application wraps every WebSocket it accepts (<see cref="AbortWatch"/>), which marks whichever thread
    /// aborts for as long as the abort lasts, and <c>OnClosed</c> records whether its own thread carries that mark.
    /// </summary>
    [Fact]
    public async Task OnClosed_LinkAbortedByTheStop_DoesNotRunInsideTheStopsAbort()
    {
        // Arrange
        AbortWatch watch = new();
        await using ServerHost host = await ServerHost.StartAsync<ClosedRecordingHandler>(new ServerHost.Setup
        {
            ConfigureServer = options => options.MaxOutgoingBytes = 64 << 20,
            ConfigureBuilder = builder => builder.Services.AddSingleton(watch),
            ConfigureApp = watch.Use,
        });
        ClosedRecordingHandler handler = (ClosedRecordingHandler)host.Services.GetRequiredService<IMqttWireHandler>();
        await using RawWebSocket client = await host.ConnectRawAsync(receiveBufferSize: 1024);
        host.Server.TryGetConnection("c1", out MqttWireConnection? connection).ShouldBeTrue();
        for (int i = 0; i < 200; i++)
        {
            // 12 MB, within the raised outgoing limit and far more than loopback's socket buffers hold (up to 4 MB
            // of send buffer on Linux): a write stays pending.
            connection.TrySend(MqttTopic.Create("a/b"), new byte[60_000], MqttQoS.AtMostOnce).ShouldBe(MqttSendResult.Taken);
        }

        // Act
        Task stopped = host.StopAsync();
        (MqttCloseReason Reason, bool InsideAnAbort) closed = await handler.Closed.WaitAsync(s_failSafe);
        await stopped.WaitAsync(s_failSafe);

        // Assert
        closed.Reason.ShouldBe(MqttCloseReason.ServerStopping);
        watch.Aborts.ShouldBeGreaterThan(0);
        closed.InsideAnAbort.ShouldBeFalse();
    }

    /// <summary>
    /// Tells whether a failed read of the raw client is how the abort of the link reaches it: a reset
    /// (<see cref="SocketError.ConnectionReset"/>, or <see cref="SocketError.ConnectionAborted"/>, which Windows
    /// reports when the reset meets a pending send of the client's own); or the end of the stream with no close
    /// frame, on every platform, because an abort that finds no send outstanding at the socket ends the connection
    /// with a shutdown: the client reads what was already sent and then the end, which can come in the middle of a
    /// frame. A stream cut short is not a corrupted stream: corruption is a close frame or foreign bytes inside a
    /// frame, which the checks of each frame still fail on, as does a close frame with another status than 1003.
    /// </summary>
    /// <param name="exception">
    /// What the read threw.
    /// </param>
    /// <returns>
    /// <see langword="true"/> when the read ended because the server aborted the link.
    /// </returns>
    private static bool IsAbortSeenByTheClient(IOException exception) =>
        exception is EndOfStreamException
        || exception.InnerException is SocketException
        {
            SocketErrorCode: SocketError.ConnectionReset or SocketError.ConnectionAborted
        };

    /// <summary>
    /// Sends a GET to the endpoint at an exact HTTP version, accepting any server certificate.
    /// </summary>
    /// <param name="host">
    /// The host.
    /// </param>
    /// <param name="version">
    /// The HTTP version, required exactly.
    /// </param>
    /// <returns>
    /// The response.
    /// </returns>
    private static async Task<HttpResponseMessage> GetAsync(ServerHost host, Version version)
    {
        using SocketsHttpHandler handler = new();
        handler.SslOptions.RemoteCertificateValidationCallback = (_, _, _, _) => true;
        using HttpClient client = new(handler);
        using HttpRequestMessage request = new(HttpMethod.Get, new Uri(host.BaseUri, "/mqtt"))
        {
            Version = version, VersionPolicy = HttpVersionPolicy.RequestVersionExact,
        };
        return await client.SendAsync(request).WaitAsync(s_failSafe);
    }

    /// <summary>
    /// Opens a client WebSocket to the endpoint, offering <c>mqtt</c>.
    /// </summary>
    /// <param name="host">
    /// The host.
    /// </param>
    /// <returns>
    /// The open WebSocket.
    /// </returns>
    private static async Task<ClientWebSocket> ConnectClientWebSocketAsync(ServerHost host)
    {
        ClientWebSocket webSocket = new();
        webSocket.Options.AddSubProtocol("mqtt");
        await webSocket.ConnectAsync(host.WebSocketUri, CancellationToken.None).WaitAsync(s_failSafe);
        return webSocket;
    }

    /// <summary>
    /// Receives one whole message.
    /// </summary>
    /// <param name="webSocket">
    /// The WebSocket.
    /// </param>
    /// <returns>
    /// The message's bytes.
    /// </returns>
    private static async Task<byte[]> ReceiveMessageAsync(ClientWebSocket webSocket)
    {
        byte[] buffer = new byte[1024];
        int count = 0;
        WebSocketReceiveResult result;
        do
        {
            result = await webSocket.ReceiveAsync(new ArraySegment<byte>(buffer, count, buffer.Length - count), CancellationToken.None).WaitAsync(s_failSafe);
            count += result.Count;
        }
        while (!result.EndOfMessage);

        return buffer[..count];
    }

    /// <summary>
    /// Accepts a WebSocket with the application's options and reads from it until it closes; the control endpoint
    /// of the keep-alive test.
    /// </summary>
    /// <param name="context">
    /// The request.
    /// </param>
    /// <returns>
    /// A task that completes when the WebSocket has closed.
    /// </returns>
    private static async Task HoldWebSocketAsync(HttpContext context)
    {
        using WebSocket webSocket = await context.WebSockets.AcceptWebSocketAsync();
        byte[] buffer = new byte[64];
        try
        {
            while ((await webSocket.ReceiveAsync(buffer, context.RequestAborted)).MessageType != WebSocketMessageType.Close)
            {
            }
        }
        catch (Exception exception) when (exception is WebSocketException or OperationCanceledException)
        {
            // The test ended the connection without a close.
        }
    }

    /// <summary>
    /// Creates a self-signed certificate for 127.0.0.1, loaded so that the system's TLS (SChannel on Windows, which
    /// MsQuic uses) can use its key.
    /// </summary>
    /// <returns>
    /// The certificate.
    /// </returns>
    private static X509Certificate2 CreateLoopbackCertificate()
    {
        using RSA key = RSA.Create(2048);
        CertificateRequest request = new("CN=localhost", key, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        SubjectAlternativeNameBuilder names = new();
        names.AddIpAddress(IPAddress.Loopback);
        names.AddDnsName("localhost");
        request.CertificateExtensions.Add(names.Build());
        request.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension([new Oid("1.3.6.1.5.5.7.3.1")], critical: false));
        using X509Certificate2 ephemeral = request.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(1));
        return X509CertificateLoader.LoadPkcs12(ephemeral.Export(X509ContentType.Pfx), password: null);
    }

    /// <summary>
    /// A handler that accepts everything and records, in <see cref="IMqttWireHandler.OnClosed"/>, why the connection
    /// ended and whether the call runs inside an abort of a WebSocket, on the thread that aborts.
    /// </summary>
    /// <param name="watch">
    /// The watch of the application's WebSockets.
    /// </param>
    private sealed class ClosedRecordingHandler(AbortWatch watch) : IMqttWireHandler
    {
        /// <summary>
        /// Completed by the first <see cref="OnClosed"/>.
        /// </summary>
        private readonly TaskCompletionSource<(MqttCloseReason Reason, bool InsideAnAbort)> _closed =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>
        /// Gets a task that completes with the reason of the first <see cref="OnClosed"/> and whether it ran inside
        /// an abort.
        /// </summary>
        public Task<(MqttCloseReason Reason, bool InsideAnAbort)> Closed => _closed.Task;

        /// <summary>
        /// Accepts every client.
        /// </summary>
        /// <param name="request">
        /// The CONNECT.
        /// </param>
        /// <param name="cancellationToken">
        /// Not used.
        /// </param>
        /// <returns>
        /// Accept, with no state.
        /// </returns>
        public ValueTask<MqttConnectResult> OnConnectAsync(MqttConnectRequest request, CancellationToken cancellationToken) =>
            ValueTask.FromResult(MqttConnectResult.Accept());

        /// <summary>
        /// Lets every connection in.
        /// </summary>
        /// <param name="connection">
        /// The new connection.
        /// </param>
        /// <param name="replaces">
        /// The connection it replaces, or <see langword="null"/>.
        /// </param>
        /// <returns>
        /// <see cref="MqttAdmitResult.Accepted"/>.
        /// </returns>
        public MqttAdmitResult OnAdmit(MqttWireConnection connection, MqttWireConnection? replaces) => MqttAdmitResult.Accepted;

        /// <summary>
        /// Grants every subscription.
        /// </summary>
        /// <param name="connection">
        /// The connection.
        /// </param>
        /// <param name="topicFilter">
        /// The filter.
        /// </param>
        /// <param name="requested">
        /// The QoS asked for.
        /// </param>
        /// <returns>
        /// The grant at <paramref name="requested"/>.
        /// </returns>
        public MqttSubscribeResult OnSubscribe(MqttWireConnection connection, string topicFilter, MqttQoS requested) =>
            MqttSubscribeResult.Granted(requested);

        /// <summary>
        /// Does nothing.
        /// </summary>
        /// <param name="connection">
        /// The connection.
        /// </param>
        /// <param name="topicFilter">
        /// The filter.
        /// </param>
        public void OnUnsubscribe(MqttWireConnection connection, string topicFilter)
        {
        }

        /// <summary>
        /// Accepts every publish.
        /// </summary>
        /// <param name="connection">
        /// The connection.
        /// </param>
        /// <param name="publish">
        /// The message.
        /// </param>
        /// <returns>
        /// <see cref="MqttPublishResult.Accepted"/>.
        /// </returns>
        public MqttPublishResult OnPublish(MqttWireConnection connection, in MqttPublish publish) => MqttPublishResult.Accepted;

        /// <summary>
        /// Records why the connection ended and whether this call runs inside an abort.
        /// </summary>
        /// <param name="connection">
        /// The connection that ended.
        /// </param>
        /// <param name="reason">
        /// Why it ended.
        /// </param>
        public void OnClosed(MqttWireConnection connection, MqttCloseReason reason) =>
            _closed.TrySetResult((reason, watch.IsCurrentThreadAborting));
    }
}
