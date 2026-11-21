using System.Net.WebSockets;
using MqttWire.Tests.Codec;

namespace MqttWire.Tests.Client;

/// <summary>
/// Tests how the client connects: the CONNECT it writes and what it makes of the server's answer, or of none.
/// </summary>
public sealed class ConnectTests
{
    /// <summary>
    /// Proves the client writes the known CONNECT and reports an accepting CONNACK as connected.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task ConnectAsync_ServerAccepts_WritesConnectAndReturnsConnected()
    {
        // Arrange
        ClientHarness harness = ClientHarness.Create();

        // Act
        MqttConnectAck ack = await harness.ConnectAsync(ClientPackets.ConnAckAccepted);

        // Assert
        harness.NextPacket().ShouldBe(ClientPackets.Connect);
        ack.IsConnected.ShouldBeTrue();
        ack.Failure.ShouldBeNull();
        ack.ReasonCode.ShouldBe(MqttReasonCode.Success);
        harness.Client.IsConnected.ShouldBeTrue();
        harness.Client.LastSendTimestamp.ShouldBe(harness.Time.GetTimestamp());
    }

    /// <summary>
    /// Proves the CONNACK's reason string and user property reach the caller.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task ConnectAsync_AckWithReasonAndUserProperty_ReturnsThem()
    {
        // Arrange
        ClientHarness harness = ClientHarness.Create();

        // Act
        MqttConnectAck ack = await harness.ConnectAsync(ClientPackets.ConnAckWithReason);

        // Assert
        ack.IsConnected.ShouldBeTrue();
        ack.ReasonString.ShouldBe("x");
        ack.UserProperties.ShouldBe([new MqttWireUserProperty("a", "b")]);
    }

    /// <summary>
    /// Proves a refusing CONNACK is reported with its reason code, the link is closed, and <c>OnClosed</c> is not
    /// called, even once the link has ended.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task ConnectAsync_ServerRefuses_ReturnsRefusalAndClosesTheLink()
    {
        // Arrange
        CountingPool pool = new();
        ClientHarness harness = ClientHarness.Create(pool: pool);

        // Act
        MqttConnectAck ack = await harness.ConnectAsync(ClientPackets.ConnAckBadUserNameOrPassword);
        WebSocketCloseStatus? closedWith = harness.Link.ClosedWith;
        harness.Link.EndFromPeer();

        // Assert
        ack.IsConnected.ShouldBeFalse();
        ack.Failure.ShouldBe(MqttClientCloseReason.ConnectRefused);
        ack.ReasonCode.ShouldBe(MqttReasonCode.BadUserNameOrPassword);
        closedWith.ShouldBe(WebSocketCloseStatus.NormalClosure);
        harness.Client.IsConnected.ShouldBeFalse();
        harness.Closed.ShouldBeEmpty();
        pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves a CONNACK with Session Present, which a clean start forbids, is a protocol error answered with
    /// DISCONNECT 0x82.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task ConnectAsync_SessionPresent_ReturnsProtocolErrorAndSendsDisconnect82()
    {
        // Arrange
        ClientHarness harness = ClientHarness.Create();

        // Act
        MqttConnectAck ack = await harness.ConnectAsync(ClientPackets.ConnAckSessionPresent);

        // Assert
        ack.Failure.ShouldBe(MqttClientCloseReason.ProtocolError);
        harness.NextPacket().ShouldBe(ClientPackets.Connect);
        harness.NextPacket().ShouldBe(ClientPackets.DisconnectProtocolError);
        harness.Link.ClosedWith.ShouldBe(WebSocketCloseStatus.NormalClosure);
        harness.Client.IsConnected.ShouldBeFalse();
    }

    /// <summary>
    /// Proves a connect with no CONNACK times out after ten seconds, not before, and aborts the link.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task ConnectAsync_NoConnAckWithinTenSeconds_ReturnsConnectTimedOut()
    {
        // Arrange
        CountingPool pool = new();
        ClientHarness harness = ClientHarness.Create(pool: pool);
        Task<MqttConnectAck> connect = harness.Client.ConnectAsync();

        // Act
        harness.Time.Advance(TimeSpan.FromSeconds(10) - TimeSpan.FromTicks(1));
        bool endedEarly = harness.Link.IsAborted;
        harness.Time.Advance(TimeSpan.FromTicks(1));
        MqttConnectAck ack = await connect;

        // Assert
        endedEarly.ShouldBeFalse();
        ack.Failure.ShouldBe(MqttClientCloseReason.ConnectTimedOut);
        ack.ReasonCode.ShouldBe(MqttReasonCode.UnspecifiedError);
        harness.Link.IsAborted.ShouldBeTrue();
        harness.Closed.ShouldBeEmpty();
        pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves a server that cannot be reached is reported as unreachable, with the network error.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task ConnectAsync_ConnectorFails_ReturnsUnreachableWithTheException()
    {
        // Arrange
        ClientHarness harness = ClientHarness.Create();
        WebSocketException error = new("unreachable");
        harness.Connector.FailNext(error);

        // Act
        MqttConnectAck ack = await harness.Client.ConnectAsync();

        // Assert
        ack.Failure.ShouldBe(MqttClientCloseReason.Unreachable);
        ack.Exception.ShouldBeSameAs(error);
        ack.ReasonCode.ShouldBe(MqttReasonCode.UnspecifiedError);
        harness.Client.IsConnected.ShouldBeFalse();
        harness.Closed.ShouldBeEmpty();
    }

    /// <summary>
    /// Proves an exception from <c>ConfigureWebSocket</c> propagates out of the connect, before anything is dialled.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task ConnectAsync_ConfigureWebSocketThrows_Throws()
    {
        // Arrange
        InvalidOperationException thrown = new("configure failed");
        await using MqttWireClient client = new(new MqttWireClientOptions
        {
            Uri = new Uri("ws://127.0.0.1:9/mqtt"),
            ClientId = "c1",
            ConfigureWebSocket = _ => throw thrown,
        });

        // Act
        Exception? exception = await Record.ExceptionAsync(() => client.ConnectAsync());

        // Assert
        exception.ShouldBeSameAs(thrown);
        client.IsConnected.ShouldBeFalse();
    }

    /// <summary>
    /// Proves a CONNACK without its property length (step 1 decision 7) is malformed and answered with
    /// DISCONNECT 0x81.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task ConnectAsync_MalformedConnAck_ReturnsProtocolErrorAndSendsDisconnect81()
    {
        // Arrange
        ClientHarness harness = ClientHarness.Create();

        // Act
        MqttConnectAck ack = await harness.ConnectAsync(Hex.Parse("20 02 00 00"));

        // Assert
        ack.Failure.ShouldBe(MqttClientCloseReason.ProtocolError);
        ack.ReasonCode.ShouldBe(MqttReasonCode.UnspecifiedError);
        harness.NextPacket().ShouldBe(ClientPackets.Connect);
        harness.NextPacket().ShouldBe(ClientPackets.DisconnectMalformedPacket);
    }

    /// <summary>
    /// Proves a first packet other than CONNACK is a protocol error answered with DISCONNECT 0x82.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task ConnectAsync_FirstPacketIsNotConnAck_ReturnsProtocolError()
    {
        // Arrange
        ClientHarness harness = ClientHarness.Create();

        // Act
        MqttConnectAck ack = await harness.ConnectAsync(ClientPackets.PingResp);

        // Assert
        ack.Failure.ShouldBe(MqttClientCloseReason.ProtocolError);
        harness.NextPacket().ShouldBe(ClientPackets.Connect);
        harness.NextPacket().ShouldBe(ClientPackets.DisconnectProtocolError);
    }

    /// <summary>
    /// Proves a second connect on the same client is a caller bug that leaves the first connection alone.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task ConnectAsync_CalledTwice_ThrowsInvalidOperationException()
    {
        // Arrange
        ClientHarness harness = ClientHarness.Create();
        await harness.ConnectAsync(ClientPackets.ConnAckAccepted);

        // Act
        Exception? exception = await Record.ExceptionAsync(() => harness.Client.ConnectAsync());

        // Assert
        exception.ShouldBeOfType<InvalidOperationException>();
        harness.Connector.Links.Count.ShouldBe(1);
        harness.Client.IsConnected.ShouldBeTrue();
    }

    /// <summary>
    /// Proves the caller's cancellation while waiting for CONNACK throws and aborts the link.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task ConnectAsync_CallerCancels_ThrowsOperationCanceledAndAbortsTheLink()
    {
        // Arrange
        CountingPool pool = new();
        ClientHarness harness = ClientHarness.Create(pool: pool);
        using CancellationTokenSource cancellation = new();
        Task<MqttConnectAck> connect = harness.Client.ConnectAsync(cancellation.Token);

        // Act
        await cancellation.CancelAsync();
        Exception? exception = await Record.ExceptionAsync(() => connect);

        // Assert
        exception.ShouldBeAssignableTo<OperationCanceledException>();
        harness.Link.IsAborted.ShouldBeTrue();
        harness.Closed.ShouldBeEmpty();
        pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves a CONNACK that arrives one byte per read is put together and accepted.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task ConnectAsync_ConnAckSplitAcrossReads_Connects()
    {
        // Arrange
        ClientHarness harness = ClientHarness.Create();
        Task<MqttConnectAck> connect = harness.Client.ConnectAsync();

        // Act
        harness.Link.DeliverInChunks(ClientPackets.ConnAckWithReason, 1);
        MqttConnectAck ack = await connect;

        // Assert
        ack.IsConnected.ShouldBeTrue();
        ack.ReasonString.ShouldBe("x");
    }
}
