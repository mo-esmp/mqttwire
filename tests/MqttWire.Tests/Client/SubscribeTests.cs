using MqttWire.Tests.Codec;

namespace MqttWire.Tests.Client;

/// <summary>
/// Tests subscribing and unsubscribing from the client: what is written, how the acknowledgement completes the
/// request, cancellation of the wait, and the acknowledgements the read loop refuses.
/// </summary>
public sealed class SubscribeTests
{
    /// <summary>
    /// Proves a subscribe writes the SUBSCRIBE and completes on the SUBACK with the QoS the server granted.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task SubscribeAsync_Granted_WritesSubscribeAndReturnsGrantedQoS()
    {
        // Arrange
        ClientHarness harness = await ConnectAsync();
        ValueTask<MqttRequestResult> subscribe = harness.Client.SubscribeAsync("a/+", MqttQoS.AtLeastOnce);
        byte[] written = harness.NextPacket();
        bool completedBeforeSubAck = subscribe.IsCompleted;

        // Act
        harness.Link.Deliver(KnownPackets.SubAck);
        MqttRequestResult result = await subscribe;

        // Assert
        written.ShouldBe(KnownPackets.Subscribe);
        completedBeforeSubAck.ShouldBeFalse();
        result.ShouldBe(new MqttRequestResult(MqttRequestStatus.Completed, MqttReasonCode.GrantedQoS1));
        result.GrantedQoS.ShouldBe(MqttQoS.AtLeastOnce);
    }

    /// <summary>
    /// Proves a subscribe reports the QoS the server granted when it is lower than the one asked for.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task SubscribeAsync_GrantedLess_ReturnsWhatWasGranted()
    {
        // Arrange
        ClientHarness harness = await ConnectAsync();
        ValueTask<MqttRequestResult> subscribe = harness.Client.SubscribeAsync("a/+", MqttQoS.ExactlyOnce);
        harness.NextPacket();

        // Act
        harness.Link.Deliver(Hex.Parse("90 04 00 01 00 00"));
        MqttRequestResult result = await subscribe;

        // Assert
        result.GrantedQoS.ShouldBe(MqttQoS.AtMostOnce);
    }

    /// <summary>
    /// Proves a subscription the server refuses completes with the server's code, is not a success and grants no
    /// QoS.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task SubscribeAsync_Refused_ReturnsTheCode()
    {
        // Arrange
        ClientHarness harness = await ConnectAsync();
        ValueTask<MqttRequestResult> subscribe = harness.Client.SubscribeAsync("a/+", MqttQoS.AtLeastOnce);
        harness.NextPacket();

        // Act
        harness.Link.Deliver(Hex.Parse("90 04 00 01 00 87"));
        MqttRequestResult result = await subscribe;

        // Assert
        result.ShouldBe(new MqttRequestResult(MqttRequestStatus.Completed, MqttReasonCode.NotAuthorized));
        result.IsSuccess.ShouldBeFalse();
        result.GrantedQoS.ShouldBeNull();
        harness.Client.IsConnected.ShouldBeTrue();
    }

    /// <summary>
    /// Proves a filter that is not valid is refused with an <see cref="ArgumentException"/> before anything is
    /// written, and uses no packet identifier.
    /// </summary>
    /// <param name="filter">
    /// The invalid filter.
    /// </param>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Theory]
    [InlineData("")]
    [InlineData("a/#/b")]
    [InlineData("a+")]
    [InlineData("a\0")]
    public async Task SubscribeAsync_InvalidFilter_ThrowsAndWritesNothing(string filter)
    {
        // Arrange
        ClientHarness harness = await ConnectAsync();

        // Act
        Action subscribe = () => harness.Client.SubscribeAsync(filter, MqttQoS.AtLeastOnce);
        Action unsubscribe = () => harness.Client.UnsubscribeAsync(filter);

        // Assert
        subscribe.ShouldThrow<ArgumentException>();
        unsubscribe.ShouldThrow<ArgumentException>();
        harness.Link.TakeWritten().ShouldBeEmpty();
        ValueTask<MqttRequestResult> next = harness.Client.SubscribeAsync("a/+", MqttQoS.AtLeastOnce);
        harness.NextPacket().ShouldBe(KnownPackets.Subscribe);
        harness.Link.Deliver(KnownPackets.SubAck);
        (await next).IsSuccess.ShouldBeTrue();
    }

    /// <summary>
    /// Proves a QoS above 2 is refused with an <see cref="ArgumentOutOfRangeException"/> before anything is written.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task SubscribeAsync_QoSAboveTwo_ThrowsAndWritesNothing()
    {
        // Arrange
        ClientHarness harness = await ConnectAsync();

        // Act
        Action subscribe = () => harness.Client.SubscribeAsync("a/+", (MqttQoS)3);

        // Assert
        subscribe.ShouldThrow<ArgumentOutOfRangeException>();
        harness.Link.TakeWritten().ShouldBeEmpty();
    }

    /// <summary>
    /// Proves a subscribe on a client that is not connected fails at once with
    /// <see cref="MqttRequestStatus.NotConnected"/> and writes nothing.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task SubscribeAsync_NotConnected_FailsAtOnce()
    {
        // Arrange
        ClientHarness harness = ClientHarness.Create();

        // Act
        MqttRequestResult subscribe = await harness.Client.SubscribeAsync("a/+", MqttQoS.AtLeastOnce);
        MqttRequestResult unsubscribe = await harness.Client.UnsubscribeAsync("a/+");

        // Assert
        subscribe.Status.ShouldBe(MqttRequestStatus.NotConnected);
        unsubscribe.Status.ShouldBe(MqttRequestStatus.NotConnected);
    }

    /// <summary>
    /// Proves a subscribe whose packet is over the server's Maximum Packet Size fails at once with
    /// <see cref="MqttRequestStatus.PacketTooLarge"/> and writes nothing.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task SubscribeAsync_OverServerMaximumPacketSize_FailsAtOnce()
    {
        // Arrange
        ClientHarness harness = await ConnectAsync(ClientPackets.ConnAckMaximumPacketSize32);

        // Act
        MqttRequestResult result = await harness.Client.SubscribeAsync(new string('a', 40), MqttQoS.AtLeastOnce);

        // Assert
        result.Status.ShouldBe(MqttRequestStatus.PacketTooLarge);
        harness.Link.TakeWritten().ShouldBeEmpty();
    }

    /// <summary>
    /// Proves a SUBACK with two reason codes, which answers a SUBSCRIBE of one filter, ends the connection with
    /// DISCONNECT 0x82 and completes the subscribe with <see cref="MqttRequestStatus.ConnectionLost"/>.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task SubscribeAsync_SubAckWithTwoCodes_SendsDisconnect82()
    {
        // Arrange
        ClientHarness harness = await ConnectAsync();
        ValueTask<MqttRequestResult> subscribe = harness.Client.SubscribeAsync("a/+", MqttQoS.AtLeastOnce);
        harness.NextPacket();

        // Act
        harness.Link.Deliver(Hex.Parse("90 05 00 01 00 01 01"));
        byte[] disconnect = harness.NextPacket();
        harness.Link.EndFromPeer();
        MqttRequestResult result = await subscribe;

        // Assert
        disconnect.ShouldBe(ClientPackets.DisconnectProtocolError);
        result.Status.ShouldBe(MqttRequestStatus.ConnectionLost);
        harness.Closed.ShouldBe([new MqttClientCloseInfo(MqttClientCloseReason.ProtocolError, MqttReasonCode.ProtocolError, null, null)]);
    }

    /// <summary>
    /// Proves a cancelled wait leaves the identifier in use: the next request gets the next identifier, and the
    /// late SUBACK is accepted without ending the connection.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task SubscribeAsync_Cancelled_KeepsTheIdentifierUntilSubAck()
    {
        // Arrange
        ClientHarness harness = await ConnectAsync();
        using CancellationTokenSource cancellation = new();
        ValueTask<MqttRequestResult> subscribe = harness.Client.SubscribeAsync("a/+", MqttQoS.AtLeastOnce, cancellation.Token);
        harness.NextPacket();

        // Act
        await cancellation.CancelAsync();
        Func<Task> wait = async () => await subscribe;
        await wait.ShouldThrowAsync<OperationCanceledException>();
        ValueTask<MqttRequestResult> unsubscribe = harness.Client.UnsubscribeAsync("a/+");
        byte[] written = harness.NextPacket();
        harness.Link.Deliver(KnownPackets.SubAck);
        harness.Link.Deliver(KnownPackets.UnsubAck);
        MqttRequestResult result = await unsubscribe;

        // Assert
        written.ShouldBe(KnownPackets.Unsubscribe);
        result.ShouldBe(new MqttRequestResult(MqttRequestStatus.Completed, MqttReasonCode.Success));
        harness.Client.IsConnected.ShouldBeTrue();
        harness.Link.TakeWritten().ShouldBeEmpty();
    }

    /// <summary>
    /// Proves a token cancelled before the call throws <see cref="OperationCanceledException"/> with nothing written
    /// and no identifier used.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task SubscribeAsync_AlreadyCancelled_ThrowsAndWritesNothing()
    {
        // Arrange
        ClientHarness harness = await ConnectAsync();
        using CancellationTokenSource cancellation = new();
        await cancellation.CancelAsync();

        // Act
        Func<Task> subscribe = async () => await harness.Client.SubscribeAsync("a/+", MqttQoS.AtLeastOnce, cancellation.Token);
        await subscribe.ShouldThrowAsync<OperationCanceledException>();
        ValueTask<MqttRequestResult> next = harness.Client.SubscribeAsync("a/+", MqttQoS.AtLeastOnce);

        // Assert
        harness.NextPacket().ShouldBe(KnownPackets.Subscribe);
        harness.Link.Deliver(KnownPackets.SubAck);
        (await next).IsSuccess.ShouldBeTrue();
    }

    /// <summary>
    /// Proves a subscribe still waiting when the connection ends completes with
    /// <see cref="MqttRequestStatus.ConnectionLost"/>.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task SubscribeAsync_ConnectionEnds_CompletesWithConnectionLost()
    {
        // Arrange
        ClientHarness harness = await ConnectAsync();
        ValueTask<MqttRequestResult> subscribe = harness.Client.SubscribeAsync("a/+", MqttQoS.AtLeastOnce);
        harness.NextPacket();

        // Act
        harness.Link.EndFromPeer();
        MqttRequestResult result = await subscribe;

        // Assert
        result.Status.ShouldBe(MqttRequestStatus.ConnectionLost);
    }

    /// <summary>
    /// Proves an unsubscribe writes the UNSUBSCRIBE and completes on the UNSUBACK with success.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task UnsubscribeAsync_Acknowledged_WritesUnsubscribeAndCompletes()
    {
        // Arrange
        ClientHarness harness = await ConnectAsync();
        ValueTask<MqttRequestResult> subscribe = harness.Client.SubscribeAsync("a/+", MqttQoS.AtLeastOnce);
        harness.NextPacket();
        harness.Link.Deliver(KnownPackets.SubAck);
        await subscribe;
        ValueTask<MqttRequestResult> unsubscribe = harness.Client.UnsubscribeAsync("a/+");
        byte[] written = harness.NextPacket();
        bool completedBeforeUnsubAck = unsubscribe.IsCompleted;

        // Act
        harness.Link.Deliver(KnownPackets.UnsubAck);
        MqttRequestResult result = await unsubscribe;

        // Assert
        written.ShouldBe(KnownPackets.Unsubscribe);
        completedBeforeUnsubAck.ShouldBeFalse();
        result.ShouldBe(new MqttRequestResult(MqttRequestStatus.Completed, MqttReasonCode.Success));
    }

    /// <summary>
    /// Proves an UNSUBACK for the identifier of a subscribe is an acknowledgement of the wrong kind: the client
    /// sends DISCONNECT 0x82.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Read_UnsubAckForASubscribeIdentifier_SendsDisconnect82()
    {
        // Arrange
        ClientHarness harness = await ConnectAsync();
        ValueTask<MqttRequestResult> subscribe = harness.Client.SubscribeAsync("a/+", MqttQoS.AtLeastOnce);
        harness.NextPacket();

        // Act
        harness.Link.Deliver(Hex.Parse("B0 04 00 01 00 00"));
        byte[] disconnect = harness.NextPacket();
        harness.Link.EndFromPeer();
        MqttRequestResult result = await subscribe;

        // Assert
        disconnect.ShouldBe(ClientPackets.DisconnectProtocolError);
        result.Status.ShouldBe(MqttRequestStatus.ConnectionLost);
    }

    /// <summary>
    /// Proves a SUBACK for the identifier of a publish is an acknowledgement of the wrong kind: the client sends
    /// DISCONNECT 0x82 and the publish completes with <see cref="MqttRequestStatus.ConnectionLost"/>.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Read_SubAckForAPublishIdentifier_SendsDisconnect82()
    {
        // Arrange
        ClientHarness harness = await ConnectAsync();
        ValueTask<MqttRequestResult> publish = harness.Client.PublishAsync("a/b", "hi"u8.ToArray(), MqttQoS.AtLeastOnce);
        harness.NextPacket();

        // Act
        harness.Link.Deliver(KnownPackets.SubAck);
        byte[] disconnect = harness.NextPacket();
        harness.Link.EndFromPeer();
        MqttRequestResult result = await publish;

        // Assert
        disconnect.ShouldBe(ClientPackets.DisconnectProtocolError);
        result.Status.ShouldBe(MqttRequestStatus.ConnectionLost);
        harness.Closed.ShouldBe([new MqttClientCloseInfo(MqttClientCloseReason.ProtocolError, MqttReasonCode.ProtocolError, null, null)]);
    }

    /// <summary>
    /// Creates a client and connects it with the given CONNACK, taking the CONNECT it wrote.
    /// </summary>
    /// <param name="connAck">
    /// The server's CONNACK, or <see langword="null"/> for a plain accepting one.
    /// </param>
    /// <returns>
    /// The harness around the connected client.
    /// </returns>
    private static async Task<ClientHarness> ConnectAsync(byte[]? connAck = null)
    {
        ClientHarness harness = ClientHarness.Create();
        await harness.ConnectAsync(connAck ?? ClientPackets.ConnAckAccepted);
        harness.NextPacket();
        return harness;
    }
}
