using System.Text;
using Microsoft.Extensions.Logging;
using MqttWire.Server;
using MqttWire.Server.Internal;
using MqttWire.Tests.Client;
using MqttWire.Tests.Codec;

namespace MqttWire.Tests.Server;

/// <summary>
/// Tests of the log (design 6.12): every end of a connection the server decides on is one line with its own event
/// id, at debug level unless it is an error, and no line carries a password, a payload or the host's reason string.
/// </summary>
public sealed class LoggingTests
{
    /// <summary>
    /// The event id of the log entry for a host that did not answer in time.
    /// </summary>
    private const int AuthenticationTimedOutEvent = 4;

    /// <summary>
    /// The event id of the log entry for the keep-alive deadline.
    /// </summary>
    private const int KeepAliveTimedOutEvent = 6;

    /// <summary>
    /// The event id of the log entry for a handler that threw.
    /// </summary>
    private const int HandlerThrewEvent = 15;

    /// <summary>
    /// The event id of the log entry for an answer that came after the deadline.
    /// </summary>
    private const int LateConnectResultEvent = 19;

    /// <summary>
    /// The highest event id of an end of a connection.
    /// </summary>
    private const int LastEndEvent = 18;

    /// <summary>
    /// The password of the CONNECT of the test that looks for secrets in the log.
    /// </summary>
    private const string Password = "s3cret-PASSWORD";

    /// <summary>
    /// The user name of the CONNECT of the test that looks for secrets in the log.
    /// </summary>
    private const string UserName = "USER-MARKER";

    /// <summary>
    /// The payload of every message of the test that looks for secrets in the log.
    /// </summary>
    private const string Payload = "PAYLOAD-MARKER";

    /// <summary>
    /// The reason string the host gives in the test that looks for secrets in the log.
    /// </summary>
    private const string HostReason = "HOST-REASON-MARKER";

    /// <summary>
    /// The topic the host sends to.
    /// </summary>
    private static readonly MqttTopic s_topic = MqttTopic.Create("a/b");

    /// <summary>
    /// Proves that each way a connection can end, driven through the scenario that ends it that way, is logged
    /// exactly once with its own event id and level, that no other end is logged for it, and that the line names the
    /// client, except the connect deadline's, which has no client id yet.
    /// </summary>
    /// <param name="eventId">
    /// The event id of the end, which also picks the scenario.
    /// </param>
    /// <param name="level">
    /// The level the end is logged at.
    /// </param>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Theory]
    [InlineData(1, LogLevel.Debug)]
    [InlineData(2, LogLevel.Debug)]
    [InlineData(3, LogLevel.Debug)]
    [InlineData(4, LogLevel.Error)]
    [InlineData(5, LogLevel.Debug)]
    [InlineData(6, LogLevel.Debug)]
    [InlineData(7, LogLevel.Debug)]
    [InlineData(8, LogLevel.Debug)]
    [InlineData(9, LogLevel.Debug)]
    [InlineData(10, LogLevel.Debug)]
    [InlineData(11, LogLevel.Debug)]
    [InlineData(12, LogLevel.Debug)]
    [InlineData(13, LogLevel.Debug)]
    [InlineData(14, LogLevel.Debug)]
    [InlineData(15, LogLevel.Error)]
    [InlineData(16, LogLevel.Debug)]
    [InlineData(17, LogLevel.Debug)]
    [InlineData(18, LogLevel.Debug)]
    public async Task End_EachReason_IsLoggedOnceWithItsEventId(int eventId, LogLevel level)
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create(eventId == 10 ? static options => options.MaxOutgoingMessages = 3 : null);

        // Act
        await EndAsync(harness, eventId);

        // Assert
        EndEvents(harness).ShouldBe([eventId]);
        (LogLevel Level, EventId Id, string Message, Exception? Exception) entry =
            harness.Log.Entries.Single(entry => entry.Id.Id == eventId);
        entry.Level.ShouldBe(level);
        if (eventId == 5)
        {
            entry.Message.ShouldNotContain("c1");
            entry.Message.ShouldContain("unknown");
        }
        else
        {
            entry.Message.ShouldContain("c1");
        }

        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a handler that throws on the read path is logged once, as an error that carries the exception and
    /// names the method and the client.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task End_HandlerThrew_IsAnErrorWithTheException()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        InvalidOperationException thrown = new("The host's bug.");
        harness.Handler.Subscribe = (_, _, _) => throw thrown;
        TestConnection connection = harness.Connect();

        // Act
        connection.Deliver(ServerPackets.Subscribe);
        await connection.Run;

        // Assert
        EndEvents(harness).ShouldBe([HandlerThrewEvent]);
        (LogLevel Level, EventId Id, string Message, Exception? Exception) entry =
            harness.Log.Entries.Single(entry => entry.Id.Id == HandlerThrewEvent);
        entry.Level.ShouldBe(LogLevel.Error);
        entry.Exception.ShouldBeSameAs(thrown);
        entry.Message.ShouldContain(nameof(IMqttWireHandler.OnSubscribe));
        entry.Message.ShouldContain("c1");
        harness.Handler.Calls.ShouldContain("OnClosed c1 HandlerThrew");
    }

    /// <summary>
    /// Proves that an <c>OnConnectAsync</c> that does not answer within the authentication deadline is logged once,
    /// as an error, and that the end is not also logged as a lost connection.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task End_AuthenticationTimedOut_IsAnError()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TaskCompletionSource<MqttConnectResult> never = new();
        harness.Handler.Connect = (_, _) => new ValueTask<MqttConnectResult>(never.Task);
        TestConnection connection = harness.Accept();
        connection.Deliver(ServerPackets.Connect);

        // Act
        harness.Advance(ServerLimits.AuthenticationDeadline);
        await connection.Run;

        // Assert
        EndEvents(harness).ShouldBe([AuthenticationTimedOutEvent]);
        harness.Log.Entries.Single(entry => entry.Id.Id == AuthenticationTimedOutEvent).Level.ShouldBe(LogLevel.Error);
        connection.TakeWritten().ShouldBe(ServerPackets.ConnAckRefused(MqttReasonCode.ServerUnavailable));
    }

    /// <summary>
    /// Proves that the ordinary ends (a client that disconnects, a lost link, the keep-alive deadline, a takeover, a
    /// host's close) are logged at debug level, and nothing at all is logged as an error.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task End_OrdinaryEnds_AreDebug()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TestConnection disconnects = harness.Connect("c1");
        TestConnection lost = harness.Connect("c2");
        TestConnection closed = harness.Connect("c3");
        TestConnection takenOver = harness.Connect("c4");

        // Act
        disconnects.Deliver(ClientPackets.Disconnect);
        lost.Link.EndFromPeer();
        closed.Connection.ShouldNotBeNull().Close(MqttReasonCode.AdministrativeAction);
        closed.Link.EndFromPeer();
        TestConnection replacement = harness.Connect("c4");
        takenOver.Link.EndFromPeer();
        harness.Advance(TimeSpan.FromSeconds(46));
        replacement.Link.EndFromPeer();
        await Task.WhenAll(disconnects.Run, lost.Run, closed.Run, takenOver.Run, replacement.Run);

        // Assert
        EndEvents(harness).ShouldBe([17, 18, 14, 13, KeepAliveTimedOutEvent], ignoreOrder: true);
        harness.Log.Entries.ShouldAllBe(entry => entry.Level == LogLevel.Debug);
    }

    /// <summary>
    /// Proves that what an <c>OnConnectAsync</c> does after the authentication deadline is logged and discarded:
    /// a late acceptance at debug level, a late exception as an error that carries it, and neither admits anything.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Connect_LateResultAfterTheDeadline_IsLoggedAndDiscarded()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        TaskCompletionSource<MqttConnectResult> lateAccept = new();
        TaskCompletionSource<MqttConnectResult> lateThrow = new();
        harness.Handler.Connect = (request, _) =>
            new ValueTask<MqttConnectResult>(request.ClientId == "c1" ? lateAccept.Task : lateThrow.Task);
        TestConnection accepted = harness.Accept();
        accepted.Deliver(ServerPackets.ConnectFor("c1"));
        TestConnection thrower = harness.Accept();
        thrower.Deliver(ServerPackets.ConnectFor("c2"));
        harness.Advance(ServerLimits.AuthenticationDeadline);
        await Task.WhenAll(accepted.Run, thrower.Run);
        InvalidOperationException thrown = new("The host failed late.");

        // Act
        lateAccept.SetResult(MqttConnectResult.Accept());
        lateThrow.SetException(thrown);

        // Assert
        List<(LogLevel Level, EventId Id, string Message, Exception? Exception)> late =
            [.. harness.Log.Entries.Where(static entry => entry.Id.Id == LateConnectResultEvent)];
        late.Count.ShouldBe(2);
        late.ShouldContain(entry => entry.Level == LogLevel.Debug && entry.Exception == null
            && entry.Message.Contains("c1", StringComparison.Ordinal));
        late.ShouldContain(entry => entry.Level == LogLevel.Error && entry.Exception == thrown
            && entry.Message.Contains("c2", StringComparison.Ordinal));
        harness.Handler.Admitted.ShouldBeEmpty();
        harness.Server.ConnectionCount.ShouldBe(0);
    }

    /// <summary>
    /// Proves that no line of the log carries the password or the user name of a CONNECT, the payload of a publish, of
    /// a delivery, of a refused publish, of a publish whose handler throws, or of a message too large to send, or the
    /// host's reason string, while every one of those is logged about.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Log_AnyLine_ContainsNoPasswordAndNoPayload()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        byte[] payload = Encoding.UTF8.GetBytes(Payload);
        byte[] connect = ServerPackets.BuildConnect(flags: 0xC2, properties: [],
            payload: [.. ServerPackets.Field("c1"u8), .. ServerPackets.Field(Encoding.UTF8.GetBytes(UserName)),
                .. ServerPackets.Field(Encoding.UTF8.GetBytes(Password))]);
        harness.Handler.Publish = static (_, in publish) => publish.GetTopic() switch
        {
            "refused" => MqttPublishResult.Refuse(MqttReasonCode.NotAuthorized, HostReason),
            "throws" => throw new InvalidOperationException("The host's bug."),
            _ => MqttPublishResult.Accepted,
        };
        harness.Handler.Connect = static (request, _) => ValueTask.FromResult(request.ClientId == "c3"
            ? MqttConnectResult.Refuse(MqttReasonCode.BadUserNameOrPassword, HostReason)
            : MqttConnectResult.Accept());
        TestConnection publisher = harness.Connect(connect: connect);
        TestConnection receiver = harness.Connect("c2");
        TestConnection refused = harness.Accept();
        TestConnection closed = harness.Connect("c4");

        // Act
        publisher.Deliver(ServerPackets.BuildPublish(MqttQoS.AtMostOnce, "a/b"u8, packetIdentifier: 0, properties: [], payload));
        publisher.Deliver(ServerPackets.BuildPublish(MqttQoS.AtLeastOnce, "refused"u8, packetIdentifier: 1, properties: [], payload));
        MqttWireConnection server = receiver.Connection.ShouldNotBeNull();
        server.TrySend(s_topic, payload, MqttQoS.AtMostOnce).ShouldBe(MqttSendResult.Taken);
        server.TrySend(s_topic, new byte[1_048_577], MqttQoS.AtMostOnce).ShouldBe(MqttSendResult.TooLarge);
        receiver.Link.EndFromPeer();
        publisher.Deliver(ServerPackets.BuildPublish(MqttQoS.AtLeastOnce, "throws"u8, packetIdentifier: 2, properties: [], payload));
        refused.Deliver(ServerPackets.ConnectFor("c3"));
        closed.Connection.ShouldNotBeNull().Close(MqttReasonCode.AdministrativeAction, HostReason);
        closed.Link.EndFromPeer();
        await Task.WhenAll(publisher.Run, receiver.Run, refused.Run, closed.Run);

        // Assert
        EndEvents(harness).ShouldBe([HandlerThrewEvent, 12, 3, 14], ignoreOrder: true);
        foreach ((LogLevel _, EventId id, string message, Exception? _) in harness.Log.Entries)
        {
            message.ShouldNotContain(Password, customMessage: $"event {id.Id}");
            message.ShouldNotContain(UserName, customMessage: $"event {id.Id}");
            message.ShouldNotContain(Payload, customMessage: $"event {id.Id}");
            message.ShouldNotContain(HostReason, customMessage: $"event {id.Id}");
        }
    }

    /// <summary>
    /// Proves that a logging provider that throws at an end decided by the sweep breaks neither the close nor the
    /// sweep: at the keep-alive deadline both connections are sent DISCONNECT 0x8D, each gets one <c>OnClosed</c>, and
    /// the provider was asked to log both ends.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task End_LoggerThrowsAtTheKeepAliveEnd_StillClosesAndTheSweepGoesOn()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        harness.Log.ThrowForEventId = KeepAliveTimedOutEvent;
        TestConnection first = harness.Connect("c1");
        TestConnection second = harness.Connect("c2");

        // Act
        harness.Advance(TimeSpan.FromSeconds(46));
        byte[] writtenToFirst = first.TakeWritten();
        byte[] writtenToSecond = second.TakeWritten();
        first.Link.EndFromPeer();
        second.Link.EndFromPeer();
        await Task.WhenAll(first.Run, second.Run);

        // Assert
        writtenToFirst.ShouldBe(ServerPackets.Disconnect(MqttReasonCode.KeepAliveTimeout));
        writtenToSecond.ShouldBe(ServerPackets.Disconnect(MqttReasonCode.KeepAliveTimeout));
        harness.Handler.Calls.Where(static call => call.StartsWith("OnClosed", StringComparison.Ordinal))
            .ShouldBe(["OnClosed c1 KeepAliveTimedOut", "OnClosed c2 KeepAliveTimedOut"], ignoreOrder: true);
        EndEvents(harness).ShouldBe([KeepAliveTimedOutEvent, KeepAliveTimedOutEvent]);
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Drives one connection to the end whose event id is given, through the scenario of the earlier tests that ends
    /// it that way, and waits until its run has completed.
    /// </summary>
    /// <param name="harness">
    /// The harness; for event 10 its options allow at most 3 waiting messages.
    /// </param>
    /// <param name="eventId">
    /// The event id of the end.
    /// </param>
    /// <returns>
    /// A task that completes when the connection is Closed.
    /// </returns>
    private static async Task EndAsync(ServerHarness harness, int eventId)
    {
        TestConnection connection;
        switch (eventId)
        {
            case 1:
                connection = harness.Connect();
                connection.Deliver(Hex.Parse("FF FF FF FF FF"));
                break;

            case 2:
                connection = harness.Connect();
                connection.Deliver(Hex.Parse("82 81 80 01 00 01 00"));
                break;

            case 3:
                harness.Handler.Connect = static (_, _) =>
                    ValueTask.FromResult(MqttConnectResult.Refuse(MqttReasonCode.BadUserNameOrPassword, "no"));
                connection = harness.Accept();
                connection.Deliver(ServerPackets.Connect);
                break;

            case 4:
                TaskCompletionSource<MqttConnectResult> never = new();
                harness.Handler.Connect = (_, _) => new ValueTask<MqttConnectResult>(never.Task);
                connection = harness.Accept();
                connection.Deliver(ServerPackets.Connect);
                harness.Advance(ServerLimits.AuthenticationDeadline);
                break;

            case 5:
                connection = harness.Accept();
                connection.Deliver(ServerPackets.Connect.AsSpan(0, 8));
                harness.Advance(ServerLimits.ConnectDeadline);
                break;

            case 6:
                connection = harness.Connect();
                harness.Advance(TimeSpan.FromSeconds(46));
                connection.Link.EndFromPeer();
                break;

            case 7:
                connection = harness.Connect();
                connection.Deliver(Hex.Parse("32 0A 00"));
                harness.Advance(ServerLimits.PartialPacketDeadline);
                connection.Link.EndFromPeer();
                break;

            case 8:
                connection = harness.Connect();
                connection.Link.HoldWrites();
                connection.Connection.ShouldNotBeNull().TrySend(s_topic, "m1"u8.ToArray(), MqttQoS.AtMostOnce);
                harness.Advance(TimeSpan.FromSeconds(16));
                connection.CompleteHeldWrites(new IOException("The socket was aborted."));
                break;

            case 9:
                connection = harness.Connect();
                connection.Connection.ShouldNotBeNull().TrySend(s_topic, "hi"u8.ToArray(), MqttQoS.AtLeastOnce);
                harness.Advance(ServerLimits.AcknowledgementDeadline);
                connection.Link.EndFromPeer();
                break;

            case 10:
                connection = harness.Connect();
                connection.Link.HoldWrites();
                for (int i = 1; i <= 5; i++)
                {
                    connection.Connection.ShouldNotBeNull().TrySend(s_topic, "hi"u8.ToArray(), MqttQoS.AtMostOnce);
                }

                connection.CompleteHeldWrites(new IOException("The socket was aborted."));
                break;

            case 11:
                connection = harness.Connect();
                for (int identifier = 1; identifier <= 257; identifier++)
                {
                    connection.Deliver(ServerPackets.BuildPublish(MqttQoS.ExactlyOnce, "a/b"u8, (ushort)identifier,
                        properties: [], "hi"u8));
                }

                break;

            case 12:
                connection = harness.Connect();
                connection.Connection.ShouldNotBeNull().TrySend(s_topic, new byte[1_048_577], MqttQoS.AtMostOnce);
                connection.Link.EndFromPeer();
                break;

            case 13:
                connection = harness.Connect();
                harness.Connect();
                connection.Link.EndFromPeer();
                break;

            case 14:
                connection = harness.Connect();
                connection.Connection.ShouldNotBeNull().Close(MqttReasonCode.AdministrativeAction);
                connection.Link.EndFromPeer();
                break;

            case 15:
                harness.Handler.Publish = static (_, in _) => throw new InvalidOperationException("The host's bug.");
                connection = harness.Connect();
                connection.Deliver(ClientPackets.PublishQoS1);
                break;

            case 16:
                connection = harness.Connect();
                Task stop = harness.Server.StopAsync();
                connection.Link.EndFromPeer();
                await stop;
                break;

            case 17:
                connection = harness.Connect();
                connection.Deliver(ClientPackets.Disconnect);
                break;

            default:
                connection = harness.Connect();
                connection.Link.EndFromPeer();
                break;
        }

        await connection.Run;
    }

    /// <summary>
    /// Gets the event ids of the ends of connections that were logged, oldest first.
    /// </summary>
    /// <param name="harness">
    /// The harness.
    /// </param>
    /// <returns>
    /// The event ids from 1 to 18.
    /// </returns>
    private static List<int> EndEvents(ServerHarness harness) =>
        [.. harness.Log.Entries.Select(static entry => entry.Id.Id).Where(static id => id is >= 1 and <= LastEndEvent)];
}
