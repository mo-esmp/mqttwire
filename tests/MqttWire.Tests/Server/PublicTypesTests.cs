using System.Text;
using Microsoft.AspNetCore.Http;
using MqttWire.Server;
using MqttWire.Server.Internal;
using MqttWire.Tests.Client;
using MqttWire.Tests.Codec;

namespace MqttWire.Tests.Server;

/// <summary>
/// Tests of the public value types of the server and of <see cref="ReasonStrings"/>.
/// </summary>
public sealed class PublicTypesTests
{
    /// <summary>
    /// Proves that every result type refuses a reason code below 0x80 with an out-of-range exception.
    /// </summary>
    [Fact]
    public void Refuse_ReasonCodeBelow0x80_Throws()
    {
        // Arrange
        MqttReasonCode success = MqttReasonCode.Success;

        // Act
        Action connect = () => MqttConnectResult.Refuse(success);
        Action admit = () => MqttAdmitResult.Refuse(success);
        Action subscribe = () => MqttSubscribeResult.Refuse(success);
        Action publish = () => MqttPublishResult.Refuse(success);

        // Assert
        connect.ShouldThrow<ArgumentOutOfRangeException>();
        admit.ShouldThrow<ArgumentOutOfRangeException>();
        subscribe.ShouldThrow<ArgumentOutOfRangeException>();
        publish.ShouldThrow<ArgumentOutOfRangeException>();
    }

    /// <summary>
    /// Proves that a reason string no packet can carry (U+0000, an unpaired surrogate) is refused with an argument
    /// exception where the host hands it over: by every refusal and by <see cref="MqttWireConnection.Close"/>.
    /// </summary>
    /// <remarks>
    /// A fact rather than a theory: xUnit would carry an unpaired surrogate in inline data as U+FFFD.
    /// </remarks>
    [Fact]
    public void Refuse_UnencodableReasonString_Throws()
    {
        // Arrange
        MqttReasonCode code = MqttReasonCode.NotAuthorized;
        MqttWireConnection connection = new(ServerHarness.Create().Server, new DefaultHttpContext());
        string[] reasonStrings = ["a\0b", "a\uD800b", "a\uDC00", "\uDE00\uD83D"];

        foreach (string reasonString in reasonStrings)
        {
            // Act
            Action connect = () => MqttConnectResult.Refuse(code, reasonString);
            Action admit = () => MqttAdmitResult.Refuse(code, reasonString);
            Action subscribe = () => MqttSubscribeResult.Refuse(code, reasonString);
            Action publish = () => MqttPublishResult.Refuse(code, reasonString);
            Action close = () => connection.Close(code, reasonString);

            // Assert
            connect.ShouldThrow<ArgumentException>().ParamName.ShouldBe("reasonString");
            admit.ShouldThrow<ArgumentException>().ParamName.ShouldBe("reasonString");
            subscribe.ShouldThrow<ArgumentException>().ParamName.ShouldBe("reasonString");
            publish.ShouldThrow<ArgumentException>().ParamName.ShouldBe("reasonString");
            close.ShouldThrow<ArgumentException>().ParamName.ShouldBe("reasonString");
        }
    }

    /// <summary>
    /// Proves that a reason string with a surrogate pair, and no reason string at all, are accepted by every refusal.
    /// </summary>
    [Fact]
    public void Refuse_EncodableReasonString_IsAccepted()
    {
        // Arrange
        string pair = "a😀b";

        // Act
        Action connect = () => MqttConnectResult.Refuse(MqttReasonCode.NotAuthorized, pair);
        Action admit = () => MqttAdmitResult.Refuse(MqttReasonCode.NotAuthorized, pair);
        Action subscribe = () => MqttSubscribeResult.Refuse(MqttReasonCode.NotAuthorized, pair);
        Action publish = () => MqttPublishResult.Refuse(MqttReasonCode.NotAuthorized, reasonString: null);

        // Assert
        connect.ShouldNotThrow();
        admit.ShouldNotThrow();
        subscribe.ShouldNotThrow();
        publish.ShouldNotThrow();
    }

    /// <summary>
    /// Proves that user properties over 1,024 encoded bytes are refused and exactly 1,024 bytes are accepted.
    /// </summary>
    [Fact]
    public void Accept_UserPropertiesOver1024Bytes_Throws()
    {
        // Arrange
        string value = new('v', 100);
        MqttWireUserProperty[] tooMany = new MqttWireUserProperty[11];
        Array.Fill(tooMany, new MqttWireUserProperty("k", value));

        // Nine properties of 106 bytes and one of 70 bytes are exactly 1,024 bytes.
        MqttWireUserProperty[] exact = new MqttWireUserProperty[10];
        Array.Fill(exact, new MqttWireUserProperty("k", value));
        exact[9] = new MqttWireUserProperty("k", new string('v', 64));

        // Act
        Action over = () => MqttConnectResult.Accept(userProperties: tooMany);
        MqttConnectResult accepted = MqttConnectResult.Accept(userProperties: exact);

        // Assert
        over.ShouldThrow<ArgumentException>();
        accepted.IsAccepted.ShouldBeTrue();
        accepted.UserProperties.ShouldBe(exact);
    }

    /// <summary>
    /// Proves that a user property no CONNACK can carry, a name or value with U+0000 or an unpaired surrogate, is
    /// refused when the host accepts with it, so only a host that changes the array afterwards reaches the server's
    /// last resort.
    /// </summary>
    /// <param name="name">
    /// The property's name.
    /// </param>
    /// <param name="value">
    /// The property's value.
    /// </param>
    [Theory]
    [InlineData("k", "bad\0value")]
    [InlineData("bad\0name", "v")]
    [InlineData("k", "lone surrogate")]
    public void Accept_UserPropertyNotEncodable_Throws(string name, string value)
    {
        // Arrange
        // A lone surrogate does not survive the test's attribute, so it is put in here.
        string carried = value == "lone surrogate" ? "a" + (char)0xD800 : value;
        MqttWireUserProperty[] properties = [new MqttWireUserProperty(name, carried)];

        // Act
        Action accept = () => MqttConnectResult.Accept(userProperties: properties);

        // Assert
        accept.ShouldThrow<ArgumentException>();
    }

    /// <summary>
    /// Proves that a granted QoS outside 0 to 2 is refused.
    /// </summary>
    [Fact]
    public void Granted_QoSOutOfRange_Throws()
    {
        // Arrange
        MqttQoS outOfRange = (MqttQoS)3;

        // Act
        Action act = () => MqttSubscribeResult.Granted(outOfRange);

        // Assert
        act.ShouldThrow<ArgumentOutOfRangeException>();
    }

    /// <summary>
    /// Proves that a grant of QoS 1 carries reason code 0x01 and a grant of QoS 2 carries 0x02, the codes a SUBACK
    /// sends for them.
    /// </summary>
    [Fact]
    public void Granted_AtLeastOnceAndExactlyOnce_Carry0x01And0x02()
    {
        // Arrange
        // Act
        MqttSubscribeResult atLeastOnce = MqttSubscribeResult.Granted(MqttQoS.AtLeastOnce);
        MqttSubscribeResult exactlyOnce = MqttSubscribeResult.Granted(MqttQoS.ExactlyOnce);

        // Assert
        atLeastOnce.IsAccepted.ShouldBeTrue();
        ((byte)atLeastOnce.ReasonCode).ShouldBe((byte)0x01);
        exactlyOnce.IsAccepted.ShouldBeTrue();
        ((byte)exactlyOnce.ReasonCode).ShouldBe((byte)0x02);
    }

    /// <summary>
    /// Proves that every result type refuses 0x7F, the last code below the failures, with an out-of-range exception,
    /// and accepts 0x80, the first failure, as a refusal.
    /// </summary>
    [Fact]
    public void Refuse_ReasonCodesAtTheBoundary_Throws0x7FAndAccepts0x80()
    {
        // Arrange
        MqttReasonCode below = (MqttReasonCode)0x7F;
        MqttReasonCode first = (MqttReasonCode)0x80;

        // Act
        Action connectBelow = () => MqttConnectResult.Refuse(below);
        Action admitBelow = () => MqttAdmitResult.Refuse(below);
        Action subscribeBelow = () => MqttSubscribeResult.Refuse(below);
        Action publishBelow = () => MqttPublishResult.Refuse(below);
        MqttConnectResult connect = MqttConnectResult.Refuse(first);
        MqttAdmitResult admit = MqttAdmitResult.Refuse(first);
        MqttSubscribeResult subscribe = MqttSubscribeResult.Refuse(first);
        MqttPublishResult publish = MqttPublishResult.Refuse(first);

        // Assert
        connectBelow.ShouldThrow<ArgumentOutOfRangeException>();
        admitBelow.ShouldThrow<ArgumentOutOfRangeException>();
        subscribeBelow.ShouldThrow<ArgumentOutOfRangeException>();
        publishBelow.ShouldThrow<ArgumentOutOfRangeException>();
        connect.IsAccepted.ShouldBeFalse();
        connect.ReasonCode.ShouldBe(first);
        admit.IsAccepted.ShouldBeFalse();
        admit.ReasonCode.ShouldBe(first);
        subscribe.IsAccepted.ShouldBeFalse();
        subscribe.ReasonCode.ShouldBe(first);
        publish.IsAccepted.ShouldBeFalse();
        publish.ReasonCode.ShouldBe(first);
    }

    /// <summary>
    /// Proves that the not-authorized subscribe result carries reason code 0x87 and no reason string.
    /// </summary>
    [Fact]
    public void NotAuthorized_Always_Carries0x87AndNoReasonString()
    {
        // Arrange
        // Act
        MqttSubscribeResult result = MqttSubscribeResult.NotAuthorized;

        // Assert
        result.IsAccepted.ShouldBeFalse();
        result.ReasonCode.ShouldBe(MqttReasonCode.NotAuthorized);
        result.ReasonString.ShouldBeNull();
    }

    /// <summary>
    /// Proves that the default value of each result type fails closed: it is a refusal with 0x87 (not authorized)
    /// and no reason string, so a handler that returns <c>default</c> lets nobody in and nothing through.
    /// </summary>
    [Fact]
    public void Default_EachResult_RefusesWith0x87()
    {
        // Arrange
        // Act
        MqttPublishResult publish = default;
        MqttAdmitResult admit = default;
        MqttConnectResult connect = default;
        MqttSubscribeResult subscribe = default;

        // Assert
        publish.IsAccepted.ShouldBeFalse();
        publish.ReasonCode.ShouldBe(MqttReasonCode.NotAuthorized);
        publish.ReasonString.ShouldBeNull();
        admit.IsAccepted.ShouldBeFalse();
        admit.ReasonCode.ShouldBe(MqttReasonCode.NotAuthorized);
        admit.ReasonString.ShouldBeNull();
        connect.IsAccepted.ShouldBeFalse();
        connect.ReasonCode.ShouldBe(MqttReasonCode.NotAuthorized);
        connect.ReasonString.ShouldBeNull();
        connect.State.ShouldBeNull();
        subscribe.IsAccepted.ShouldBeFalse();
        subscribe.ReasonCode.ShouldBe(MqttReasonCode.NotAuthorized);
        subscribe.ReasonString.ShouldBeNull();
    }

    /// <summary>
    /// Proves that only the factory members accept: each is accepted, with the reason code its packet carries.
    /// </summary>
    [Fact]
    public void Accept_EachResultsFactory_IsAccepted()
    {
        // Arrange
        // Act
        MqttPublishResult publish = MqttPublishResult.Accepted;
        MqttAdmitResult admit = MqttAdmitResult.Accepted;
        MqttConnectResult connect = MqttConnectResult.Accept();
        MqttSubscribeResult subscribe = MqttSubscribeResult.Granted(MqttQoS.AtMostOnce);

        // Assert
        publish.IsAccepted.ShouldBeTrue();
        publish.ReasonCode.ShouldBe(MqttReasonCode.Success);
        admit.IsAccepted.ShouldBeTrue();
        admit.ReasonCode.ShouldBe(MqttReasonCode.Success);
        connect.IsAccepted.ShouldBeTrue();
        connect.ReasonCode.ShouldBe(MqttReasonCode.Success);
        subscribe.IsAccepted.ShouldBeTrue();
        subscribe.ReasonCode.ShouldBe(MqttReasonCode.Success);
    }

    /// <summary>
    /// Proves that a handler that returns <c>default</c> from <c>OnConnectAsync</c> authenticates nobody: the CONNECT
    /// is refused with CONNACK 0x87 and the admission is not reached.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task OnConnectAsync_ReturnsTheDefaultValue_RefusesWith0x87()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        harness.Handler.Connect = static (_, _) => ValueTask.FromResult<MqttConnectResult>(default);
        TestConnection connection = harness.Accept();

        // Act
        connection.Deliver(ServerPackets.Connect);
        byte[] written = connection.TakeWritten();
        connection.Link.EndFromPeer();
        await connection.RunEnded();

        // Assert
        written.ShouldBe(ServerPackets.ConnAckRefused(MqttReasonCode.NotAuthorized));
        harness.Handler.Calls.ShouldBe(["OnConnectAsync c1"]);
        harness.Server.ConnectionCount.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a handler that returns <c>default</c> from <c>OnAdmit</c> admits nobody: the CONNECT is refused
    /// with CONNACK 0x87, the connection is in no table and gets no <c>OnClosed</c>.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task OnAdmit_ReturnsTheDefaultValue_RefusesWith0x87()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        harness.Handler.Admit = static (_, _) => default;
        TestConnection connection = harness.Accept();

        // Act
        connection.Deliver(ServerPackets.Connect);
        byte[] written = connection.TakeWritten();
        connection.Link.EndFromPeer();
        await connection.RunEnded();

        // Assert
        written.ShouldBe(ServerPackets.ConnAckRefused(MqttReasonCode.NotAuthorized));
        harness.Handler.Calls.ShouldBe(["OnConnectAsync c1", "OnAdmit c1 replaces=-"]);
        harness.Server.ConnectionCount.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a handler that returns <c>default</c> from <c>OnSubscribe</c> grants nothing: the SUBACK carries
    /// 0x87 for the filter.
    /// </summary>
    [Fact]
    public void OnSubscribe_ReturnsTheDefaultValue_RefusesTheFilterWith0x87()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        harness.Handler.Subscribe = static (_, _, _) => default;
        TestConnection connection = harness.Connect();

        // Act
        connection.Deliver(ServerPackets.Subscribe);

        // Assert
        connection.TakeWritten().ShouldBe(ServerPackets.SubAckNotAuthorized);
    }

    /// <summary>
    /// Proves that a handler that returns <c>default</c> from <c>OnPublish</c> accepts nothing: a QoS 1 publish is
    /// answered with PUBACK 0x87.
    /// </summary>
    [Fact]
    public void OnPublish_ReturnsTheDefaultValue_RefusesWith0x87()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        harness.Handler.Publish = static (MqttWireConnection _, in MqttPublish _) => default;
        TestConnection connection = harness.Connect();

        // Act
        connection.Deliver(ClientPackets.PublishQoS1);

        // Assert
        connection.TakeWritten().ShouldBe(Hex.Parse("40 03 00 01 87"));
    }

    /// <summary>
    /// Proves that a refusal keeps its reason code and string, cutting the string to the limit.
    /// </summary>
    [Fact]
    public void Refuse_ReasonCodeAndString_AreKept()
    {
        // Arrange
        string longString = new('x', 1_000);

        // Act
        MqttPublishResult result = MqttPublishResult.Refuse(MqttReasonCode.QuotaExceeded, "full");
        MqttPublishResult cut = MqttPublishResult.Refuse(MqttReasonCode.QuotaExceeded, longString);

        // Assert
        result.IsAccepted.ShouldBeFalse();
        result.ReasonCode.ShouldBe(MqttReasonCode.QuotaExceeded);
        result.ReasonString.ShouldBe("full");
        cut.ReasonString.ShouldNotBeNull();
        cut.ReasonString.Length.ShouldBe(256);
    }

    /// <summary>
    /// Proves that a topic built from a valid string keeps its UTF-8 bytes.
    /// </summary>
    [Fact]
    public void Create_ValidTopic_KeepsItsUtf8()
    {
        // Arrange
        // Act
        MqttTopic topic = MqttTopic.Create("a/b");

        // Assert
        topic.IsDefault.ShouldBeFalse();
        topic.Utf8.ToArray().ShouldBe(new byte[] { 0x61, 0x2F, 0x62 });
    }

    /// <summary>
    /// Proves that an invalid topic string is refused.
    /// </summary>
    /// <param name="topic">
    /// The invalid topic.
    /// </param>
    [Theory]
    [InlineData("")]
    [InlineData("a/+")]
    [InlineData("a/#")]
    [InlineData("a\0b")]
    public void Create_InvalidTopic_Throws(string topic)
    {
        // Arrange
        // Act
        Action act = () => MqttTopic.Create(topic);

        // Assert
        act.ShouldThrow<ArgumentException>();
    }

    /// <summary>
    /// Proves that a topic with an unpaired surrogate is refused.
    /// </summary>
    [Fact]
    public void Create_UnpairedSurrogate_Throws()
    {
        // Arrange
        string topic = "a\uD800b";

        // Act
        Action act = () => MqttTopic.Create(topic);

        // Assert
        act.ShouldThrow<ArgumentException>();
    }

    /// <summary>
    /// Proves that a topic of 65,527 bytes is refused and one of 65,526 bytes is accepted.
    /// </summary>
    [Fact]
    public void Create_TopicOf65527Bytes_Throws()
    {
        // Arrange
        string tooLong = new('a', 65_527);
        string longest = new('a', 65_526);

        // Act
        Action act = () => MqttTopic.Create(tooLong);
        MqttTopic accepted = MqttTopic.Create(longest);

        // Assert
        act.ShouldThrow<ArgumentException>();
        accepted.Utf8.Length.ShouldBe(65_526);
    }

    /// <summary>
    /// Proves that a topic built from bytes copies them.
    /// </summary>
    [Fact]
    public void Create_Utf8Span_CopiesTheBytes()
    {
        // Arrange
        byte[] source = [0x61, 0x2F, 0x62];

        // Act
        MqttTopic topic = MqttTopic.Create(source);
        source[0] = 0x7A;

        // Assert
        topic.Utf8.ToArray().ShouldBe(new byte[] { 0x61, 0x2F, 0x62 });
    }

    /// <summary>
    /// Proves that a topic built from bytes is checked as a topic name is.
    /// </summary>
    [Fact]
    public void Create_InvalidUtf8Span_Throws()
    {
        // Arrange
        byte[] invalid = [0x61, 0xFF];
        byte[] wildcard = [0x61, 0x2B];
        byte[] empty = [];

        // Act
        Action badBytes = () => MqttTopic.Create(invalid);
        Action badWildcard = () => MqttTopic.Create(wildcard);
        Action noBytes = () => MqttTopic.Create(empty);

        // Assert
        badBytes.ShouldThrow<ArgumentException>();
        badWildcard.ShouldThrow<ArgumentException>();
        noBytes.ShouldThrow<ArgumentException>();
    }

    /// <summary>
    /// Proves that a connection that has not been admitted has no client id and no state, and a send finds it not
    /// open.
    /// </summary>
    [Fact]
    public void TrySend_ConnectionNotAdmitted_ReturnsNotOpen()
    {
        // Arrange
        MqttWireConnection connection = new(ServerHarness.Create().Server, new DefaultHttpContext());

        // Act
        MqttSendResult result = connection.TrySend(MqttTopic.Create("a"), ReadOnlyMemory<byte>.Empty, MqttQoS.AtMostOnce);

        // Assert
        result.ShouldBe(MqttSendResult.NotOpen);
        connection.ClientId.ShouldBeEmpty();
        connection.State.ShouldBeNull();
    }

    /// <summary>
    /// Proves that 300 two-byte characters are cut to 256 bytes.
    /// </summary>
    [Fact]
    public void Cut_600BytesOfTwoByteCharacters_Returns256Bytes()
    {
        // Arrange
        string text = new('é', 300);

        // Act
        string? cut = ReasonStrings.Cut(text);

        // Assert
        cut.ShouldNotBeNull();
        cut.Length.ShouldBe(128);
        Encoding.UTF8.GetByteCount(cut).ShouldBe(256);
    }

    /// <summary>
    /// Proves that 100 three-byte characters are cut to 255 bytes.
    /// </summary>
    [Fact]
    public void Cut_300BytesOfThreeByteCharacters_Returns255Bytes()
    {
        // Arrange
        string text = new('€', 100);

        // Act
        string? cut = ReasonStrings.Cut(text);

        // Assert
        cut.ShouldNotBeNull();
        cut.Length.ShouldBe(85);
        Encoding.UTF8.GetByteCount(cut).ShouldBe(255);
    }

    /// <summary>
    /// Proves that a surrogate pair that straddles the limit is dropped whole.
    /// </summary>
    [Fact]
    public void Cut_SurrogatePairAtTheLimit_IsNotSplit()
    {
        // Arrange
        string text = string.Concat(Enumerable.Repeat("\U0001F600", 64)) + "\U0001F600";

        // Act
        string? cut = ReasonStrings.Cut(text);

        // Assert
        cut.ShouldNotBeNull();
        Encoding.UTF8.GetByteCount(cut).ShouldBe(256);
        cut.Length.ShouldBe(128);
        char.IsLowSurrogate(cut[^1]).ShouldBeTrue();
    }

    /// <summary>
    /// Proves that a pair that would end past the limit is not split: 63 pairs and a pair that starts at byte 252
    /// and ends at 256 fit, but with one extra ASCII character in front the last pair is dropped.
    /// </summary>
    [Fact]
    public void Cut_PairStartingBeforeTheLimit_IsDroppedWhole()
    {
        // Arrange
        string text = "a" + string.Concat(Enumerable.Repeat("\U0001F600", 64));

        // Act
        string? cut = ReasonStrings.Cut(text);

        // Assert
        cut.ShouldNotBeNull();
        Encoding.UTF8.GetByteCount(cut).ShouldBe(253);
        cut.Length.ShouldBe(1 + 126);
    }

    /// <summary>
    /// Proves that a string that fits comes back as the same instance and null stays null.
    /// </summary>
    [Fact]
    public void Cut_ShortString_ReturnsTheSameInstance()
    {
        // Arrange
        string text = "short";

        // Act
        string? cut = ReasonStrings.Cut(text);

        // Assert
        cut.ShouldBeSameAs(text);
        ReasonStrings.Cut(null).ShouldBeNull();
    }
}
