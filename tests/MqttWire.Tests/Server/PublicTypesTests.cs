using System.Text;
using Microsoft.AspNetCore.Http;
using MqttWire.Server;
using MqttWire.Server.Internal;

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
    /// Proves that the default value of each result type is its accepted value.
    /// </summary>
    [Fact]
    public void Default_EachResult_IsAccepted()
    {
        // Arrange
        // Act
        MqttPublishResult publish = default;
        MqttAdmitResult admit = default;
        MqttConnectResult connect = default;
        MqttSubscribeResult subscribe = default;

        // Assert
        publish.IsAccepted.ShouldBeTrue();
        admit.IsAccepted.ShouldBeTrue();
        connect.IsAccepted.ShouldBeTrue();
        connect.State.ShouldBeNull();
        subscribe.IsAccepted.ShouldBeTrue();
        subscribe.ReasonCode.ShouldBe(MqttReasonCode.Success);
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
