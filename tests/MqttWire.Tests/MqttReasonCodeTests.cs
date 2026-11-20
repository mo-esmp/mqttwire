namespace MqttWire.Tests;

/// <summary>
/// Pins the values of the public reason codes to the bytes MQTT 5 gives them (MQTT 5, 2.4).
/// </summary>
public sealed class MqttReasonCodeTests
{
    /// <summary>
    /// Proves every reason code, found by its name, has the byte value the standard gives it.
    /// </summary>
    /// <param name="name">
    /// The name of the member.
    /// </param>
    /// <param name="value">
    /// The byte the standard gives it.
    /// </param>
    [Theory]
    [InlineData("Success", (byte)0x00)]
    [InlineData("GrantedQoS1", (byte)0x01)]
    [InlineData("GrantedQoS2", (byte)0x02)]
    [InlineData("DisconnectWithWillMessage", (byte)0x04)]
    [InlineData("NoMatchingSubscribers", (byte)0x10)]
    [InlineData("NoSubscriptionExisted", (byte)0x11)]
    [InlineData("ContinueAuthentication", (byte)0x18)]
    [InlineData("ReAuthenticate", (byte)0x19)]
    [InlineData("UnspecifiedError", (byte)0x80)]
    [InlineData("MalformedPacket", (byte)0x81)]
    [InlineData("ProtocolError", (byte)0x82)]
    [InlineData("ImplementationSpecificError", (byte)0x83)]
    [InlineData("UnsupportedProtocolVersion", (byte)0x84)]
    [InlineData("ClientIdentifierNotValid", (byte)0x85)]
    [InlineData("BadUserNameOrPassword", (byte)0x86)]
    [InlineData("NotAuthorized", (byte)0x87)]
    [InlineData("ServerUnavailable", (byte)0x88)]
    [InlineData("ServerBusy", (byte)0x89)]
    [InlineData("Banned", (byte)0x8A)]
    [InlineData("ServerShuttingDown", (byte)0x8B)]
    [InlineData("BadAuthenticationMethod", (byte)0x8C)]
    [InlineData("KeepAliveTimeout", (byte)0x8D)]
    [InlineData("SessionTakenOver", (byte)0x8E)]
    [InlineData("TopicFilterInvalid", (byte)0x8F)]
    [InlineData("TopicNameInvalid", (byte)0x90)]
    [InlineData("PacketIdentifierInUse", (byte)0x91)]
    [InlineData("PacketIdentifierNotFound", (byte)0x92)]
    [InlineData("ReceiveMaximumExceeded", (byte)0x93)]
    [InlineData("TopicAliasInvalid", (byte)0x94)]
    [InlineData("PacketTooLarge", (byte)0x95)]
    [InlineData("MessageRateTooHigh", (byte)0x96)]
    [InlineData("QuotaExceeded", (byte)0x97)]
    [InlineData("AdministrativeAction", (byte)0x98)]
    [InlineData("PayloadFormatInvalid", (byte)0x99)]
    [InlineData("RetainNotSupported", (byte)0x9A)]
    [InlineData("QoSNotSupported", (byte)0x9B)]
    [InlineData("UseAnotherServer", (byte)0x9C)]
    [InlineData("ServerMoved", (byte)0x9D)]
    [InlineData("SharedSubscriptionsNotSupported", (byte)0x9E)]
    [InlineData("ConnectionRateExceeded", (byte)0x9F)]
    [InlineData("MaximumConnectTime", (byte)0xA0)]
    [InlineData("SubscriptionIdentifiersNotSupported", (byte)0xA1)]
    [InlineData("WildcardSubscriptionsNotSupported", (byte)0xA2)]
    public void Parse_EveryReasonCode_HasTheStandardValue(string name, byte value)
    {
        // Act
        MqttReasonCode code = Enum.Parse<MqttReasonCode>(name);

        // Assert
        ((byte)code).ShouldBe(value);
    }
}
