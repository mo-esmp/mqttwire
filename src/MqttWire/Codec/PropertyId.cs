namespace MqttWire.Codec;

/// <summary>
/// The MQTT 5 property identifiers (section 2.2.2.2); the value is the identifier byte on the wire.
/// </summary>
internal enum PropertyId : byte
{
    /// <summary>
    /// Payload format indicator (byte, 0 binary or 1 UTF-8).
    /// </summary>
    PayloadFormatIndicator = 0x01,

    /// <summary>
    /// Message expiry interval in seconds (four-byte integer).
    /// </summary>
    MessageExpiryInterval = 0x02,

    /// <summary>
    /// Content type (UTF-8 string).
    /// </summary>
    ContentType = 0x03,

    /// <summary>
    /// Response topic (UTF-8 string).
    /// </summary>
    ResponseTopic = 0x08,

    /// <summary>
    /// Correlation data (binary).
    /// </summary>
    CorrelationData = 0x09,

    /// <summary>
    /// Subscription identifier (variable byte integer, never 0).
    /// </summary>
    SubscriptionIdentifier = 0x0B,

    /// <summary>
    /// Session expiry interval in seconds (four-byte integer).
    /// </summary>
    SessionExpiryInterval = 0x11,

    /// <summary>
    /// Client identifier assigned by the server (UTF-8 string).
    /// </summary>
    AssignedClientIdentifier = 0x12,

    /// <summary>
    /// Keep alive the server requires, in seconds (two-byte integer).
    /// </summary>
    ServerKeepAlive = 0x13,

    /// <summary>
    /// Extended authentication method (UTF-8 string).
    /// </summary>
    AuthenticationMethod = 0x15,

    /// <summary>
    /// Extended authentication data (binary).
    /// </summary>
    AuthenticationData = 0x16,

    /// <summary>
    /// Whether the server may send reason strings and user properties on failures (byte, 0 or 1).
    /// </summary>
    RequestProblemInformation = 0x17,

    /// <summary>
    /// Delay before the will message is published, in seconds (four-byte integer).
    /// </summary>
    WillDelayInterval = 0x18,

    /// <summary>
    /// Whether the server should return response information (byte, 0 or 1).
    /// </summary>
    RequestResponseInformation = 0x19,

    /// <summary>
    /// Response information for building a response topic (UTF-8 string).
    /// </summary>
    ResponseInformation = 0x1A,

    /// <summary>
    /// Another server the client should use (UTF-8 string).
    /// </summary>
    ServerReference = 0x1C,

    /// <summary>
    /// Human-readable reason for the response (UTF-8 string).
    /// </summary>
    ReasonString = 0x1F,

    /// <summary>
    /// Number of unacknowledged QoS 1 and 2 publishes allowed (two-byte integer, never 0).
    /// </summary>
    ReceiveMaximum = 0x21,

    /// <summary>
    /// Highest topic alias the sender accepts (two-byte integer).
    /// </summary>
    TopicAliasMaximum = 0x22,

    /// <summary>
    /// Topic alias of a publish (two-byte integer, never 0).
    /// </summary>
    TopicAlias = 0x23,

    /// <summary>
    /// Highest QoS the server supports (byte, 0 or 1).
    /// </summary>
    MaximumQoS = 0x24,

    /// <summary>
    /// Whether the server supports retained messages (byte, 0 or 1).
    /// </summary>
    RetainAvailable = 0x25,

    /// <summary>
    /// User property (UTF-8 string pair); may repeat in every packet that has properties.
    /// </summary>
    UserProperty = 0x26,

    /// <summary>
    /// Largest packet the sender accepts, in bytes (four-byte integer, never 0).
    /// </summary>
    MaximumPacketSize = 0x27,

    /// <summary>
    /// Whether the server supports wildcard subscriptions (byte, 0 or 1).
    /// </summary>
    WildcardSubscriptionAvailable = 0x28,

    /// <summary>
    /// Whether the server supports subscription identifiers (byte, 0 or 1).
    /// </summary>
    SubscriptionIdentifierAvailable = 0x29,

    /// <summary>
    /// Whether the server supports shared subscriptions (byte, 0 or 1).
    /// </summary>
    SharedSubscriptionAvailable = 0x2A,
}
