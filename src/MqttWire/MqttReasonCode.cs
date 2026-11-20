namespace MqttWire;

/// <summary>
/// The MQTT 5 reason codes.
/// </summary>
public enum MqttReasonCode : byte
{
    /// <summary>
    /// The operation succeeded. Also the normal disconnection and granted QoS 0 reason.
    /// </summary>
    Success = 0x00,

    /// <summary>
    /// The subscription is accepted with a maximum QoS of 1.
    /// </summary>
    GrantedQoS1 = 0x01,

    /// <summary>
    /// The subscription is accepted with a maximum QoS of 2.
    /// </summary>
    GrantedQoS2 = 0x02,

    /// <summary>
    /// The client disconnects and wants its will message published.
    /// </summary>
    DisconnectWithWillMessage = 0x04,

    /// <summary>
    /// The message is accepted but no subscriber matches it.
    /// </summary>
    NoMatchingSubscribers = 0x10,

    /// <summary>
    /// No subscription existed for the topic filter.
    /// </summary>
    NoSubscriptionExisted = 0x11,

    /// <summary>
    /// Continue the authentication with another step.
    /// </summary>
    ContinueAuthentication = 0x18,

    /// <summary>
    /// Initiate a re-authentication.
    /// </summary>
    ReAuthenticate = 0x19,

    /// <summary>
    /// The operation failed and no other reason code fits.
    /// </summary>
    UnspecifiedError = 0x80,

    /// <summary>
    /// The packet cannot be parsed according to the specification.
    /// </summary>
    MalformedPacket = 0x81,

    /// <summary>
    /// The packet is well formed but breaks the protocol.
    /// </summary>
    ProtocolError = 0x82,

    /// <summary>
    /// The packet is valid but the implementation does not accept it.
    /// </summary>
    ImplementationSpecificError = 0x83,

    /// <summary>
    /// The server does not support the requested protocol version.
    /// </summary>
    UnsupportedProtocolVersion = 0x84,

    /// <summary>
    /// The client identifier is valid but not allowed by the server.
    /// </summary>
    ClientIdentifierNotValid = 0x85,

    /// <summary>
    /// The user name or password is not accepted.
    /// </summary>
    BadUserNameOrPassword = 0x86,

    /// <summary>
    /// The client is not authorized for the operation.
    /// </summary>
    NotAuthorized = 0x87,

    /// <summary>
    /// The server is not available.
    /// </summary>
    ServerUnavailable = 0x88,

    /// <summary>
    /// The server is busy and the client should try again later.
    /// </summary>
    ServerBusy = 0x89,

    /// <summary>
    /// The client is banned by an administrative action.
    /// </summary>
    Banned = 0x8A,

    /// <summary>
    /// The server is shutting down.
    /// </summary>
    ServerShuttingDown = 0x8B,

    /// <summary>
    /// The authentication method is not supported or does not match the current one.
    /// </summary>
    BadAuthenticationMethod = 0x8C,

    /// <summary>
    /// No packet arrived within one and a half times the keep alive interval.
    /// </summary>
    KeepAliveTimeout = 0x8D,

    /// <summary>
    /// Another connection with the same client identifier took over the session.
    /// </summary>
    SessionTakenOver = 0x8E,

    /// <summary>
    /// The topic filter is well formed but not accepted by the server.
    /// </summary>
    TopicFilterInvalid = 0x8F,

    /// <summary>
    /// The topic name is well formed but not accepted.
    /// </summary>
    TopicNameInvalid = 0x90,

    /// <summary>
    /// The packet identifier is already in use.
    /// </summary>
    PacketIdentifierInUse = 0x91,

    /// <summary>
    /// The packet identifier is not known.
    /// </summary>
    PacketIdentifierNotFound = 0x92,

    /// <summary>
    /// More unacknowledged QoS 1 and 2 messages arrived than the receive maximum allows.
    /// </summary>
    ReceiveMaximumExceeded = 0x93,

    /// <summary>
    /// The topic alias is not valid.
    /// </summary>
    TopicAliasInvalid = 0x94,

    /// <summary>
    /// The packet exceeds the maximum packet size.
    /// </summary>
    PacketTooLarge = 0x95,

    /// <summary>
    /// The message rate is too high.
    /// </summary>
    MessageRateTooHigh = 0x96,

    /// <summary>
    /// An implementation or administrative quota is exceeded.
    /// </summary>
    QuotaExceeded = 0x97,

    /// <summary>
    /// The connection is closed because of an administrative action.
    /// </summary>
    AdministrativeAction = 0x98,

    /// <summary>
    /// The payload does not match the payload format indicator.
    /// </summary>
    PayloadFormatInvalid = 0x99,

    /// <summary>
    /// The server does not support retained messages.
    /// </summary>
    RetainNotSupported = 0x9A,

    /// <summary>
    /// The server does not support the requested QoS.
    /// </summary>
    QoSNotSupported = 0x9B,

    /// <summary>
    /// The client should temporarily use another server.
    /// </summary>
    UseAnotherServer = 0x9C,

    /// <summary>
    /// The client should permanently use another server.
    /// </summary>
    ServerMoved = 0x9D,

    /// <summary>
    /// The server does not support shared subscriptions.
    /// </summary>
    SharedSubscriptionsNotSupported = 0x9E,

    /// <summary>
    /// The connection rate limit is exceeded.
    /// </summary>
    ConnectionRateExceeded = 0x9F,

    /// <summary>
    /// The maximum connection time authorized for the connection is exceeded.
    /// </summary>
    MaximumConnectTime = 0xA0,

    /// <summary>
    /// The server does not support subscription identifiers.
    /// </summary>
    SubscriptionIdentifiersNotSupported = 0xA1,

    /// <summary>
    /// The server does not support wildcard subscriptions.
    /// </summary>
    WildcardSubscriptionsNotSupported = 0xA2,
}
