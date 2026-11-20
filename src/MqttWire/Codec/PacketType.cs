namespace MqttWire.Codec;

/// <summary>
/// The MQTT control packet types; the value is the type nibble of the fixed header.
/// </summary>
internal enum PacketType : byte
{
    /// <summary>
    /// A client asks to connect.
    /// </summary>
    Connect = 1,
    /// <summary>
    /// The server answers a connect request.
    /// </summary>
    ConnAck,
    /// <summary>
    /// A message is published.
    /// </summary>
    Publish,
    /// <summary>
    /// Acknowledges a QoS 1 publish.
    /// </summary>
    PubAck,
    /// <summary>
    /// First answer to a QoS 2 publish: received.
    /// </summary>
    PubRec,
    /// <summary>
    /// Second step of QoS 2: release.
    /// </summary>
    PubRel,
    /// <summary>
    /// Last step of QoS 2: complete.
    /// </summary>
    PubComp,
    /// <summary>
    /// A client asks to subscribe.
    /// </summary>
    Subscribe,
    /// <summary>
    /// The server answers a subscribe request.
    /// </summary>
    SubAck,
    /// <summary>
    /// A client asks to unsubscribe.
    /// </summary>
    Unsubscribe,
    /// <summary>
    /// The server answers an unsubscribe request.
    /// </summary>
    UnsubAck,
    /// <summary>
    /// A keep-alive request.
    /// </summary>
    PingReq,
    /// <summary>
    /// The answer to a keep-alive request.
    /// </summary>
    PingResp,
    /// <summary>
    /// The sender is closing the connection.
    /// </summary>
    Disconnect,
    /// <summary>
    /// An extended authentication exchange.
    /// </summary>
    Auth,
}
