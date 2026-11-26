namespace MqttWire.Client;

/// <summary>
/// What a packet handed to the send lock is, for the work its write does besides writing: a DISCONNECT is still
/// written while the client closes, a PINGREQ starts its PINGRESP deadline when its write begins, a PUBLISH gives
/// back its share of the queued bytes, and a CONNECT has its credentials cleared before its buffer goes back.
/// </summary>
internal enum SendKind
{
    /// <summary>
    /// A SUBSCRIBE or UNSUBSCRIBE, with nothing to do besides the write.
    /// </summary>
    Other,

    /// <summary>
    /// A PUBLISH, whose length was reserved from the queued publish bytes and is given back when its write ends.
    /// </summary>
    Publish,

    /// <summary>
    /// A PINGREQ, whose PINGRESP deadline starts when its write begins, not when it was queued.
    /// </summary>
    PingRequest,

    /// <summary>
    /// A CONNECT, which may carry a user name and password; its bytes are cleared before the buffer goes back to
    /// the shared pool.
    /// </summary>
    Connect,

    /// <summary>
    /// A DISCONNECT, which is written even though the client is already closing.
    /// </summary>
    Disconnect,

    /// <summary>
    /// The read loop's batched PUBACK, PUBREC, PUBREL and PUBCOMP write; the answers in it are counted against a
    /// cap of waiting answers, and given back when its write ends.
    /// </summary>
    Answers,

    /// <summary>
    /// A marker with no buffer, completed when every write handed over before it has ended.
    /// </summary>
    WritesEnded,
}
