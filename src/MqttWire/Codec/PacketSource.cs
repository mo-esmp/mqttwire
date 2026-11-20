namespace MqttWire.Codec;

/// <summary>
/// Who sent a packet that is being decoded; a few rules differ by direction.
/// </summary>
internal enum PacketSource : byte
{
    /// <summary>
    /// The packet came from a client, so the server is decoding it.
    /// </summary>
    Client,

    /// <summary>
    /// The packet came from a server, so a client is decoding it.
    /// </summary>
    Server,
}
