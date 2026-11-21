namespace MqttWire.Client;

/// <summary>
/// What <see cref="ReceiveBuffer.TryFrame"/> found at the start of the buffered bytes.
/// </summary>
internal enum FrameStatus : byte
{
    /// <summary>
    /// A whole packet is buffered.
    /// </summary>
    Done,

    /// <summary>
    /// The packet, or its fixed header, is not whole yet; the buffer has room for the rest of it.
    /// </summary>
    NeedMoreData,

    /// <summary>
    /// The fixed header is malformed: a forbidden type or flags, or a remaining length longer than four bytes.
    /// </summary>
    Malformed,

    /// <summary>
    /// The fixed header announces a packet larger than the client accepts; nothing was rented for it.
    /// </summary>
    PacketTooLarge,
}
