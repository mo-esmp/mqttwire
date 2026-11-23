using System.Runtime.InteropServices;

namespace MqttWire.Server.Internal;

/// <summary>
/// The record of one QoS 1 or 2 exchange (design 6.6, plan decision 13): its packet identifier, its phase and when
/// it entered that phase; never a payload. Laid out in field order, it takes 16 bytes. A mutable struct, changed in
/// place in the array of <see cref="ExchangeRecords"/> that holds it.
/// </summary>
[StructLayout(LayoutKind.Sequential)]
internal struct ExchangeRecord
{
    /// <summary>
    /// The packet identifier, from 1 to 65,535.
    /// </summary>
    public ushort PacketIdentifier;

    /// <summary>
    /// What the exchange waits for.
    /// </summary>
    public ExchangePhase Phase;

    /// <summary>
    /// When the exchange entered <see cref="Phase"/>, a timestamp of the server's clock; the acknowledgement deadline
    /// is measured from it.
    /// </summary>
    public long EnteredAt;
}
