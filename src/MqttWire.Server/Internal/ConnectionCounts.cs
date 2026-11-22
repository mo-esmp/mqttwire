using System.Runtime.InteropServices;

namespace MqttWire.Server.Internal;

/// <summary>
/// What one connection holds at a moment, for the tests that hold it to its bounds (design 6.13 item 5).
/// </summary>
/// <param name="IncomingRecords">
/// The exchange records of QoS 1 and 2 publishes from the client.
/// </param>
/// <param name="DeliveryRecords">
/// The exchange records of QoS 1 and 2 deliveries to the client.
/// </param>
/// <param name="WaitingDeliveries">
/// The deliveries that wait behind the write in progress.
/// </param>
/// <param name="WaitingControlPackets">
/// The control packets that wait behind the write in progress.
/// </param>
/// <param name="OutgoingBytes">
/// The bytes counted against <see cref="MqttWireServerOptions.MaxOutgoingBytes"/>.
/// </param>
/// <param name="IsWriting">
/// Whether the send path is taken: a write is in progress, or one is about to start.
/// </param>
/// <param name="HoldsReadBuffer">
/// Whether the connection holds a rented read buffer.
/// </param>
[StructLayout(LayoutKind.Auto)]
internal readonly record struct ConnectionCounts(
    int IncomingRecords, int DeliveryRecords, int WaitingDeliveries, int WaitingControlPackets,
    long OutgoingBytes, bool IsWriting, bool HoldsReadBuffer);
