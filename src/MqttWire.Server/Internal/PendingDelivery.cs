using System.Runtime.InteropServices;

namespace MqttWire.Server.Internal;

/// <summary>
/// A message the host handed to <see cref="MqttWireConnection.TrySend"/>, kept until it has been written. The topic
/// and the payload are the host's memory (design 6.8 rule 2): they are referenced, never copied into a pool and
/// never changed.
/// </summary>
/// <param name="Topic">
/// The topic of the message.
/// </param>
/// <param name="Payload">
/// The payload of the message.
/// </param>
/// <param name="QoS">
/// The QoS to deliver with, already lowered to <see cref="MqttWireServerOptions.MaxQoS"/>.
/// </param>
[StructLayout(LayoutKind.Auto)]
internal readonly record struct PendingDelivery(MqttTopic Topic, ReadOnlyMemory<byte> Payload, MqttQoS QoS);
