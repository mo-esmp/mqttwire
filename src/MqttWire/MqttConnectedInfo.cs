using System.Runtime.InteropServices;

namespace MqttWire;

/// <summary>
/// What <see cref="MqttWireReconnectOptions.OnConnected"/> is told about a connection the reconnecting client has
/// made.
/// </summary>
/// <param name="IsReconnect">
/// <see langword="false"/> for the first connection; <see langword="true"/> for every later one, which is the
/// application's signal to catch up on what was sent while it was away, because nothing of it is kept.
/// </param>
/// <param name="Ack">
/// The server's accepting CONNACK, as <see cref="MqttWireClient.ConnectAsync"/> reported it.
/// </param>
[StructLayout(LayoutKind.Auto)]
public readonly record struct MqttConnectedInfo(bool IsReconnect, MqttConnectAck Ack);
