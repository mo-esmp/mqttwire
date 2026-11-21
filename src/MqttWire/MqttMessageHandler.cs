namespace MqttWire;

/// <summary>
/// Handles a message the client received.
/// </summary>
/// <param name="message">
/// The message, valid only until the handler returns. Copy what must outlive the call.
/// </param>
public delegate void MqttMessageHandler(in MqttPublish message);
