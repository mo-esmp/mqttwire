namespace MqttWire;

/// <summary>
/// An MQTT 5 user property: a name and value pair of text.
/// </summary>
/// <param name="Name">
/// The name of the property.
/// </param>
/// <param name="Value">
/// The value of the property.
/// </param>
public readonly record struct MqttWireUserProperty(string Name, string Value);
