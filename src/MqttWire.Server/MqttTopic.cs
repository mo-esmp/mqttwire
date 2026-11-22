using System.Buffers;
using System.Runtime.InteropServices;
using MqttWire.Client;
using MqttWire.Codec;
using MqttWire.Server.Internal;

namespace MqttWire.Server;

/// <summary>
/// A topic name encoded once. A host that sends to the same topic often keeps the value; one that does not builds
/// it per call. The default value holds no topic and is not valid for sending.
/// </summary>
[StructLayout(LayoutKind.Auto)]
public readonly struct MqttTopic
{
    /// <summary>
    /// The encoded topic; <see langword="null"/> for the default value.
    /// </summary>
    private readonly byte[]? _utf8;

    /// <summary>
    /// Creates a topic from bytes that are already validated.
    /// </summary>
    /// <param name="utf8">
    /// The topic name as UTF-8; the topic keeps this array.
    /// </param>
    private MqttTopic(byte[] utf8) => _utf8 = utf8;

    /// <summary>
    /// Gets the topic as UTF-8 bytes; empty for the default value.
    /// </summary>
    internal ReadOnlySpan<byte> Utf8 => _utf8;

    /// <summary>
    /// Gets a value indicating whether this is the default value, which holds no topic.
    /// </summary>
    internal bool IsDefault => _utf8 is null;

    /// <summary>
    /// Creates a topic from a string.
    /// </summary>
    /// <param name="topic">
    /// The topic name.
    /// </param>
    /// <returns>
    /// The encoded topic.
    /// </returns>
    /// <exception cref="ArgumentNullException">
    /// The topic is <see langword="null"/>.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// The topic is empty, holds <c>+</c>, <c>#</c> or U+0000, has an unpaired surrogate, or is over
    /// <see cref="ServerLimits.MaxTopicBytes"/> UTF-8 bytes.
    /// </exception>
    public static MqttTopic Create(string topic)
    {
        ArgumentNullException.ThrowIfNull(argument: topic);
        TopicChecks.ThrowIfInvalidTopicName(topic: topic, pool: ArrayPool<byte>.Shared);
        if (FieldSize.StrictUtf8.GetByteCount(topic) > ServerLimits.MaxTopicBytes)
        {
            throw new ArgumentException(
                message: $"The topic is over {ServerLimits.MaxTopicBytes} UTF-8 bytes.", paramName: nameof(topic));
        }

        return new MqttTopic(utf8: FieldSize.StrictUtf8.GetBytes(topic));
    }

    /// <summary>
    /// Creates a topic from UTF-8 bytes, which it copies: the caller may change the source afterwards.
    /// </summary>
    /// <param name="utf8Topic">
    /// The topic name as UTF-8.
    /// </param>
    /// <returns>
    /// The topic.
    /// </returns>
    /// <exception cref="ArgumentException">
    /// The bytes are not valid UTF-8, are empty, hold <c>+</c>, <c>#</c> or U+0000, or are over
    /// <see cref="ServerLimits.MaxTopicBytes"/> bytes.
    /// </exception>
    public static MqttTopic Create(ReadOnlySpan<byte> utf8Topic)
    {
        if (utf8Topic.Length > ServerLimits.MaxTopicBytes)
        {
            throw new ArgumentException(
                message: $"The topic is over {ServerLimits.MaxTopicBytes} UTF-8 bytes.", paramName: nameof(utf8Topic));
        }

        if (!System.Text.Unicode.Utf8.IsValid(value: utf8Topic))
        {
            throw new ArgumentException(message: "The topic is not valid UTF-8.", paramName: nameof(utf8Topic));
        }

        TopicChecks.CheckTopicName(topic: utf8Topic);

        return new MqttTopic(utf8: utf8Topic.ToArray());
    }
}
