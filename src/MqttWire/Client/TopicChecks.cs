using System.Buffers;
using MqttWire.Codec;

namespace MqttWire.Client;

/// <summary>
/// The checks a topic name or topic filter passes before a client sends it, shared by
/// <see cref="MqttWireClient"/> and <see cref="MqttWireReconnectingClient"/> so that both refuse the same strings
/// with the same exceptions, connected or not.
/// </summary>
internal static class TopicChecks
{
    /// <summary>
    /// The longest topic or filter, in UTF-8 bytes, that is encoded on the stack; a longer one is encoded into a
    /// rented buffer.
    /// </summary>
    public const int StackTopicBytes = 256;

    /// <summary>
    /// Measures a topic name as UTF-8 and checks that MQTT can carry it as a string.
    /// </summary>
    /// <param name="topic">
    /// The topic name; not <see langword="null"/>.
    /// </param>
    /// <returns>
    /// The number of UTF-8 bytes, without the length prefix.
    /// </returns>
    /// <exception cref="ArgumentException">
    /// The topic is over 65,535 UTF-8 bytes, contains U+0000 or has an unpaired surrogate.
    /// </exception>
    public static int MeasureTopicName(string topic)
    {
        try
        {
            return FieldSize.Utf8String(topic) - FieldSize.LengthPrefix;
        }
        catch (ArgumentException ex)
        {
            throw new ArgumentException($"The topic cannot be sent: {ex.Message}", nameof(topic), ex);
        }
    }

    /// <summary>
    /// Checks that an encoded topic is a topic name a client may publish to.
    /// </summary>
    /// <param name="topic">
    /// The topic name as UTF-8.
    /// </param>
    /// <exception cref="ArgumentException">
    /// The topic is empty or holds the wildcard <c>+</c> or <c>#</c>.
    /// </exception>
    public static void CheckTopicName(ReadOnlySpan<byte> topic)
    {
        if (!Topics.IsValidName(topic))
        {
            throw new ArgumentException("The topic cannot be sent: it is empty or holds the wildcard + or #.", nameof(topic));
        }
    }

    /// <summary>
    /// Measures a topic filter as UTF-8 and checks that MQTT can carry it as a string.
    /// </summary>
    /// <param name="topicFilter">
    /// The topic filter; not <see langword="null"/>.
    /// </param>
    /// <returns>
    /// The number of UTF-8 bytes, without the length prefix.
    /// </returns>
    /// <exception cref="ArgumentException">
    /// The filter is over 65,535 UTF-8 bytes, contains U+0000 or has an unpaired surrogate.
    /// </exception>
    public static int MeasureTopicFilter(string topicFilter)
    {
        try
        {
            return FieldSize.Utf8String(topicFilter) - FieldSize.LengthPrefix;
        }
        catch (ArgumentException ex)
        {
            throw new ArgumentException($"The topic filter cannot be sent: {ex.Message}", nameof(topicFilter), ex);
        }
    }

    /// <summary>
    /// Checks that an encoded topic filter is valid.
    /// </summary>
    /// <param name="topicFilter">
    /// The topic filter as UTF-8.
    /// </param>
    /// <exception cref="ArgumentException">
    /// The filter is empty or misplaces a wildcard.
    /// </exception>
    public static void CheckTopicFilter(ReadOnlySpan<byte> topicFilter)
    {
        if (!Topics.IsValidFilter(topicFilter))
        {
            throw new ArgumentException("The topic filter cannot be sent: it is not a valid topic filter.", nameof(topicFilter));
        }
    }

    /// <summary>
    /// Checks a topic name as <see cref="MqttWireClient.PublishAsync"/> does, without sending it. Allocates nothing:
    /// a long topic is encoded into a buffer rented from the pool and returned.
    /// </summary>
    /// <param name="topic">
    /// The topic name; not <see langword="null"/>.
    /// </param>
    /// <param name="pool">
    /// The pool a long topic's buffer is rented from.
    /// </param>
    /// <exception cref="ArgumentException">
    /// The topic is empty, holds <c>+</c>, <c>#</c> or U+0000, has an unpaired surrogate, or is over 65,535 UTF-8
    /// bytes.
    /// </exception>
    public static void ThrowIfInvalidTopicName(string topic, ArrayPool<byte> pool)
    {
        int length = MeasureTopicName(topic);
        byte[]? rented = length <= StackTopicBytes ? null : pool.Rent(length);
        Span<byte> utf8 = rented is null ? stackalloc byte[StackTopicBytes] : rented;
        try
        {
            CheckTopicName(utf8[..FieldSize.StrictUtf8.GetBytes(topic, utf8)]);
        }
        finally
        {
            if (rented is not null)
            {
                pool.Return(rented);
            }
        }
    }

    /// <summary>
    /// Checks a topic filter as <see cref="MqttWireClient.SubscribeAsync"/> does, without sending it. Allocates
    /// nothing: a long filter is encoded into a buffer rented from the pool and returned.
    /// </summary>
    /// <param name="topicFilter">
    /// The topic filter; not <see langword="null"/>.
    /// </param>
    /// <param name="pool">
    /// The pool a long filter's buffer is rented from.
    /// </param>
    /// <exception cref="ArgumentException">
    /// The filter is empty, misplaces a wildcard, holds U+0000, has an unpaired surrogate, or is over 65,535 UTF-8
    /// bytes.
    /// </exception>
    public static void ThrowIfInvalidTopicFilter(string topicFilter, ArrayPool<byte> pool)
    {
        int length = MeasureTopicFilter(topicFilter);
        byte[]? rented = length <= StackTopicBytes ? null : pool.Rent(length);
        Span<byte> utf8 = rented is null ? stackalloc byte[StackTopicBytes] : rented;
        try
        {
            CheckTopicFilter(utf8[..FieldSize.StrictUtf8.GetBytes(topicFilter, utf8)]);
        }
        finally
        {
            if (rented is not null)
            {
                pool.Return(rented);
            }
        }
    }
}
