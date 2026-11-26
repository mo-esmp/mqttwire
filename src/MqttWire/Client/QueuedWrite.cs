using System.Runtime.InteropServices;

namespace MqttWire.Client;

/// <summary>
/// A write that waits for its turn in the send queue: the buffer to write and what its end has to do.
/// </summary>
/// <param name="buffer">
/// The pooled buffer holding the encoded packets, or <see langword="null"/> only for
/// <see cref="SendKind.WritesEnded"/>.
/// </param>
/// <param name="length">
/// The number of bytes to write from the start of the buffer.
/// </param>
/// <param name="kind">
/// What the write is, for the work its end does besides returning the buffer.
/// </param>
/// <param name="answers">
/// The number of answers in the buffer; 0 unless <paramref name="kind"/> is <see cref="SendKind.Answers"/>.
/// </param>
[StructLayout(LayoutKind.Auto)]
internal readonly struct QueuedWrite(byte[]? buffer, int length, SendKind kind, int answers)
{
    /// <summary>
    /// Gets the pooled buffer holding the encoded packets, or <see langword="null"/> only for
    /// <see cref="SendKind.WritesEnded"/>.
    /// </summary>
    public byte[]? Buffer { get; } = buffer;

    /// <summary>
    /// Gets the number of bytes to write from the start of the buffer.
    /// </summary>
    public int Length { get; } = length;

    /// <summary>
    /// Gets what the write is.
    /// </summary>
    public SendKind Kind { get; } = kind;

    /// <summary>
    /// Gets the number of answers in the buffer; 0 unless <see cref="Kind"/> is <see cref="SendKind.Answers"/>.
    /// </summary>
    public int Answers { get; } = answers;
}
