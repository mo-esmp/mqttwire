using System.Buffers;
using MqttWire.Codec;

namespace MqttWire.Client;

/// <summary>
/// The read loop's buffer: rented from the pool when bytes arrive, grown to hold one whole packet, and returned
/// whenever it holds no bytes, so an idle connection holds no array. Used by the read loop only; not thread-safe.
/// </summary>
/// <param name="pool">
/// The pool the arrays are rented from and returned to.
/// </param>
/// <param name="maxPacketBytes">
/// The largest packet accepted, fixed header included.
/// </param>
internal sealed class ReceiveBuffer(ArrayPool<byte> pool, int maxPacketBytes)
{
    private byte[]? _array;
    private int _start;
    private int _end;

    /// <summary>
    /// Gets the number of bytes buffered and not yet consumed.
    /// </summary>
    public int Count => _end - _start;

    /// <summary>
    /// Gets the free space after the buffered bytes, renting the first array if none is held.
    /// </summary>
    /// <returns>
    /// The memory the next read fills; never empty. It belongs to that read until the read has ended (6.8).
    /// </returns>
    public Memory<byte> GetReadMemory()
    {
        _array ??= pool.Rent(ClientLimits.FirstReadBufferBytes);
        return _array.AsMemory(_end);
    }

    /// <summary>
    /// Counts bytes a read placed in the memory from <see cref="GetReadMemory"/>.
    /// </summary>
    /// <param name="count">
    /// The number of bytes read.
    /// </param>
    public void Advance(int count) => _end += count;

    /// <summary>
    /// Looks for a whole packet at the start of the buffered bytes. When the fixed header is whole but the packet
    /// is not, the array grows to the packet's size (a new one is rented, the bytes copied, the old one returned),
    /// so the next read has room for the rest.
    /// </summary>
    /// <param name="header">
    /// The packet's fixed header when the result is <see cref="FrameStatus.Done"/>; otherwise unspecified.
    /// </param>
    /// <returns>
    /// <see cref="FrameStatus.Done"/> when a whole packet is buffered; <see cref="FrameStatus.NeedMoreData"/> when
    /// it is not; <see cref="FrameStatus.Malformed"/> for a malformed fixed header;
    /// <see cref="FrameStatus.PacketTooLarge"/> when the header announces more than the maximum, decided before
    /// anything is rented for the packet. Only growing changes the buffer.
    /// </returns>
    public FrameStatus TryFrame(out PacketHeader header)
    {
        ReadOnlySpan<byte> data = _array.AsSpan(_start, Count);
        OperationStatus status = PacketHeader.TryRead(data, out header);
        if (status == OperationStatus.InvalidData)
        {
            return FrameStatus.Malformed;
        }

        if (status != OperationStatus.Done)
        {
            return FrameStatus.NeedMoreData;
        }

        if (header.TotalLength > maxPacketBytes)
        {
            return FrameStatus.PacketTooLarge;
        }

        if (data.Length >= header.TotalLength)
        {
            return FrameStatus.Done;
        }

        // A whole header means bytes are buffered, so an array is held.
        if (header.TotalLength > _array!.Length)
        {
            Grow(header.TotalLength);
        }

        return FrameStatus.NeedMoreData;
    }

    /// <summary>
    /// Gets the whole packet <see cref="TryFrame"/> found.
    /// </summary>
    /// <param name="header">
    /// The header <see cref="TryFrame"/> returned with <see cref="FrameStatus.Done"/>.
    /// </param>
    /// <returns>
    /// The packet, fixed header included: a view valid until the next call that changes the buffer.
    /// </returns>
    public ReadOnlySpan<byte> GetPacket(in PacketHeader header) => _array.AsSpan(_start, header.TotalLength);

    /// <summary>
    /// Drops a handled packet from the start of the buffered bytes.
    /// </summary>
    /// <param name="length">
    /// The packet's total length.
    /// </param>
    public void Consume(int length) => _start += length;

    /// <summary>
    /// Moves what is left of an incomplete packet to the start of the array, or returns the array when nothing is
    /// left. Called after the packets of a read have been handled.
    /// </summary>
    public void Compact()
    {
        if (Count == 0)
        {
            Clear();
            return;
        }

        if (_start > 0)
        {
            _array.AsSpan(_start, Count).CopyTo(_array);
            _end = Count;
            _start = 0;
        }
    }

    /// <summary>
    /// Drops every buffered byte and returns the array, if one is held. The caller must not have a read in
    /// progress into it.
    /// </summary>
    public void Clear()
    {
        byte[]? array = _array;
        _array = null;
        _start = 0;
        _end = 0;
        if (array is not null)
        {
            pool.Return(array);
        }
    }

    /// <summary>
    /// Replaces the array with one that holds a whole packet, keeping the buffered bytes.
    /// </summary>
    /// <param name="packetLength">
    /// The packet's total length.
    /// </param>
    private void Grow(int packetLength)
    {
        byte[] bigger = pool.Rent(packetLength);
        int count = Count;
        _array.AsSpan(_start, count).CopyTo(bigger);
        // Only TryFrame grows, and only with bytes buffered, so an array is held.
        byte[] old = _array!;
        _array = bigger;
        _start = 0;
        _end = count;
        pool.Return(old);
    }
}
