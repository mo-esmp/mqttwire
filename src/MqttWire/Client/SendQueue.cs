using System.Runtime.InteropServices;

namespace MqttWire.Client;

/// <summary>
/// The first-in first-out queue of the writes that wait for their turn on the link: a ring of slots that is created
/// when the first write is queued and doubles when full. A ring that grew past
/// <see cref="ClientLimits.SendQueueRetainedCapacity"/> is released when it empties, so a burst does not raise the
/// client's idle memory for the rest of its connection, while routine contention keeps its ring and allocates
/// nothing. Not thread-safe; the client guards it with a lock of its own.
/// </summary>
/// <remarks>
/// A mutable struct, so that it lives inline in the client and constructing a client allocates nothing for it. It
/// must be used in place, from a field that is not <see langword="readonly"/>, and never copied: a copy would share
/// the ring and see only part of what the other did. The default value holds no array and no write.
/// </remarks>
[StructLayout(LayoutKind.Auto)]
internal struct SendQueue
{
    /// <summary>
    /// The ring of slots, or <see langword="null"/> until the first write is queued.
    /// </summary>
    private QueuedWrite[]? _slots;

    /// <summary>
    /// The index of the oldest write in <see cref="_slots"/>.
    /// </summary>
    private int _head;

    /// <summary>
    /// The number of writes waiting.
    /// </summary>
    private int _count;

    /// <summary>
    /// Gets the number of writes waiting.
    /// </summary>
    public readonly int Count => _count;

    /// <summary>
    /// Gets the number of slots in the ring, or 0 while it holds none.
    /// </summary>
    public readonly int Capacity => _slots?.Length ?? 0;

    /// <summary>
    /// Adds a write after the ones waiting, creating the ring on the first and doubling it when it is full.
    /// </summary>
    /// <param name="write">
    /// The write to queue.
    /// </param>
    public void Enqueue(in QueuedWrite write)
    {
        QueuedWrite[]? slots = _slots;
        if (slots is null)
        {
            slots = new QueuedWrite[ClientLimits.SendQueueFirstCapacity];
            _slots = slots;
        }
        else if (_count == slots.Length)
        {
            slots = Grow(slots);
        }

        int tail = _head + _count;
        if (tail >= slots.Length)
        {
            tail -= slots.Length;
        }

        slots[tail] = write;
        _count++;
    }

    /// <summary>
    /// Appends answers to the newest write waiting when it is an answers write whose buffer has room for them, so
    /// that answers handed over one by one behind a slow write go out together. The bytes are copied; the buffer of
    /// <paramref name="answers"/> is then the caller's to return. Nothing is merged across a write of another kind,
    /// so the order of the writes is kept.
    /// </summary>
    /// <param name="answers">
    /// An answers write (<see cref="SendKind.Answers"/>) that would otherwise be queued.
    /// </param>
    /// <returns>
    /// <see langword="true"/> when the answers were appended to the newest write, which now counts them too;
    /// <see langword="false"/> when none waits, the newest is not answers, or its buffer has no room, and nothing
    /// changed.
    /// </returns>
    public bool TryAppendAnswers(in QueuedWrite answers)
    {
        if (_count == 0)
        {
            return false;
        }

        // A write waits, so the ring exists.
        QueuedWrite[] slots = _slots!;
        int tail = _head + _count - 1;
        if (tail >= slots.Length)
        {
            tail -= slots.Length;
        }

        ref QueuedWrite newest = ref slots[tail];
        if (newest.Kind != SendKind.Answers
            || newest.Buffer is not { } buffer
            || answers.Buffer is not { } appended
            || buffer.Length - newest.Length < answers.Length)
        {
            return false;
        }

        appended.AsSpan(0, answers.Length).CopyTo(buffer.AsSpan(newest.Length));
        newest = new QueuedWrite(buffer, newest.Length + answers.Length, SendKind.Answers,
            newest.Answers + answers.Answers);
        return true;
    }

    /// <summary>
    /// Removes the oldest write. The slot is cleared, so the queue keeps no reference to the write's buffer. When
    /// the last write leaves a ring larger than <see cref="ClientLimits.SendQueueRetainedCapacity"/>, the ring is
    /// released, and the next write to wait creates a small one.
    /// </summary>
    /// <param name="write">
    /// The oldest write, or the default value when none waits.
    /// </param>
    /// <returns>
    /// <see langword="true"/> when a write was removed; <see langword="false"/> when none waits.
    /// </returns>
    public bool TryDequeue(out QueuedWrite write)
    {
        if (_count == 0)
        {
            write = default;
            return false;
        }

        // A write waits, so the ring exists.
        QueuedWrite[] slots = _slots!;
        write = slots[_head];
        slots[_head] = default;
        _head++;
        if (_head == slots.Length)
        {
            _head = 0;
        }

        _count--;
        if (_count == 0 && slots.Length > ClientLimits.SendQueueRetainedCapacity)
        {
            // A burst grew it: give it to the garbage collector rather than hold it for the rest of the connection.
            _slots = null;
            _head = 0;
        }

        return true;
    }

    /// <summary>
    /// Replaces the full ring with one of twice the size, holding the writes in order from index 0.
    /// </summary>
    /// <param name="slots">
    /// The full ring.
    /// </param>
    /// <returns>
    /// The new ring.
    /// </returns>
    private QueuedWrite[] Grow(QueuedWrite[] slots)
    {
        QueuedWrite[] larger = new QueuedWrite[slots.Length * 2];
        int firstPart = slots.Length - _head;
        Array.Copy(sourceArray: slots, sourceIndex: _head, destinationArray: larger, destinationIndex: 0, length: firstPart);
        Array.Copy(sourceArray: slots, sourceIndex: 0, destinationArray: larger, destinationIndex: firstPart, length: _head);
        _slots = larger;
        _head = 0;
        return larger;
    }
}
