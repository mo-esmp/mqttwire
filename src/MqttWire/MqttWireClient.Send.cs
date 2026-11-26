using MqttWire.Client;
using MqttWire.Codec;
using MqttWire.Link;

namespace MqttWire;

/// <content>
/// The send path (the client's send-path design, section 5, modelled on the server's): every write to the link goes
/// through it, one at a time, in the order the packets were handed over. One flag, <see cref="_writing"/>, says
/// whether someone holds the path: whoever hands a packet over and finds it free takes it and writes on its own
/// thread; everyone else adds the packet to <see cref="_sendQueue"/> and returns. Whoever holds the path, when a
/// write has ended, either takes the next waiting write or clears the flag, in one hold of <see cref="_sendGate"/>.
/// A write that does not complete at once is handed to <see cref="_continuation"/>, and its completion carries the
/// path on. The lock is never held across a write, a completion, a return to the pool or a call into the
/// application, and nothing is rented or encoded under it.
/// </content>
public sealed partial class MqttWireClient
{
    /// <summary>
    /// Guards <see cref="_writing"/>, <see cref="_sendQueue"/>, <see cref="_writesEnded"/>, <see cref="_faulted"/>,
    /// <see cref="_waitingAnswers"/> and <see cref="_answerRoom"/>; never held across a write, a completion or a return
    /// to the pool.
    /// </summary>
    private readonly Lock _sendGate = new();

    /// <summary>
    /// The writes that wait for their turn, oldest first; under <see cref="_sendGate"/>. A mutable struct, kept inline
    /// so that a client that never meets a busy path allocates nothing for it: not <see langword="readonly"/>, so its
    /// members change this field and not a copy, and never passed or returned by value. It holds no write and no ring
    /// until the first write has to wait, so its initializer allocates nothing.
    /// </summary>
    private SendQueue _sendQueue = new();

    /// <summary>
    /// Whether someone holds the send path: a caller writing on its own thread, a write's continuation, or the thread
    /// pool after a hand-over. Set and cleared under <see cref="_sendGate"/>, and cleared only in the hold that found
    /// nothing waiting, so that a write is never left in the queue with nobody to take it.
    /// </summary>
    private bool _writing;

    /// <summary>
    /// What carries the send path on after a write that did not complete at once, and from the thread pool after a
    /// hand-over; created on first need and kept. Used by whoever holds the send path, and replaced only when a write
    /// refused the registration of its continuation, which then stays armed for that write.
    /// </summary>
    private SendPathContinuation? _continuation;

    /// <summary>
    /// Completes with whether the link took the DISCONNECT, once its write has ended or it was dropped; set by
    /// <see cref="SendDisconnectAsync"/> before the DISCONNECT is handed over. A client sends at most one.
    /// </summary>
    private TaskCompletionSource<bool>? _disconnectWritten;

    /// <summary>
    /// Completes once every write handed over before the marker of <see cref="WhenWritesEndedAsync"/> has ended; set
    /// once, under <see cref="_sendGate"/>, when that marker is handed over. From then on nothing more is taken.
    /// </summary>
    private TaskCompletionSource? _writesEnded;

    /// <summary>
    /// Whether the link refused the registration of a write's continuation, which faults the send path for good: the
    /// link is aborted, and nothing is written on it again, a DISCONNECT included, since the refused write may still
    /// be in progress (design 6.8, rule 3). Set once, under <see cref="_sendGate"/>, by the holder that met the
    /// refusal; read under the lock by whoever hands a write over, and by the holders that follow, which take the path
    /// in a hold of that lock.
    /// </summary>
    private bool _faulted;

    /// <summary>
    /// The read loop's answers handed to the send path whose write has not ended, whether they wait in the queue or
    /// are being written (the client's send-path design, section 5, "How many answers may wait"); under
    /// <see cref="_sendGate"/>. Counted up when the answers are handed over, refused ones included, and down when their
    /// write ends or they are given back, so that every hand-over is counted down exactly once. An answers write whose
    /// link refused the registration of its continuation stays counted until that write ends, which it may never do;
    /// the end of the connection releases the read loop all the same.
    /// </summary>
    private int _waitingAnswers;

    /// <summary>
    /// What the read loop waits for when its hand-over took <see cref="_waitingAnswers"/> past
    /// <see cref="ClientLimits.MaxWaitingAnswers"/>, or <see langword="null"/> while it does not wait; under
    /// <see cref="_sendGate"/>. Created in the hold that took the count past the cap; taken, in a hold of the lock,
    /// and completed outside it, by whoever brings the count back to the cap or below, and by whoever records the end
    /// of the connection or meets it recorded (<see cref="ReleaseAnswerRoom"/>). Its continuations run asynchronously,
    /// so the read loop never carries on inside the send path. It allocates, but only for a server that has broken
    /// its window or floods the client with packets that each call for an answer.
    /// </summary>
    private TaskCompletionSource? _answerRoom;

    /// <summary>
    /// Hands a packet to the send path and returns at once: written now on this thread when the path is free, added
    /// to the queue behind the writes in progress otherwise. Once the end has begun, anything but a DISCONNECT is
    /// refused, and once <see cref="WhenWritesEndedAsync"/> has handed its marker over, or the path is faulted
    /// (<see cref="_faulted"/>), everything is: the packet is given back as a write that ended would give it back
    /// (<see cref="GiveBack"/>), and nothing is written.
    /// </summary>
    /// <param name="packet">
    /// The encoded packet in a buffer rented from the client's pool. The send path owns it from now on and returns it
    /// once its write has ended, or at once when it is refused or dropped.
    /// </param>
    /// <param name="length">
    /// The packet's length.
    /// </param>
    /// <param name="kind">
    /// What the packet is: a DISCONNECT is the only packet written after the end began; a PINGREQ starts its PINGRESP
    /// deadline when its write begins; a PUBLISH gives back its reserved queued bytes, and a CONNECT has its
    /// credentials cleared, when its write ends or it is dropped.
    /// </param>
    /// <remarks>
    /// It never throws, but for a bug of the client. A failed write is nobody's error here: it ends the connection
    /// with <see cref="MqttClientCloseReason.WriteFailed"/> and aborts the link. It allocates nothing in the steady
    /// state, also when the packet has to wait or its write does not complete at once; what allocates is the first
    /// use of the queue and of <see cref="SendPathContinuation"/>, which the client keeps, and the growth of the queue.
    /// </remarks>
    private void Send(byte[] packet, int length, SendKind kind) =>
        _ = HandOver(new QueuedWrite(packet, length, kind, answers: 0));

    /// <summary>
    /// Hands the read loop's answers to the send path, as <see cref="Send"/> does, and counts them among the answers
    /// waiting to be written (<see cref="_waitingAnswers"/>), in the same hold of the lock. It returns at once; the
    /// read loop does not wait for their write, unless this hand-over took the count past
    /// <see cref="ClientLimits.MaxWaitingAnswers"/>. It never throws, but for a bug of the client.
    /// </summary>
    /// <param name="packet">
    /// The encoded answers in one rented buffer, which the send path owns from now on.
    /// </param>
    /// <param name="length">
    /// The answers' length in bytes.
    /// </param>
    /// <param name="answers">
    /// The number of answers in the buffer.
    /// </param>
    /// <returns>
    /// <see langword="null"/> when the read loop goes on: the count is at or below the cap, or the answers were
    /// refused, the end having begun. Otherwise <see cref="_answerRoom"/>, which completes once the count is back at
    /// or below the cap, or the connection ends; it may have completed already, when the answers' write, made on
    /// this thread, ended at once.
    /// </returns>
    private TaskCompletionSource? SendAnswers(byte[] packet, int length, int answers) =>
        HandOver(new QueuedWrite(packet, length, SendKind.Answers, answers));

    /// <summary>
    /// Hands the DISCONNECT to the send path, behind every write handed over before it. Called once in a client's
    /// life, by whoever recorded the end with <see cref="EndMode.Close"/>; it allocates its completion.
    /// </summary>
    /// <param name="packet">
    /// The encoded DISCONNECT in a rented buffer, which the send path owns from now on.
    /// </param>
    /// <param name="length">
    /// The packet's length.
    /// </param>
    /// <returns>
    /// A task that completes once the DISCONNECT's write has ended, with <see langword="true"/> when the link took
    /// it. A link whose own close has started takes it without sending it (<see cref="IMqttLink.WriteAsync"/>), so
    /// <see langword="true"/> does not promise that it was sent. <see langword="false"/> when it was not written,
    /// because <see cref="WhenWritesEndedAsync"/> had already handed its marker over or the link had refused the
    /// registration of a write's continuation, this one's or an earlier one's, or when the write failed, which ended
    /// the connection with <see cref="MqttClientCloseReason.WriteFailed"/> and aborted the link. It never fails, and
    /// its continuations run asynchronously.
    /// </returns>
    private Task<bool> SendDisconnectAsync(byte[] packet, int length)
    {
        TaskCompletionSource<bool> written = new(TaskCreationOptions.RunContinuationsAsynchronously);

        // In place before the DISCONNECT is handed over: the hold of the send path's lock publishes it to whoever
        // ends the DISCONNECT's write.
        Volatile.Write(ref _disconnectWritten, written);
        Send(packet, length, SendKind.Disconnect);
        return written.Task;
    }

    /// <summary>
    /// Hands the send path a marker behind every write handed over so far, and from then on refuses every write. The
    /// marker is completed by whoever holds the path when it takes it, or at once when nobody holds it. A second call
    /// hands nothing over and returns the first call's task.
    /// </summary>
    /// <returns>
    /// A task that completes once every write handed over before the marker has ended and returned its buffer. A write
    /// whose link refused the registration of its continuation is not waited for: the link may never end it. The task
    /// never fails, and its continuations run asynchronously.
    /// </returns>
    private Task WhenWritesEndedAsync()
    {
        TaskCompletionSource marker;
        lock (_sendGate)
        {
            if (_writesEnded is { } handedOver)
            {
                return handedOver.Task;
            }

            marker = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            _writesEnded = marker;
            if (_writing)
            {
                _sendQueue.Enqueue(new QueuedWrite(buffer: null, length: 0, SendKind.WritesEnded, answers: 0));
                return marker.Task;
            }
        }

        // Nobody holds the path, so every write handed over before the marker has ended.
        marker.TrySetResult();
        return marker.Task;
    }

    /// <summary>
    /// The one hold of the lock in which a write handed over is refused, added to the queue, or taken with the send
    /// path by this caller, who then writes it, and whatever waits behind it, on this thread
    /// (<see cref="RunSendPath"/>). In the same hold, the answers of a <see cref="SendKind.Answers"/> write are counted
    /// among those waiting (<see cref="_waitingAnswers"/>), refused ones too, since their give-back counts them down;
    /// and when a write that is not refused takes the count past <see cref="ClientLimits.MaxWaitingAnswers"/>, the
    /// read loop's wait is created (<see cref="_answerRoom"/>). A refused write is given back outside the lock
    /// (<see cref="GiveBack"/>).
    /// </summary>
    /// <param name="write">
    /// The write handed over; its buffer belongs to the send path from now on.
    /// </param>
    /// <returns>
    /// <see cref="_answerRoom"/> when this write took the waiting answers past the cap; <see langword="null"/>
    /// otherwise, and always for a write that is not answers.
    /// </returns>
    private TaskCompletionSource? HandOver(in QueuedWrite write)
    {
        bool refused = false;
        TaskCompletionSource? room = null;
        lock (_sendGate)
        {
            _waitingAnswers += write.Answers;
            if (_writesEnded is not null || _faulted || IsDropped(write.Kind))
            {
                refused = true;
            }
            else
            {
                if (_waitingAnswers > ClientLimits.MaxWaitingAnswers)
                {
                    // Only the read loop hands answers over, so at most one wait exists; it is created here, where
                    // the end, recorded before this hold, would have refused the write, and recorded after it, takes
                    // it (ReleaseAnswerRoom).
                    room = _answerRoom ??= new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                }

                if (_writing)
                {
                    _sendQueue.Enqueue(write);
                    return room;
                }

                _writing = true;
            }
        }

        if (refused)
        {
            GiveBack(write);
            return null;
        }

        RunSendPath(write, ClientLimits.InlineWrites);
        return room;
    }

    /// <summary>
    /// Writes a write and whatever waits behind it, for as long as the writes complete at once; called by whoever
    /// holds the send path, outside the lock. It is a loop, not a recursion: each write that has ended does its end's
    /// work, then the next is taken in one hold of the lock (<see cref="TryTakeNext"/>). Once the end has begun, every
    /// write it takes that is not a DISCONNECT is dropped (<see cref="GiveBack"/>), and counts as one more write. A
    /// write that does not complete at once is handed to <see cref="_continuation"/>, and this returns with the path
    /// held by that write. After <paramref name="inlineWrites"/> writes, with more waiting, the path, still held, is
    /// handed to the thread pool. It never throws, but for a bug of the client.
    /// </summary>
    /// <param name="write">
    /// The first write, which the caller took with the path.
    /// </param>
    /// <param name="inlineWrites">
    /// How many writes this thread makes, dropped ones included, before it hands the path to the thread pool;
    /// <see cref="int.MaxValue"/> for a thread that is nobody's call: a write's completion that runs later, or the
    /// thread pool itself.
    /// </param>
    private void RunSendPath(QueuedWrite write, int inlineWrites)
    {
        while (true)
        {
            if (!TryWrite(write, inlineWrites))
            {
                // Pending: the path is the write's now, and its completion carries it on.
                return;
            }

            inlineWrites--;
            if (!TryTakeNext(inlineWrites, out write))
            {
                return;
            }
        }
    }

    /// <summary>
    /// Carries the send path on after a write that did not complete at once has ended; called by
    /// <see cref="SendPathContinuation"/>, once per such write, on the thread that completed it. It does the work of
    /// the write's end (<see cref="FinishWrite"/>), then takes the next waiting write and writes on, or gives the path
    /// up. It never throws, but for a bug of the client.
    /// </summary>
    /// <param name="ended">
    /// The write that has ended.
    /// </param>
    /// <param name="failure">
    /// What the write failed with, or <see langword="null"/> when the link took it.
    /// </param>
    /// <param name="insideTheRegistration">
    /// Whether the write completed while its continuation was being registered and the continuation runs inside that
    /// registration, on the stack of the caller that started the write: it then counts as one more of that caller's
    /// writes, so that such calls cannot nest deeper than <see cref="ClientLimits.InlineWrites"/>.
    /// </param>
    /// <param name="inlineWrites">
    /// How many writes the caller that started the write still made on its thread; used only with
    /// <paramref name="insideTheRegistration"/>.
    /// </param>
    internal void ContinueSendPath(in QueuedWrite ended, Exception? failure, bool insideTheRegistration,
        int inlineWrites)
    {
        int writes = insideTheRegistration
            ? Math.Min(inlineWrites, ClientLimits.InlineWrites) - 1
            : int.MaxValue;
        FinishWrite(ended, failure);
        if (TryTakeNext(writes, out QueuedWrite next))
        {
            RunSendPath(next, writes);
        }
    }

    /// <summary>
    /// Carries the send path on from the thread pool, after a holder handed it over, still held, with writes waiting;
    /// called by <see cref="SendPathContinuation"/> as a work item. It never throws, but for a bug of the client.
    /// </summary>
    internal void RunHandedOverSendPath()
    {
        if (TryTakeNext(int.MaxValue, out QueuedWrite next))
        {
            RunSendPath(next, int.MaxValue);
        }
    }

    /// <summary>
    /// Ends a write whose link refused the registration of its continuation, when the link ends it after all; called
    /// by that write's <see cref="SendPathContinuation"/>. The send path is no longer this write's, so it is not
    /// carried on: the write only does its end's work and returns its buffer, which nobody could return before, since
    /// the write might still have been using it (design 6.8, rule 4).
    /// </summary>
    /// <param name="ended">
    /// The write that has ended.
    /// </param>
    /// <param name="failure">
    /// What the write failed with, or <see langword="null"/> when the link took it.
    /// </param>
    internal void EndRefusedWrite(in QueuedWrite ended, Exception? failure) =>
        EndWrite(ended, succeeded: failure is null);

    /// <summary>
    /// Writes one write the holder of the send path has taken, or drops it, or completes the marker; called by the
    /// holder, outside the lock. A write whose link refuses the registration of its continuation faults the send path
    /// (<see cref="FaultSendPath"/>) and is from then on left to its own completion, which returns its buffer; the
    /// holder then drops every write it takes, the DISCONNECT included, until the queue is empty and it gives the
    /// path up.
    /// </summary>
    /// <param name="write">
    /// The write taken.
    /// </param>
    /// <param name="inlineWrites">
    /// How many writes this thread still makes, for the continuation of a write that does not complete at once.
    /// </param>
    /// <returns>
    /// <see langword="true"/> when the write has ended, was dropped, or was refused the registration, and the caller
    /// still holds the path; <see langword="false"/> when the write is pending and its completion holds the path now.
    /// </returns>
    private bool TryWrite(in QueuedWrite write, int inlineWrites)
    {
        IMqttLink? link = _link;
        // Set only by a holder; a later holder took the path in a hold of the lock, which published it.
        if (write.Kind == SendKind.WritesEnded || link is null || _faulted || IsDropped(write.Kind))
        {
            GiveBack(write);
            return true;
        }

        if (write.Kind == SendKind.PingRequest)
        {
            // The deadline counts from here, not from the tick that queued it: a write stalled ahead of it in the send
            // path is not the server failing to answer. Set before the write, so a PINGRESP that comes at once finds
            // it.
            Volatile.Write(ref _pingRequestTimestamp, _time.GetTimestamp());
        }

        ValueTask writing = StartWrite(link, write);
        if (writing.IsCompleted)
        {
            FinishWrite(write, TakeFailure(writing));
            return true;
        }

        SendPathContinuation continuation = _continuation ??= new SendPathContinuation(this);
        try
        {
            continuation.Await(writing, write, inlineWrites);
            return false;
        }
        catch (Exception exception)
        {
            // A fault of the link. The write may still be using its buffer (design 6.8, rule 4), so it is not
            // returned: the continuation stays armed and returns it if the write ever ends.
            _continuation = null;
            FaultSendPath(write, exception);
            return true;
        }
    }

    /// <summary>
    /// Faults the send path after the link refused the registration of a write's continuation; called by the holder,
    /// outside the lock. The refused write may still be in progress, so nothing may reach the link again: the path is
    /// marked faulted, which refuses every later write and makes the holder drop what waits, the DISCONNECT included;
    /// the connection ends with <see cref="MqttClientCloseReason.WriteFailed"/> unless another reason was recorded
    /// first; and the link is aborted in either case, so that a close already under way does not wait for a
    /// DISCONNECT that never comes. A DISCONNECT that was the refused write is told now that it was not written.
    /// </summary>
    /// <param name="refused">
    /// The write whose registration was refused; its buffer stays with it.
    /// </param>
    /// <param name="exception">
    /// What the registration threw.
    /// </param>
    private void FaultSendPath(in QueuedWrite refused, Exception exception)
    {
        lock (_sendGate)
        {
            _faulted = true;
        }

        if (!TryBeginEnd(
                new MqttClientCloseInfo(MqttClientCloseReason.WriteFailed, MqttReasonCode.Success, null, exception),
                EndMode.Abort))
        {
            // Another reason ended the connection first, perhaps with a close that does not abort: abort all the same.
            AbortLink();
        }

        if (refused.Kind == SendKind.Disconnect)
        {
            Volatile.Read(ref _disconnectWritten)?.TrySetResult(result: false);
        }
    }

    /// <summary>
    /// The one hold of the lock in which the holder of the send path, its write having ended, takes the next waiting
    /// write or gives the path up. A holder that has made its writes on its thread and finds more waiting takes
    /// nothing: it hands the path, still held, to the thread pool, which takes the next write itself.
    /// </summary>
    /// <param name="inlineWrites">
    /// How many writes this thread still makes; at 0 or below, it hands over instead of taking.
    /// </param>
    /// <param name="next">
    /// The write taken when the result is <see langword="true"/>; otherwise the default value.
    /// </param>
    /// <returns>
    /// <see langword="true"/> when the caller still holds the path and is to write <paramref name="next"/>;
    /// <see langword="false"/> when it no longer does: the path was given up in the hold, or handed to the thread
    /// pool.
    /// </returns>
    private bool TryTakeNext(int inlineWrites, out QueuedWrite next)
    {
        lock (_sendGate)
        {
            if (_sendQueue.Count == 0)
            {
                _writing = false;
                next = default;
                return false;
            }

            if (inlineWrites > 0)
            {
                return _sendQueue.TryDequeue(out next);
            }
        }

        // Still held across the hand-over, so nobody else writes and nothing is lost: whatever is added meanwhile
        // waits for the thread pool's hold, which takes the next write itself.
        next = default;
        SendPathContinuation workItem = _continuation ??= new SendPathContinuation(this);
        ThreadPool.UnsafeQueueUserWorkItem(workItem, preferLocal: false);
        return false;
    }

    /// <summary>
    /// Does the work of a write that has ended: a failed write ends the connection with
    /// <see cref="MqttClientCloseReason.WriteFailed"/> and aborts the link, before anything waiting is taken, so that
    /// the holder drops what waits; then the write's end does its work (<see cref="EndWrite"/>).
    /// </summary>
    /// <param name="write">
    /// The write that has ended.
    /// </param>
    /// <param name="failure">
    /// What the write failed with, or <see langword="null"/> when the link took it.
    /// </param>
    private void FinishWrite(in QueuedWrite write, Exception? failure)
    {
        if (failure is not null)
        {
            // Whatever the link threw, the write did not happen: the connection cannot go on.
            TryBeginEnd(
                new MqttClientCloseInfo(MqttClientCloseReason.WriteFailed, MqttReasonCode.Success, null, failure),
                EndMode.Abort);
        }

        EndWrite(write, succeeded: failure is null);
    }

    /// <summary>
    /// Gives back a write that is not written: refused when handed over, or dropped by the holder once the end has
    /// begun. It does what the end of a write does (<see cref="EndWrite"/>), as for a write that did not succeed; the
    /// marker of <see cref="WhenWritesEndedAsync"/> is completed.
    /// </summary>
    /// <param name="write">
    /// The write.
    /// </param>
    private void GiveBack(in QueuedWrite write) => EndWrite(write, succeeded: false);

    /// <summary>
    /// The work of a write's end, whatever the outcome, outside the lock: records the time of the last send after a
    /// write that succeeded, clears a CONNECT's bytes, gives back a PUBLISH's queued bytes or the PINGREQ's flag, and
    /// returns the buffer to the client's pool, once; then tells whoever waits for a DISCONNECT whether it was
    /// written. The marker of <see cref="WhenWritesEndedAsync"/> has no buffer: its end completes it.
    /// </summary>
    /// <param name="write">
    /// The write that has ended, or was refused or dropped.
    /// </param>
    /// <param name="succeeded">
    /// Whether the link took it.
    /// </param>
    private void EndWrite(in QueuedWrite write, bool succeeded)
    {
        if (write.Kind == SendKind.WritesEnded)
        {
            // Handed over under the lock, which published it to the holder that took the marker.
            Volatile.Read(ref _writesEnded)?.TrySetResult();
            return;
        }

        if (succeeded)
        {
            Volatile.Write(ref _lastSendTimestamp, _time.GetTimestamp());
        }

        switch (write.Kind)
        {
            case SendKind.Connect:
                // The shared pool hands this array to the next renter as it is, credentials included.
                write.Buffer.AsSpan(0, write.Length).Clear();
                break;
            case SendKind.Publish:
                Interlocked.Add(ref _pendingPublishBytes, -write.Length);
                break;
            case SendKind.PingRequest:
                Volatile.Write(ref _pingQueued, 0);
                break;
        }

        // The write has ended, so the buffer is no longer in use (6.8).
        if (write.Buffer is { } buffer)
        {
            _pool.Return(buffer);
        }

        if (write.Kind == SendKind.Disconnect)
        {
            Volatile.Read(ref _disconnectWritten)?.TrySetResult(succeeded);
        }
        else if (write.Kind == SendKind.Answers)
        {
            CountDownAnswers(write.Answers);
        }
    }

    /// <summary>
    /// Takes answers whose write has ended, or which were given back, off the answers waiting
    /// (<see cref="_waitingAnswers"/>); when that brings the count back to the cap or below while the read loop waits,
    /// completes its wait, outside the lock.
    /// </summary>
    /// <param name="answers">
    /// The number of answers in the write.
    /// </param>
    private void CountDownAnswers(int answers)
    {
        TaskCompletionSource? room = null;
        lock (_sendGate)
        {
            _waitingAnswers -= answers;
            if (_waitingAnswers <= ClientLimits.MaxWaitingAnswers)
            {
                room = _answerRoom;
                _answerRoom = null;
            }
        }

        room?.TrySetResult();
    }

    /// <summary>
    /// Ends the read loop's wait at the cap, if it waits, once the end of the connection is recorded; called by
    /// whoever recorded it or found it recorded, after its hold of the client's lock (<see cref="TryBeginEnd"/>,
    /// <see cref="DisposeAsync"/>, the connect's deadline). Once the end is recorded no wait is created again: a hold
    /// of <see cref="_sendGate"/> after this one sees the end and refuses the answers. A wait created in a hold before
    /// it is taken here. The read loop then sees the end on its next turn.
    /// </summary>
    private void ReleaseAnswerRoom()
    {
        TaskCompletionSource? room;
        lock (_sendGate)
        {
            room = _answerRoom;
            _answerRoom = null;
        }

        room?.TrySetResult();
    }

    /// <summary>
    /// Whether a write of this kind is not written once the end has begun: everything but a DISCONNECT.
    /// </summary>
    /// <param name="kind">
    /// The write's kind.
    /// </param>
    /// <returns>
    /// <see langword="true"/> when the end has begun and the write is not a DISCONNECT; it is then dropped.
    /// </returns>
    /// <remarks>
    /// It reads <see cref="_phase"/> without the client's lock, so it may miss an end being recorded at that moment.
    /// A stale read is benign: the write it lets through is one handed over or taken as the end was recorded, which
    /// the link may take or fail like any write already in progress; and every recorder of the end then takes
    /// <see cref="_sendGate"/> (<see cref="ReleaseAnswerRoom"/>), so the next hold of that lock sees the end.
    /// </remarks>
    private bool IsDropped(SendKind kind) => kind != SendKind.Disconnect && _phase >= Phase.Closing;

    /// <summary>
    /// Hands a write's bytes to the link. A link that throws instead of returning a failed task is treated the same
    /// way.
    /// </summary>
    /// <param name="link">
    /// The link.
    /// </param>
    /// <param name="write">
    /// The write, which has a buffer.
    /// </param>
    /// <returns>
    /// The write; it may have completed, with success or with an error.
    /// </returns>
    private static ValueTask StartWrite(IMqttLink link, in QueuedWrite write)
    {
        try
        {
            return link.WriteAsync(write.Buffer.AsMemory(0, write.Length), CancellationToken.None);
        }
        catch (Exception exception)
        {
            return ValueTask.FromException(exception);
        }
    }

    /// <summary>
    /// Takes the outcome of a write that has completed, which also lets the link reuse what backs its task.
    /// </summary>
    /// <param name="completed">
    /// The write, which has completed.
    /// </param>
    /// <returns>
    /// <see langword="null"/> when it succeeded; what it failed with, or was cancelled with, otherwise.
    /// </returns>
    private static Exception? TakeFailure(ValueTask completed)
    {
        if (completed.IsCompletedSuccessfully)
        {
            completed.GetAwaiter().GetResult();
            return null;
        }

        // Takes the failure out of the write, so that it is observed here and reported nowhere else; a cancelled
        // write has no exception of its own and is reported as the cancellation an await would have thrown.
        Task failed = completed.AsTask();
        return failed.Exception?.InnerException ?? new TaskCanceledException(failed);
    }

    /// <summary>
    /// Whether a packet of the given size is within the server's Maximum Packet Size, which limits every packet the
    /// client sends after the CONNACK (MQTT 5, 3.2.2.3.6).
    /// </summary>
    /// <param name="size">
    /// The packet's encoded size, fixed header included.
    /// </param>
    /// <returns>
    /// <see langword="true"/> when the server accepts a packet that large; <see langword="false"/> when it may not
    /// be sent.
    /// </returns>
    private bool FitsServerMaximumPacketSize(int size) => (uint)size <= _serverMaximumPacketSize;

    /// <summary>
    /// Ends the connection because a packet it needs, an acknowledgement or a PINGREQ, is over the server's Maximum
    /// Packet Size: <see cref="MqttClientCloseReason.ProtocolError"/> with 0x95, packet too large, aborting the
    /// link without a DISCONNECT. Does nothing when another reason ended the connection first.
    /// </summary>
    private void EndOverServerMaximumPacketSize()
        => TryBeginEnd(
            new MqttClientCloseInfo(MqttClientCloseReason.ProtocolError, MqttReasonCode.PacketTooLarge, null, null),
            EndMode.Abort);

    /// <summary>
    /// Encodes a DISCONNECT into a rented buffer.
    /// </summary>
    /// <param name="reasonCode">
    /// The DISCONNECT's reason code.
    /// </param>
    /// <param name="length">
    /// The packet's length.
    /// </param>
    /// <returns>
    /// The rented buffer, which the caller hands to <see cref="SendDisconnectAsync"/>.
    /// </returns>
    private byte[] RentDisconnect(MqttReasonCode reasonCode, out int length)
    {
        byte[] buffer = _pool.Rent(DisconnectCodec.GetSize(reasonCode, null));
        length = DisconnectCodec.Write(buffer, reasonCode, null);
        return buffer;
    }
}
