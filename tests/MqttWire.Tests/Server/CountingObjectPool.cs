using Microsoft.Extensions.ObjectPool;

namespace MqttWire.Tests.Server;

/// <summary>
/// An <see cref="ObjectPool{T}"/> over another that counts what is out and every distinct object it has handed out,
/// so a test can prove that every object taken comes back and that the pool keeps no more than it may.
/// </summary>
/// <typeparam name="T">
/// The type of the pooled objects.
/// </typeparam>
/// <param name="inner">
/// The pool every call goes to: the server's own.
/// </param>
internal sealed class CountingObjectPool<T>(ObjectPool<T> inner) : ObjectPool<T>
    where T : class
{
    /// <summary>
    /// Guards <see cref="_seen"/> and <see cref="_out"/>.
    /// </summary>
    private readonly Lock _gate = new();

    /// <summary>
    /// Every object handed out, compared by reference.
    /// </summary>
    private readonly HashSet<T> _seen = new(ReferenceEqualityComparer.Instance);

    /// <summary>
    /// The objects handed out and not yet returned, compared by reference.
    /// </summary>
    private readonly HashSet<T> _out = new(ReferenceEqualityComparer.Instance);

    /// <summary>
    /// Gets the number of objects handed out and not yet returned.
    /// </summary>
    public int Outstanding
    {
        get
        {
            lock (_gate)
            {
                return _out.Count;
            }
        }
    }

    /// <summary>
    /// Gets the number of distinct objects ever handed out. Those not <see cref="Outstanding"/> are all the pool can
    /// hold, so this less <see cref="Outstanding"/> bounds what it keeps.
    /// </summary>
    public int Distinct
    {
        get
        {
            lock (_gate)
            {
                return _seen.Count;
            }
        }
    }

    /// <summary>
    /// Gets the object handed out last, or <see langword="null"/> before the first.
    /// </summary>
    public T? LastHandedOut
    {
        get
        {
            lock (_gate)
            {
                return field;
            }
        }

        private set;
    }

    /// <summary>
    /// Takes an object from the inner pool and counts it.
    /// </summary>
    /// <returns>
    /// The object.
    /// </returns>
    public override T Get()
    {
        T obj = inner.Get();
        lock (_gate)
        {
            _seen.Add(obj);
            _out.Add(obj);
            LastHandedOut = obj;
        }

        return obj;
    }

    /// <summary>
    /// Counts an object back and gives it to the inner pool.
    /// </summary>
    /// <param name="obj">
    /// The object.
    /// </param>
    /// <exception cref="InvalidOperationException">
    /// The object is not out: it was returned twice, or never handed out here; it is not given to the inner pool.
    /// </exception>
    public override void Return(T obj)
    {
        lock (_gate)
        {
            if (!_out.Remove(obj))
            {
                throw new InvalidOperationException("The object was not handed out by this pool, or was returned twice.");
            }
        }

        inner.Return(obj);
    }
}
