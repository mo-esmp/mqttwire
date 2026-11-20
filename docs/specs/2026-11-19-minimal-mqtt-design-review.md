# Review: GrainWire minimal MQTT design

Date: 2026-11-19  
Reviewed document: [2026-11-19-minimal-mqtt-design.md](2026-11-19-minimal-mqtt-design.md)  
Scope: architecture, correctness, performance, resource management, and verification.

## Assessment

The direction is sound, but several parts should be revised before implementation. The strongest choices are measuring first, retaining .NET's WebSocket implementation, separating the codec from transport, and testing against independent MQTT implementations.

The biggest risk is that removing idle objects introduces more complicated ownership and concurrency rules. Those rules need to be specified as precisely as the memory target.

This review compares the design with the current gateway/client code and the relevant MQTT and .NET behavior. These are design findings, not measured performance results. Statements and implementation instructions in the design document were treated as material to review, not as authorization to implement changes.

## Findings

### 1. Define whether sending completes on enqueue or on network delivery

**Design sections:** 7.4–7.5  
**Priority:** Resolve before implementation; this is the most important performance gap.

The existing [`PublishBatchAsync`](https://github.com/mo-esmp/grainwire/blob/main/src/GrainWire.Server/Internal/Gateway.cs) awaits each recipient sequentially. The proposed replacement calls the connection's send path directly.

If that await now waits for a socket write, **one slow recipient can delay everyone else in the room**. Its outgoing queue might never reach the overflow threshold because the producer is blocked on the first send.

Specify that gateway publication completes when the connection accepts or rejects ownership of the message. Network completion belongs to the connection's drain operation. Add a test where one recipient stops reading while healthy recipients continue receiving within their latency budget.

### 2. Correct the MQTT compatibility contract and add a property matrix

**Design sections:** 7.2, 7.8–7.9  
**Priority:** Resolve before implementation.

Accepting a Will and silently discarding it conflicts with MQTT's required Will behavior. Rejecting CONNECT with an appropriate reason is the better minimal implementation. See [MQTT 5, section 3.1.2.5](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html).

Similarly, “parse and skip unused properties” is too broad. Some properties change required behavior: `Request Problem Information=0` restricts diagnostic properties, and properties must be valid for their packet type. See [MQTT 5, sections 2.2.2 and 3.1.2.11.7](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html).

Add a compact matrix recording each relevant property's validation, default, and treatment: **honored, legally ignored, or rejected**. Include session expiry, authentication method, subscription identifiers, maximum packet size, and receive maximum.

Existing tests should protect application behavior, but they cannot be the sole definition of MQTT correctness.

### 3. Specify buffer ownership across every asynchronous boundary

**Design sections:** 7.2–7.4  
**Priority:** Resolve before implementation.

The codec returns views into rented memory, while requests and outgoing messages may outlive the read operation. That creates a potential use-after-return bug unless ownership is explicit.

Define these invariants:

- Decoded views remain valid only until the input buffer is returned.
- Queued requests own their payload, through a copy or transferred buffer ownership.
- Outgoing buffers remain owned until writing completes or fails.
- Every close, overflow, rejection, and exception path releases ownership exactly once.

For the first implementation, copying accepted request payloads is a reasonable tradeoff. Message-allocation optimization is already a non-goal.

### 4. Add byte budgets and bound stalled writes

**Design section:** 7.4  
**Priority:** Resolve before implementation.

Message-count limits are insufficient for a memory-focused design. `MaxOutgoingQueue=1000` could retain roughly **50 MB of payloads per connection** if those messages are 50 KB snapshots, before encoding buffers and object overhead. Incoming requests also have a meaningful worst case: 32 × 64 KiB is 2 MiB.

Add byte budgets alongside count limits, including the active write and queued payloads. Also define limits for aggregate buffered bytes, concurrent unauthenticated connections, and authentication operations.

A stalled consumer may continue sending PINGREQs, so receive-side keep-alive does not bound how long an outgoing write holds memory. Specify a write-progress deadline and a bounded close procedure: attempt DISCONNECT when possible, then abort. Do not require a blocked socket to successfully send its own eviction notice.

### 5. Make connection lifecycle and takeover explicit

**Design section:** 7.4  
**Priority:** Resolve before implementation.

“Cleanup runs once” is necessary, but the transitions need more detail. A small state diagram covering connecting, authenticating, active, closing, and closed would help.

In particular, decide:

- Whether the connect deadline includes authentication, and how late authentication completion is handled.
- Whether replacing an existing connection consumes another player slot temporarily.
- How old cleanup avoids removing or releasing resources belonging to the replacement.
- Whether an in-flight request's reply is discarded after its connection generation closes.

The slot question matters when a player is already at `MaxConnectionsPerPlayer`: a reconnect should not be accidentally rejected because the connection it is replacing still occupies a slot.

Specify monotonic deadlines, sweep frequency, and maximum timeout overshoot as well.

### 6. Strengthen the QoS 1 counter's correctness argument

**Design section:** 7.6  
**Priority:** Resolve before implementation.

MQTT identifiers must remain unused by another operation until the corresponding acknowledgment arrives; PUBLISH and SUBSCRIBE share the identifier space. See [MQTT 5, section 2.2.1](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html).

A count tells you how many publishes are outstanding, but does not itself identify them. Specify reservation, acknowledgment validation, wraparound, and failed-send handling. Use bounded identifier tracking, or document and test the invariants that make a compact acknowledgment window correct.

Also define what happens when PUBACKs stop arriving but PINGRESPs continue. Otherwise the connection can remain “healthy” while its publish window stays exhausted and application retries repeatedly fail locally.

Saving a small amount of client-side bookkeeping is not worth weakening correctness when the primary target is server memory per connection.

### 7. Define recovery when a snapshot exceeds the client's packet limit

**Design section:** 7.4  
**Priority:** Resolve before implementation.

“Drop the oversized packet, then sync” is not a complete recovery strategy. Consider a client whose maximum packet size is 32 KB joining a room with a 50 KB snapshot. The snapshot is dropped. Every subsequent sync produces another snapshot that is also dropped.

That is a permanent incompatibility, not a recoverable gap.

Define a detectable failure or a documented minimum supported receive size. Maximum-packet-size handling must also cover control packets, not just PUBLISH. See [MQTT 5, section 3.1.2.11.4](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html).

### 8. Make the idle-memory target precise and test memory after activity

**Design sections:** 2, 8–9  
**Priority:** Incorporate into the measurement spike.

“No buffer, no parked worker” is too absolute. The .NET WebSocket implementation retains internal state and a small receive buffer; the pending receive operation also exists. Zero-byte reads avoid holding an application payload buffer while waiting—they do not eliminate the transport's memory. The approach is supported by the [.NET implementation](https://github.com/dotnet/runtime/blob/v10.0.0/src/libraries/System.Net.WebSockets/src/System/Net/WebSockets/ManagedWebSocket.cs) and is already used by [ASP.NET Core](https://github.com/dotnet/aspnetcore/blob/v10.0.0/src/SignalR/common/Http.Connections/src/Internal/Transports/WebSocketsServerTransport.cs).

Define the goal as **no additional GrainWire payload buffer, empty queue backing storage, per-connection timer, or waiting request worker while idle**.

Extend the benchmark to measure freshly connected, joined, active-then-idle, and disconnected-after-churn states. Include several connection counts, TLS, realistic claims, and different room counts. This exposes retained pools, peak allocations, and shared structures that a fresh-idle measurement can miss.

Keep `floor + 2 KB` provisional until attribution shows whether it is realistic.

### 9. Add a capacity test to the performance acceptance criteria

**Design section:** 9  
**Priority:** Define before collecting acceptance results.

The existing workloads establish performance at a fixed offered load. Matching approximately 1,000 commands/s does not establish equal maximum throughput.

Add a ramp-up test measuring sustainable throughput before latency or failure limits are exceeded. Record CPU, allocation rate, GC pauses, and retained memory alongside p95/p99 latency.

Three runs are a useful starting point, but assess their variability before treating a 10% difference as meaningful. The benchmark environment is a VM, so small differences need particular care.

### 10. Expand verification beyond happy-path interoperability and stress tests

**Design section:** 7.8  
**Priority:** Include in the implementation and verification plan.

Add parser fuzzing; randomized split/coalesced packet streams; malformed property combinations; packet-ID wraparound; stalled writes; takeover during authentication and request execution; and repeated connect/disconnect cycles with resource accounting.

For drain concurrency, deterministic tests should force the transition between “queue became empty” and “another producer added work.” Random stress alone may miss that exact race.

Replace the single manual Blazor check with an automated browser smoke test covering connect, join, command, disconnect, and reconnect. Add operational metrics for queued bytes, oldest write age, pending acknowledgments, authentication duration, sweep duration, and disconnect reasons.

## Recommended next step

Keep Approach A and proceed with the measurement spike. Before replacing MQTTnet, resolve the send-completion contract, MQTT feature handling, buffer ownership, and bounded shutdown behavior in the document. Those decisions determine whether the implementation remains both small and reliable.
