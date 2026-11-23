using System.Net.WebSockets;
using MqttWire.Server;
using MqttWire.Server.Internal;
using MqttWire.Tests.Codec;

namespace MqttWire.Tests.Server;

/// <summary>
/// Tests that the bytes of a CONNECT, its password among them, are cleared before the pooled array that held them
/// goes back to the pool (design 6.8, rule 7): the array pool hands a returned array to its next renter as it is.
/// Each test looks at every array at the moment the server returns it.
/// </summary>
public sealed class ConnectBufferTests
{
    /// <summary>
    /// The length of the recognisable password; each of its two halves is looked for.
    /// </summary>
    private const int PasswordLength = 32;

    /// <summary>
    /// Proves that an accepted CONNECT leaves no byte of its password in any array the server returns, and that the
    /// host was given the password whole.
    /// </summary>
    [Fact]
    public void Connect_Accepted_LeavesNoPasswordInAReturnedArray()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        byte[] password = NewPassword();
        Watch watch = new(harness, password);
        byte[]? seen = null;
        harness.Handler.Connect = (request, _) =>
        {
            seen = request.Password.ToArray();
            return ValueTask.FromResult(MqttConnectResult.Accept());
        };
        TestConnection connection = harness.Accept();

        // Act
        connection.Deliver(ConnectWith(password));

        // Assert
        connection.TakeWritten().ShouldBe(ServerPackets.ConnAck);
        seen.ShouldBe(password);
        watch.AssertNothingLeaked();
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a CONNECT MqttWire refuses itself (it carries a Will) leaves no byte of its password in any array
    /// the server returns.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Connect_Refused_LeavesNoPasswordInAReturnedArray()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        byte[] password = NewPassword();
        Watch watch = new(harness, password);
        byte[] connect = ServerPackets.BuildConnect(flags: 0x46, properties: [],
            payload:
            [
                .. ServerPackets.Field("c1"u8), 0x00, .. ServerPackets.Field("t"u8), .. ServerPackets.Field([]),
                .. ServerPackets.Field(password),
            ]);
        TestConnection connection = harness.Accept();

        // Act
        connection.Deliver(connect);

        // Assert
        connection.TakeWritten().ShouldBe(ServerPackets.ConnAckWillRefused);
        await connection.RunEnded();
        harness.Handler.Calls.ShouldBeEmpty();
        watch.AssertNothingLeaked();
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a CONNECT the codec refuses as malformed (a property twice) leaves no byte of its password in any
    /// array the server returns.
    /// </summary>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Fact]
    public async Task Connect_Malformed_LeavesNoPasswordInAReturnedArray()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        byte[] password = NewPassword();
        Watch watch = new(harness, password);
        byte[] connect = ServerPackets.BuildConnect(flags: 0x42, properties: Hex.Parse("22 00 0A 22 00 0A"),
            payload: [.. ServerPackets.Field("c1"u8), .. ServerPackets.Field(password)]);
        TestConnection connection = harness.Accept();

        // Act
        connection.Deliver(connect);

        // Assert
        connection.TakeWritten().ShouldBe(ServerPackets.ConnAckRefused(MqttReasonCode.MalformedPacket));
        await connection.RunEnded();
        harness.Handler.Calls.ShouldBeEmpty();
        watch.AssertNothingLeaked();
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a CONNECT too long for the first read buffer, whose password begins in that buffer, leaves no
    /// byte of it in the array the growth returns, nor in any other, and reaches the host whole.
    /// </summary>
    [Fact]
    public void Connect_NeededTheBufferToGrow_LeavesNoPasswordInAReturnedArray()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        byte[] start = NewPassword();
        byte[] password = [.. start, .. new byte[5_968]];
        Watch watch = new(harness, start);
        byte[]? seen = null;
        harness.Handler.Connect = (request, _) =>
        {
            seen = request.Password.ToArray();
            return ValueTask.FromResult(MqttConnectResult.Accept());
        };
        byte[] connect = ConnectWith(password);
        TestConnection connection = harness.Accept();

        // Act
        connection.Link.DeliverInChunks(connect, chunkSize: 1_000);

        // Assert
        connect.Length.ShouldBeGreaterThan(ServerLimits.FirstReadBufferBytes);
        connection.TakeWritten().ShouldBe(ServerPackets.ConnAck);
        seen.ShouldBe(password);
        watch.Returned.ShouldBeGreaterThanOrEqualTo(2);
        watch.AssertNothingLeaked();
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that a CONNECT followed by a SUBSCRIBE in the same message leaves no byte of its password in the
    /// buffer that is kept for the SUBSCRIBE while the host decides, nor in any array the server returns, and that
    /// the SUBSCRIBE is still answered after the CONNACK.
    /// </summary>
    [Fact]
    public void Connect_FollowedByASubscribeInTheSameMessage_LeavesNoPasswordAndAnswersTheSubscribe()
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        byte[] password = NewPassword();
        Watch watch = new(harness, password);
        int heldWhileDeciding = -1;
        bool passwordHeldWhileDeciding = true;
        harness.Handler.Connect = (_, _) =>
        {
            heldWhileDeciding = harness.Pool.Outstanding;
            passwordHeldWhileDeciding = watch.IsInARentedArray();
            return ValueTask.FromResult(MqttConnectResult.Accept());
        };
        TestConnection connection = harness.Accept();

        // Act
        connection.Deliver([.. ConnectWith(password), .. ServerPackets.Subscribe]);

        // Assert
        connection.TakeWritten().ShouldBe([.. ServerPackets.ConnAck, .. ServerPackets.SubAckGrantedQoS1]);
        heldWhileDeciding.ShouldBe(1);
        passwordHeldWhileDeciding.ShouldBeFalse();
        watch.AssertNothingLeaked();
        harness.Pool.Outstanding.ShouldBe(0);
    }

    /// <summary>
    /// Proves that half a CONNECT, with the start of the password in the half that arrived, leaves no byte of it in
    /// the array the server returns when the connect phase ends without the rest: at the connect deadline, when the
    /// peer closes, and when the request is aborted.
    /// </summary>
    /// <param name="ending">
    /// How the connect phase ends: <c>"deadline"</c>, <c>"peer"</c> or <c>"aborted"</c>.
    /// </param>
    /// <returns>
    /// A task that completes when the test has run.
    /// </returns>
    [Theory]
    [InlineData("deadline")]
    [InlineData("peer")]
    [InlineData("aborted")]
    public async Task Connect_HalfAConnectThenTheEnd_LeavesNoPasswordInAReturnedArray(string ending)
    {
        // Arrange
        ServerHarness harness = ServerHarness.Create();
        byte[] password = NewPassword();
        Watch watch = new(harness, password[..(PasswordLength / 2)]);
        byte[] connect = ConnectWith(password);
        TestConnection connection = harness.Accept();
        connection.Deliver(connect.AsSpan(start: 0, connect.Length - 8));
        int heldForTheHalf = harness.Pool.Outstanding;

        // Act
        switch (ending)
        {
            case "deadline":
                harness.Advance(ServerLimits.ConnectDeadline + ServerLimits.SweepInterval);
                break;
            case "peer":
                connection.Link.EndFromPeer();
                break;
            default:
                await connection.RequestAborted.CancelAsync();
                break;
        }

        // Assert
        await connection.RunEnded();
        heldForTheHalf.ShouldBe(1);
        connection.TakeWritten().ShouldBeEmpty();
        harness.Handler.Calls.ShouldBeEmpty();
        watch.Returned.ShouldBe(1);
        watch.AssertNothingLeaked();
        harness.Pool.Outstanding.ShouldBe(0);
        if (ending == "peer")
        {
            connection.Link.ClosedWith.ShouldBe(WebSocketCloseStatus.NormalClosure);
        }
    }

    /// <summary>
    /// Creates a password no other test, and no earlier use of a pooled array, can have left behind: two fresh
    /// GUIDs, so that finding either half in an array means this test's CONNECT left it there.
    /// </summary>
    /// <returns>
    /// The password, <see cref="PasswordLength"/> bytes.
    /// </returns>
    private static byte[] NewPassword() => [.. Guid.NewGuid().ToByteArray(), .. Guid.NewGuid().ToByteArray()];

    /// <summary>
    /// Builds a CONNECT with client id "c1", keep-alive 30, a clean start and a password.
    /// </summary>
    /// <param name="password">
    /// The password.
    /// </param>
    /// <returns>
    /// The packet's bytes, fixed header included; the password is its last field.
    /// </returns>
    private static byte[] ConnectWith(byte[] password) =>
        ServerPackets.BuildConnect(flags: 0x42, properties: [],
            payload: [.. ServerPackets.Field("c1"u8), .. ServerPackets.Field(password)]);

    /// <summary>
    /// Looks at every array the server returns to the harness's pool, at the moment of the return, for bytes of a
    /// secret.
    /// </summary>
    private sealed class Watch
    {
        /// <summary>
        /// The harness whose pool is watched.
        /// </summary>
        private readonly ServerHarness _harness;

        /// <summary>
        /// The bytes looked for, at least 16: its first 16 and its last 16 are searched.
        /// </summary>
        private readonly byte[] _secret;

        /// <summary>
        /// The number of arrays that came back holding the secret.
        /// </summary>
        private int _leaks;

        /// <summary>
        /// The number of arrays that came back.
        /// </summary>
        private int _returned;

        /// <summary>
        /// Starts watching the returns of a harness's pool.
        /// </summary>
        /// <param name="harness">
        /// The harness.
        /// </param>
        /// <param name="secret">
        /// The bytes that must be in no returned array, at least 16: its first 16 and its last 16 are looked for.
        /// </param>
        public Watch(ServerHarness harness, byte[] secret)
        {
            _harness = harness;
            _secret = secret;
            harness.Pool.Returning = Look;
        }

        /// <summary>
        /// Gets the number of arrays that came back so far.
        /// </summary>
        public int Returned => Volatile.Read(ref _returned);

        /// <summary>
        /// Tells whether the secret is in an array the server holds now.
        /// </summary>
        /// <returns>
        /// <see langword="true"/> when an array that is out of the pool holds the secret.
        /// </returns>
        public bool IsInARentedArray() => _harness.Pool.AnyRented(Holds);

        /// <summary>
        /// Asserts that arrays came back and that none of them held the secret.
        /// </summary>
        public void AssertNothingLeaked()
        {
            Returned.ShouldBeGreaterThan(0);
            Volatile.Read(ref _leaks).ShouldBe(0);
        }

        /// <summary>
        /// Looks at an array that is coming back.
        /// </summary>
        /// <param name="array">
        /// The array, whole: the pool hands all of it to the next renter.
        /// </param>
        private void Look(byte[] array)
        {
            Interlocked.Increment(ref _returned);
            if (Holds(array))
            {
                Interlocked.Increment(ref _leaks);
            }
        }

        /// <summary>
        /// Tells whether an array holds the first or the last 16 bytes of the secret.
        /// </summary>
        /// <param name="array">
        /// The array.
        /// </param>
        /// <returns>
        /// <see langword="true"/> when it does.
        /// </returns>
        private bool Holds(byte[] array) =>
            array.AsSpan().IndexOf(_secret.AsSpan(start: 0, PasswordLength / 2)) >= 0
            || array.AsSpan().IndexOf(_secret.AsSpan(_secret.Length - (PasswordLength / 2))) >= 0;
    }
}
