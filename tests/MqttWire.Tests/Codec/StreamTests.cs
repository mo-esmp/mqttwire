using MqttWire.Codec;

namespace MqttWire.Tests.Codec;

/// <summary>
/// Tests the framing and the codecs over streams of packets cut at every byte, joined and split at random, and
/// over random and mutated bytes.
/// </summary>
public class StreamTests
{
    /// <summary>
    /// The packets a client sends, in the order of <see cref="s_clientStream"/>.
    /// </summary>
    private static readonly byte[][] s_clientPackets =
    [
        KnownPackets.Connect,
        KnownPackets.PublishQoS0,
        KnownPackets.PublishQoS1,
        KnownPackets.PublishQoS2,
        KnownPackets.PubAck,
        KnownPackets.PubRecRefused,
        KnownPackets.PubRel,
        KnownPackets.PubCompNotFound,
        KnownPackets.Subscribe,
        KnownPackets.SubscribeTwo,
        KnownPackets.Unsubscribe,
        KnownPackets.PingReq,
        KnownPackets.DisconnectWithReason,
    ];

    /// <summary>
    /// The packets a server sends, in the order of <see cref="s_serverStream"/>.
    /// </summary>
    private static readonly byte[][] s_serverPackets =
    [
        KnownPackets.ConnAck,
        KnownPackets.PublishQoS1,
        KnownPackets.PublishQoS2,
        KnownPackets.PubAckWithReason,
        KnownPackets.PubRel,
        KnownPackets.SubAck,
        KnownPackets.UnsubAck,
        KnownPackets.PingResp,
        KnownPackets.DisconnectTakenOver,
    ];

    /// <summary>
    /// One stream of every packet a client sends.
    /// </summary>
    private static readonly byte[] s_clientStream = Join(s_clientPackets);

    /// <summary>
    /// One stream of every packet a server sends.
    /// </summary>
    private static readonly byte[] s_serverStream = Join(s_serverPackets);

    /// <summary>
    /// Gives the seeds 0 to count - 1 as test cases.
    /// </summary>
    /// <param name="count">
    /// How many seeds.
    /// </param>
    /// <returns>
    /// One case per seed.
    /// </returns>
    public static IEnumerable<object[]> Seeds(int count)
    {
        for (int seed = 0; seed < count; seed++)
        {
            yield return [seed];
        }
    }

    /// <summary>
    /// Proves a stream cut in two at every position, or into single bytes, decodes to the same packets as the whole,
    /// and that every packet of the reader's own stream decodes as success.
    /// </summary>
    /// <param name="reader">
    /// The reader, as the underlying value of <see cref="Reader"/>.
    /// </param>
    [Theory]
    [InlineData((int)Reader.Server)]
    [InlineData((int)Reader.Client)]
    public void DecodeInPieces_StreamCutAtEveryPosition_DecodesLikeTheWhole(int reader)
    {
        // Arrange
        Reader who = (Reader)reader;
        byte[] stream = who == Reader.Server ? s_clientStream : s_serverStream;
        int packets = who == Reader.Server ? s_clientPackets.Length : s_serverPackets.Length;
        List<DecodedEntry> whole = PacketStream.DecodeAll(stream, who);
        List<int> everyByte = [];
        for (int k = 1; k < stream.Length; k++)
        {
            everyByte.Add(k);
        }

        // Act
        List<List<DecodedEntry>> cutInTwo = [];
        for (int k = 1; k < stream.Length; k++)
        {
            cutInTwo.Add(PacketStream.DecodeInPieces(stream, [k], who));
        }

        List<DecodedEntry> singleBytes = PacketStream.DecodeInPieces(stream, everyByte, who);

        // Assert
        whole.Count.ShouldBe(packets);
        whole.ShouldAllBe(entry => entry.Status == MqttReasonCode.Success);
        foreach (List<DecodedEntry> entries in cutInTwo)
        {
            entries.ShouldBe(whole);
        }

        singleBytes.ShouldBe(whole);
    }

    /// <summary>
    /// Proves packets of one reader's stream, chosen at random, joined and cut at random positions, decode to the
    /// same packets however they are cut.
    /// </summary>
    /// <param name="seed">
    /// The seed of the random generator.
    /// </param>
    [Theory]
    [MemberData(nameof(Seeds), 200)]
    public void DecodeInPieces_RandomJoinsAndSplits_DecodeToTheSamePackets(int seed)
    {
        // Arrange
        Random random = new(seed);
        Reader reader = seed % 2 == 0 ? Reader.Server : Reader.Client;
        byte[][] known = reader == Reader.Server ? s_clientPackets : s_serverPackets;
        List<byte> joined = [];
        int count = random.Next(1, 21);
        for (int i = 0; i < count; i++)
        {
            joined.AddRange(known[random.Next(known.Length)]);
        }

        byte[] stream = [.. joined];
        List<int> cuts = RandomCuts(random, stream.Length);

        // Act
        List<DecodedEntry> whole = PacketStream.DecodeAll(stream, reader);
        List<DecodedEntry> pieces = PacketStream.DecodeInPieces(stream, cuts, reader);

        // Assert
        whole.Count.ShouldBe(count);
        whole.ShouldAllBe(entry => entry.Status == MqttReasonCode.Success);
        pieces.ShouldBe(whole);
    }

    /// <summary>
    /// Proves random bytes, and known streams with a few bytes changed, never throw and decode to the same packets
    /// or errors however they are cut, for both readers.
    /// </summary>
    /// <param name="seed">
    /// The seed of the random generator.
    /// </param>
    [Theory]
    [MemberData(nameof(Seeds), 500)]
    public void DecodeInPieces_RandomAndMutatedBytes_EndInAPacketOrAnError(int seed)
    {
        // Arrange
        Random random = new(seed);
        byte[] stream = random.Next(2) == 0
            ? RandomBytes(random)
            : Mutate(random, random.Next(2) == 0 ? s_clientStream : s_serverStream);
        List<int> cuts = RandomCuts(random, stream.Length);

        foreach (Reader reader in new[] { Reader.Server, Reader.Client })
        {
            // Act
            List<DecodedEntry> whole = PacketStream.DecodeAll(stream, reader);
            List<DecodedEntry> pieces = PacketStream.DecodeInPieces(stream, cuts, reader);

            // Assert
            pieces.ShouldBe(whole);
        }
    }

    /// <summary>
    /// Proves inputs that once failed or are known to be extreme end in packets or errors without throwing, however
    /// they are cut. A seed that ever fails is added here as its bytes, with "seed N" in a comment.
    /// </summary>
    /// <param name="hex">
    /// The bytes of the stream in hex.
    /// </param>
    [Theory]
    [InlineData("30 FF FF FF 7F 00")] // A header that promises 268 MB: needs more data for ever, never an exception.
    public void DecodeInPieces_FixedCases_DecodeLikeTheWhole(string hex)
    {
        // Arrange
        byte[] stream = Hex.Parse(hex);
        List<int> everyByte = [];
        for (int k = 1; k < stream.Length; k++)
        {
            everyByte.Add(k);
        }

        foreach (Reader reader in new[] { Reader.Server, Reader.Client })
        {
            // Act
            List<DecodedEntry> whole = PacketStream.DecodeAll(stream, reader);
            List<DecodedEntry> pieces = PacketStream.DecodeInPieces(stream, everyByte, reader);

            // Assert
            pieces.ShouldBe(whole);
        }
    }

    /// <summary>
    /// Proves a header that promises far more than the stream holds gives no entry from either helper, and no exception.
    /// </summary>
    [Fact]
    public void DecodeAll_HeaderPromisesMoreThanIsThere_GivesNoEntries()
    {
        // Arrange
        byte[] stream = Hex.Parse("30 FF FF FF 7F 00");

        // Act
        List<DecodedEntry> whole = PacketStream.DecodeAll(stream, Reader.Server);
        List<DecodedEntry> pieces = PacketStream.DecodeInPieces(stream, [2, 5], Reader.Server);

        // Assert
        whole.ShouldBeEmpty();
        pieces.ShouldBeEmpty();
    }

    /// <summary>
    /// Proves a packet the reader never reads is a protocol error, and an invalid header ends the stream.
    /// </summary>
    [Fact]
    public void DecodeAll_PacketTheReaderNeverReads_IsProtocolErrorAndFramingErrorEndsTheStream()
    {
        // Arrange
        byte[] stream = [.. KnownPackets.ConnAck, .. Hex.Parse("F0 00"), .. KnownPackets.PingReq, .. Hex.Parse("00 00"), .. KnownPackets.PingReq];

        // Act
        List<DecodedEntry> entries = PacketStream.DecodeAll(stream, Reader.Server);

        // Assert
        entries.Select(entry => (entry.Type, entry.Status)).ShouldBe(
        [
            (PacketType.ConnAck, MqttReasonCode.ProtocolError),
            (PacketType.Auth, MqttReasonCode.ProtocolError),
            (PacketType.PingReq, MqttReasonCode.Success),
            (null, MqttReasonCode.MalformedPacket),
        ]);
    }

    /// <summary>
    /// Joins byte arrays into one.
    /// </summary>
    /// <param name="parts">
    /// The arrays, in order.
    /// </param>
    /// <returns>
    /// One array holding all of them.
    /// </returns>
    private static byte[] Join(byte[][] parts) => [.. parts.SelectMany(part => part)];

    /// <summary>
    /// Picks between 0 and 10 distinct cut positions inside a stream.
    /// </summary>
    /// <param name="random">
    /// The generator.
    /// </param>
    /// <param name="length">
    /// The length of the stream.
    /// </param>
    /// <returns>
    /// The positions, strictly increasing, each from 1 to length - 1.
    /// </returns>
    private static List<int> RandomCuts(Random random, int length)
    {
        SortedSet<int> cuts = [];
        if (length > 1)
        {
            int wanted = random.Next(0, 11);
            for (int i = 0; i < wanted; i++)
            {
                cuts.Add(random.Next(1, length));
            }
        }

        return [.. cuts];
    }

    /// <summary>
    /// Makes 0 to 64 random bytes.
    /// </summary>
    /// <param name="random">
    /// The generator.
    /// </param>
    /// <returns>
    /// The bytes.
    /// </returns>
    private static byte[] RandomBytes(Random random)
    {
        byte[] bytes = new byte[random.Next(0, 65)];
        random.NextBytes(bytes);
        return bytes;
    }

    /// <summary>
    /// Changes a stream in 1 to 3 places: flips a byte, inserts a byte or removes a byte.
    /// </summary>
    /// <param name="random">
    /// The generator.
    /// </param>
    /// <param name="source">
    /// The stream to start from; it is not changed.
    /// </param>
    /// <returns>
    /// The changed copy.
    /// </returns>
    private static byte[] Mutate(Random random, byte[] source)
    {
        List<byte> bytes = [.. source];
        int changes = random.Next(1, 4);
        for (int i = 0; i < changes && bytes.Count > 0; i++)
        {
            int position = random.Next(bytes.Count);
            switch (random.Next(3))
            {
                case 0:
                    bytes[position] ^= (byte)random.Next(1, 256);
                    break;
                case 1:
                    bytes.Insert(position, (byte)random.Next(256));
                    break;
                default:
                    bytes.RemoveAt(position);
                    break;
            }
        }

        return [.. bytes];
    }
}
