using MqttWire.Client;

namespace MqttWire.Tests.Client;

/// <summary>
/// Tests the checks the client's constructor makes on its options, and the helpers of the request result.
/// </summary>
public sealed class MqttWireClientOptionsTests
{
    /// <summary>
    /// Keep-alive values in milliseconds that are outside 1 to 65,535 seconds.
    /// </summary>
    public static TheoryData<double> KeepAlivesOutOfRange => new()
    {
        0,
        999,
        65_536_000,
        -1000,
    };

    /// <summary>
    /// Keep-alive values in milliseconds and the whole seconds they round up to.
    /// </summary>
    public static TheoryData<double, double> KeepAliveRoundings => new()
    {
        { 1001, 2 },
        { 15_000, 15 },
        { 65_534_500, 65_535 },
    };

    /// <summary>
    /// Proves a plain set of options is accepted.
    /// </summary>
    [Fact]
    public void Constructor_ValidOptions_DoesNotThrow()
    {
        // Arrange
        MqttWireClientOptions options = Options();

        // Act
        Exception? exception = Record.Exception(() => new MqttWireClient(options));

        // Assert
        exception.ShouldBeNull();
    }

    /// <summary>
    /// Proves a URI that is not an absolute ws or wss URI is refused.
    /// </summary>
    /// <param name="uri">
    /// The URI text.
    /// </param>
    [Theory]
    [InlineData("http://x/mqtt")]
    [InlineData("tcp://x")]
    [InlineData("mqtt")]
    public void Constructor_UriNotWebSocket_ThrowsArgumentException(string uri)
    {
        // Arrange
        MqttWireClientOptions options = new() { Uri = new Uri(uri, UriKind.RelativeOrAbsolute), ClientId = "c" };

        // Act
        Action act = () => new MqttWireClient(options);

        // Assert
        act.ShouldThrow<ArgumentException>();
    }

    /// <summary>
    /// Proves an empty client identifier is refused.
    /// </summary>
    [Fact]
    public void Constructor_EmptyClientId_ThrowsArgumentException()
    {
        // Arrange
        MqttWireClientOptions options = Options(clientId: string.Empty);

        // Act
        Action act = () => new MqttWireClient(options);

        // Assert
        act.ShouldThrow<ArgumentException>();
    }

    /// <summary>
    /// Proves a client identifier of 65,536 UTF-8 bytes is refused.
    /// </summary>
    [Fact]
    public void Constructor_ClientIdOver65535Utf8Bytes_ThrowsArgumentException()
    {
        // Arrange
        MqttWireClientOptions options = Options(clientId: new string('é', 32_768));

        // Act
        Action act = () => new MqttWireClient(options);

        // Assert
        act.ShouldThrow<ArgumentException>();
    }

    /// <summary>
    /// Proves a client identifier holding U+0000 is refused.
    /// </summary>
    [Fact]
    public void Constructor_ClientIdWithNullCharacter_ThrowsArgumentException()
    {
        // Arrange
        MqttWireClientOptions options = Options(clientId: "a\0b");

        // Act
        Action act = () => new MqttWireClient(options);

        // Assert
        act.ShouldThrow<ArgumentException>();
    }

    /// <summary>
    /// Proves text that cannot be sent is refused naming the constructor's parameter, with a message that names
    /// the property.
    /// </summary>
    [Fact]
    public void Constructor_TextCannotBeSent_NamesTheOptionsParameter()
    {
        // Arrange
        MqttWireClientOptions options = Options(userName: "a\0b");

        // Act
        ArgumentException exception = Should.Throw<ArgumentException>(() => new MqttWireClient(options));

        // Assert
        exception.ParamName.ShouldBe("options");
        exception.Message.ShouldContain("user name");
    }

    /// <summary>
    /// Proves a client identifier with an unpaired surrogate is refused.
    /// </summary>
    [Fact]
    public void Constructor_ClientIdWithUnpairedSurrogate_ThrowsArgumentException()
    {
        // Arrange
        MqttWireClientOptions options = Options(clientId: "a\uD800b");

        // Act
        Action act = () => new MqttWireClient(options);

        // Assert
        act.ShouldThrow<ArgumentException>();
    }

    /// <summary>
    /// Proves a user name holding U+0000 is refused at construction.
    /// </summary>
    [Fact]
    public void Constructor_UserNameWithNullCharacter_ThrowsArgumentException()
    {
        // Arrange
        MqttWireClientOptions options = Options(userName: "a\0b");

        // Act
        Action act = () => new MqttWireClient(options);

        // Assert
        act.ShouldThrow<ArgumentException>();
    }

    /// <summary>
    /// Proves a user name with an unpaired surrogate is refused at construction.
    /// </summary>
    [Fact]
    public void Constructor_UserNameWithUnpairedSurrogate_ThrowsArgumentException()
    {
        // Arrange
        MqttWireClientOptions options = Options(userName: "a\uD800b");

        // Act
        Action act = () => new MqttWireClient(options);

        // Assert
        act.ShouldThrow<ArgumentException>();
    }

    /// <summary>
    /// Proves a user name of 65,536 UTF-8 bytes is refused at construction.
    /// </summary>
    [Fact]
    public void Constructor_UserNameOver65535Utf8Bytes_ThrowsArgumentException()
    {
        // Arrange
        MqttWireClientOptions options = Options(userName: new string('é', 32_768));

        // Act
        Action act = () => new MqttWireClient(options);

        // Assert
        act.ShouldThrow<ArgumentException>();
    }

    /// <summary>
    /// Proves a password holding U+0000 is refused at construction.
    /// </summary>
    [Fact]
    public void Constructor_PasswordWithNullCharacter_ThrowsArgumentException()
    {
        // Arrange
        MqttWireClientOptions options = Options(password: "a\0b");

        // Act
        Action act = () => new MqttWireClient(options);

        // Assert
        act.ShouldThrow<ArgumentException>();
    }

    /// <summary>
    /// Proves a password with an unpaired surrogate is refused at construction.
    /// </summary>
    [Fact]
    public void Constructor_PasswordWithUnpairedSurrogate_ThrowsArgumentException()
    {
        // Arrange
        MqttWireClientOptions options = Options(password: "a\uD800b");

        // Act
        Action act = () => new MqttWireClient(options);

        // Assert
        act.ShouldThrow<ArgumentException>();
    }

    /// <summary>
    /// Proves a password of 65,536 UTF-8 bytes is refused at construction.
    /// </summary>
    [Fact]
    public void Constructor_PasswordOver65535Utf8Bytes_ThrowsArgumentException()
    {
        // Arrange
        MqttWireClientOptions options = Options(password: new string('é', 32_768));

        // Act
        Action act = () => new MqttWireClient(options);

        // Assert
        act.ShouldThrow<ArgumentException>();
    }

    /// <summary>
    /// Proves a keep-alive outside 1 to 65,535 seconds is refused.
    /// </summary>
    /// <param name="milliseconds">
    /// The keep-alive in milliseconds.
    /// </param>
    [Theory]
    [MemberData(nameof(KeepAlivesOutOfRange))]
    public void Constructor_KeepAliveOutOfRange_ThrowsArgumentOutOfRangeException(double milliseconds)
    {
        // Arrange
        MqttWireClientOptions options = Options(keepAlive: TimeSpan.FromMilliseconds(milliseconds));

        // Act
        Action act = () => new MqttWireClient(options);

        // Assert
        act.ShouldThrow<ArgumentOutOfRangeException>();
    }

    /// <summary>
    /// Proves a fractional keep-alive is rounded up to whole seconds.
    /// </summary>
    /// <param name="milliseconds">
    /// The keep-alive in milliseconds.
    /// </param>
    /// <param name="expectedSeconds">
    /// The whole seconds expected.
    /// </param>
    [Theory]
    [MemberData(nameof(KeepAliveRoundings))]
    public void Constructor_KeepAliveFraction_RoundsUpToWholeSeconds(double milliseconds, double expectedSeconds)
    {
        // Arrange
        MqttWireClientOptions options = Options(keepAlive: TimeSpan.FromMilliseconds(milliseconds));

        // Act
        MqttWireClient client = new(options);

        // Assert
        client.KeepAlive.ShouldBe(TimeSpan.FromSeconds(expectedSeconds));
    }

    /// <summary>
    /// Proves a maximum incoming packet size outside 4,096 to 268,435,460 is refused.
    /// </summary>
    /// <param name="bytes">
    /// The size.
    /// </param>
    [Theory]
    [InlineData(ClientLimits.MinIncomingPacketBytes - 1)]
    [InlineData(ClientLimits.MaxIncomingPacketBytes + 1)]
    public void Constructor_MaxIncomingPacketBytesOutOfRange_ThrowsArgumentOutOfRangeException(int bytes)
    {
        // Arrange
        MqttWireClientOptions options = Options(maxIncomingPacketBytes: bytes);

        // Act
        Action act = () => new MqttWireClient(options);

        // Assert
        act.ShouldThrow<ArgumentOutOfRangeException>();
    }

    /// <summary>
    /// Proves the smallest and largest maximum incoming packet sizes are accepted.
    /// </summary>
    /// <param name="bytes">
    /// The size.
    /// </param>
    [Theory]
    [InlineData(ClientLimits.MinIncomingPacketBytes)]
    [InlineData(ClientLimits.MaxIncomingPacketBytes)]
    public void Constructor_MaxIncomingPacketBytesAtEdge_DoesNotThrow(int bytes)
    {
        // Arrange
        MqttWireClientOptions options = Options(maxIncomingPacketBytes: bytes);

        // Act
        Exception? exception = Record.Exception(() => new MqttWireClient(options));

        // Assert
        exception.ShouldBeNull();
    }

    /// <summary>
    /// Proves missing options are refused.
    /// </summary>
    [Fact]
    public void Constructor_NullOptions_ThrowsArgumentNullException()
    {
        // Arrange
        MqttWireClientOptions options = null!; // The test passes null on purpose.

        // Act
        Action act = () => new MqttWireClient(options);

        // Assert
        act.ShouldThrow<ArgumentNullException>();
    }

    /// <summary>
    /// Proves a completed subscribe with reason code 0x01 reports a granted QoS of at least once.
    /// </summary>
    [Fact]
    public void GrantedQoS_SubscribeGrantedQoS1_IsAtLeastOnce()
    {
        // Arrange
        MqttRequestResult result = new(MqttRequestStatus.Completed, MqttReasonCode.GrantedQoS1);

        // Act
        MqttQoS? granted = result.GrantedQoS;

        // Assert
        granted.ShouldBe(MqttQoS.AtLeastOnce);
    }

    /// <summary>
    /// Proves a refusal and a request that did not complete grant no QoS.
    /// </summary>
    [Fact]
    public void GrantedQoS_RefusedOrNotCompleted_IsNull()
    {
        // Arrange
        MqttRequestResult refused = new(MqttRequestStatus.Completed, MqttReasonCode.NotAuthorized);
        MqttRequestResult lost = new(MqttRequestStatus.ConnectionLost, MqttReasonCode.Success);

        // Act
        MqttQoS? fromRefused = refused.GrantedQoS;
        MqttQoS? fromLost = lost.GrantedQoS;

        // Assert
        fromRefused.ShouldBeNull();
        fromLost.ShouldBeNull();
    }

    /// <summary>
    /// Proves a completed exchange with a refusal code is not a success.
    /// </summary>
    [Fact]
    public void IsSuccess_CompletedWithRefusal_IsFalse()
    {
        // Arrange
        MqttRequestResult result = new(MqttRequestStatus.Completed, MqttReasonCode.NotAuthorized);

        // Act
        bool isSuccess = result.IsSuccess;

        // Assert
        isSuccess.ShouldBeFalse();
    }

    /// <summary>
    /// Proves a completed exchange with a code below 0x80 is a success and one that did not complete is not.
    /// </summary>
    [Fact]
    public void IsSuccess_CompletedWithSuccessCode_IsTrueOnlyWhenCompleted()
    {
        // Arrange
        MqttRequestResult completed = new(MqttRequestStatus.Completed, MqttReasonCode.NoMatchingSubscribers);
        MqttRequestResult notConnected = new(MqttRequestStatus.NotConnected, MqttReasonCode.Success);

        // Act
        bool completedSuccess = completed.IsSuccess;
        bool notConnectedSuccess = notConnected.IsSuccess;

        // Assert
        completedSuccess.ShouldBeTrue();
        notConnectedSuccess.ShouldBeFalse();
    }

    /// <summary>
    /// Builds options that pass every check, with the named values changed.
    /// </summary>
    /// <param name="clientId">
    /// The client identifier.
    /// </param>
    /// <param name="userName">
    /// The user name.
    /// </param>
    /// <param name="password">
    /// The password.
    /// </param>
    /// <param name="keepAlive">
    /// The keep-alive; the default when <see langword="null"/>.
    /// </param>
    /// <param name="maxIncomingPacketBytes">
    /// The maximum incoming packet size; the default when <see langword="null"/>.
    /// </param>
    /// <returns>
    /// The options.
    /// </returns>
    private static MqttWireClientOptions Options(
        string clientId = "client",
        string? userName = null,
        string? password = null,
        TimeSpan? keepAlive = null,
        int? maxIncomingPacketBytes = null) => new()
        {
            Uri = new Uri("ws://localhost/mqtt"),
            ClientId = clientId,
            UserName = userName,
            Password = password,
            KeepAlive = keepAlive ?? TimeSpan.FromSeconds(15),
            MaxIncomingPacketBytes = maxIncomingPacketBytes ?? 1_048_576,
        };
}
