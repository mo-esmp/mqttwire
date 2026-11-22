using MqttWire.Server;
using MqttWire.Server.Internal;

namespace MqttWire.Tests.Server;

/// <summary>
/// Tests of the internal <see cref="CloseCause"/>: a superset of the public <see cref="MqttCloseReason"/> with the same
/// values, narrowed to it only for the causes <c>OnClosed</c> can report.
/// </summary>
public sealed class CloseCauseTests
{
    /// <summary>
    /// The causes of ends before an admission, which no <c>OnClosed</c> reports.
    /// </summary>
    private static readonly CloseCause[] s_beforeAdmission =
        [CloseCause.RefusedByHost, CloseCause.AuthenticationTimedOut, CloseCause.ConnectTimedOut];

    /// <summary>
    /// Proves that every member of the public enum has a member of the internal enum with the same name and value.
    /// </summary>
    [Fact]
    public void CloseCause_EveryPublicReason_HasTheSameNameAndValue()
    {
        // Arrange
        MqttCloseReason[] reasons = Enum.GetValues<MqttCloseReason>();

        // Act
        List<string> missing = [];
        foreach (MqttCloseReason reason in reasons)
        {
            if (!Enum.TryParse(reason.ToString(), out CloseCause cause) || (int)cause != (int)reason)
            {
                missing.Add(reason.ToString());
            }
        }

        // Assert
        reasons.Length.ShouldBe(15);
        missing.ShouldBeEmpty();
    }

    /// <summary>
    /// Proves that no two members of the internal enum share a value, so no cause can be taken for another.
    /// </summary>
    [Fact]
    public void CloseCause_EveryValue_IsDistinct()
    {
        // Arrange
        string[] names = Enum.GetNames<CloseCause>();

        // Act
        int distinct = Enum.GetValues<CloseCause>().Select(static cause => (int)cause).Distinct().Count();

        // Assert
        distinct.ShouldBe(names.Length);
    }

    /// <summary>
    /// Proves that narrowing gives each cause the public reason the design says: itself for those the public enum
    /// names, <see cref="MqttCloseReason.ConnectionLost"/> for a fault of the send path, and nothing for the three
    /// ends before an admission.
    /// </summary>
    [Fact]
    public void TryNarrow_EveryCause_GivesThePublicReasonOrNone()
    {
        foreach (CloseCause cause in Enum.GetValues<CloseCause>())
        {
            // Arrange
            bool beforeAdmission = s_beforeAdmission.Contains(cause);

            // Act
            bool narrowed = MqttWireConnection.TryNarrow(cause, out MqttCloseReason reason);

            // Assert
            narrowed.ShouldBe(!beforeAdmission, cause.ToString());
            if (cause == CloseCause.SendPathFaulted)
            {
                reason.ShouldBe(MqttCloseReason.ConnectionLost);
            }
            else if (!beforeAdmission)
            {
                reason.ToString().ShouldBe(cause.ToString());
            }
        }
    }
}
