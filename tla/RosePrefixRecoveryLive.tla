--------------------- MODULE RosePrefixRecoveryLive ---------------------
EXTENDS RosePrefixRecovery

\* Conditional progress for the exact safety actions, with a cumulative finite
\* fault budget. This does not require progress during an unbounded outage.
\* Ordinary early media flushes remain unrestricted. Weak fairness applies to
\* successful protocol steps, not to faults or background cache flushing.
CONSTANT MaxFaults
VARIABLE faultsLeft
liveVars == <<vars, faultsLeft>>

ProtocolProgress ==
    Begin \/ Prepare \/ SyncJournal \/ RenameJournal \/ InstallJournal
    \/ WriteData \/ SyncData \/ RemoveJournal \/ RetireJournal \/ RetryCommit
    \/ Publish \/ Replay \/ ReplaySync \/ ReplayRemove \/ ReplayRetire

Fault == Crash \/ ProcessCrash \/ TornWrite \/ FailInstallSync \/ FailRetireSync

LiveInit == Init /\ faultsLeft = MaxFaults
LiveNext ==
    \/ /\ faultsLeft > 0 /\ faultsLeft' = faultsLeft - 1 /\ Fault
    \/ /\ UNCHANGED faultsLeft
       /\ (ProtocolProgress \/ FlushData \/ FlushDirectory)

LiveTypeOK == TypeOK /\ faultsLeft \in 0..MaxFaults
FairProgress == ProtocolProgress /\ UNCHANGED faultsLeft
LiveSpec == LiveInit /\ [][LiveNext]_liveVars /\ WF_liveVars(FairProgress)

RecoveryCompletes == (phase # "Idle") ~> (phase = "Idle")
PrefixesEventuallyAcknowledged == <>(published = MaxVersion)
=============================================================================
