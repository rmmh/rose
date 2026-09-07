----------------------- MODULE RosePrefixRecovery -----------------------
EXTENDS Integers

\* One plog's prefix journal, not the full file-publication protocol. Versions
\* stand for successively longer correct byte prefixes; intact=FALSE models a
\* torn overwrite that may damage previously acknowledged bytes. File contents
\* and directory entries can reach media independently before explicit sync.
\* Journal contents are verified and synced before rename; corrupt journal
\* evidence causes fail-closed behavior outside this small safety model.
CONSTANT MaxVersion
VARIABLES file, durableFile, journal, durableJournal, published,
          nextVersion, backup, phase

vars == <<file, durableFile, journal, durableJournal, published,
          nextVersion, backup, phase>>
None == -1
Versions == 0..MaxVersion
Clean(v) == [version |-> v, intact |-> TRUE]
DataState == <<file, durableFile, journal, durableJournal, published,
               nextVersion, backup>>

Init ==
    /\ file = Clean(0) /\ durableFile = Clean(0)
    /\ journal = None /\ durableJournal = None
    /\ published = 0 /\ nextVersion = 0 /\ backup = 0
    /\ phase = "Idle"

Begin ==
    /\ phase = "Idle" /\ published < MaxVersion
    /\ nextVersion' = published + 1 /\ phase' = "Prepare"
    /\ UNCHANGED <<file, durableFile, journal, durableJournal, published, backup>>

Prepare ==
    /\ phase = "Prepare"
    /\ IF journal = None
          THEN /\ backup' = file.version /\ phase' = "JournalWritten"
          ELSE /\ backup' = backup /\ phase' = "Install"
    /\ UNCHANGED <<file, durableFile, journal, durableJournal, published, nextVersion>>

SyncJournal ==
    /\ phase = "JournalWritten" /\ phase' = "Rename"
    /\ UNCHANGED DataState

RenameJournal ==
    /\ phase = "Rename"
    /\ journal' = backup /\ phase' = "Install"
    /\ UNCHANGED <<file, durableFile, durableJournal, published, nextVersion, backup>>

InstallJournal ==
    /\ phase = "Install"
    /\ durableJournal' = journal
    /\ phase' = "Write"
    /\ UNCHANGED <<file, durableFile, journal, published, nextVersion, backup>>

FailInstallSync ==
    /\ phase = "Install" /\ phase' = "Prepare"
    /\ UNCHANGED DataState

WriteData ==
    /\ phase \in {"Write", "RetryWrite"}
    /\ file' = Clean(nextVersion) /\ phase' = "SyncData"
    /\ UNCHANGED <<durableFile, journal, durableJournal, published, nextVersion, backup>>

TornWrite ==
    /\ phase = "Write"
    /\ file' = [version |-> nextVersion, intact |-> FALSE]
    /\ phase' = "RetryWrite"
    /\ UNCHANGED <<durableFile, journal, durableJournal, published, nextVersion, backup>>

SyncData ==
    /\ phase = "SyncData"
    /\ durableFile' = file /\ phase' = "Remove"
    /\ UNCHANGED <<file, journal, durableJournal, published, nextVersion, backup>>

RemoveJournal ==
    /\ phase = "Remove"
    /\ journal' = None /\ phase' = "Retire"
    /\ UNCHANGED <<file, durableFile, durableJournal, published, nextVersion, backup>>

RetireJournal ==
    /\ phase = "Retire"
    /\ durableJournal' = None
    /\ phase' = "Publish"
    /\ UNCHANGED <<file, durableFile, journal, published, nextVersion, backup>>

FailRetireSync ==
    /\ phase = "Retire" /\ phase' = "RetryCommit"
    /\ UNCHANGED DataState

RetryCommit ==
    /\ phase = "RetryCommit"
    /\ durableFile' = file /\ phase' = "Remove"
    /\ UNCHANGED <<file, journal, durableJournal, published, nextVersion, backup>>

Publish ==
    /\ phase = "Publish"
    /\ published' = nextVersion /\ phase' = "Idle"
    /\ UNCHANGED <<file, durableFile, journal, durableJournal, nextVersion, backup>>

\* Media may receive unsynced writes; sync is an ordering guarantee, not the
\* only event that can persist bytes. Including torn data persistence matters.
FlushData ==
    /\ durableFile' = file
    /\ UNCHANGED <<file, journal, durableJournal, published, nextVersion, backup, phase>>
FlushDirectory ==
    /\ durableJournal' = journal
    /\ UNCHANGED <<file, durableFile, journal, published, nextVersion, backup, phase>>

Crash ==
    /\ file' = durableFile /\ journal' = durableJournal
    /\ phase' = "Recover"
    /\ UNCHANGED <<durableFile, durableJournal, published, nextVersion, backup>>

\* A process exit preserves the kernel's current file and directory state.
ProcessCrash ==
    /\ phase' = "Recover"
    /\ UNCHANGED DataState

Replay ==
    /\ phase = "Recover"
    /\ IF journal = None
          THEN /\ file' = file /\ phase' = "ReplayRetire"
          ELSE /\ file' = Clean(journal) /\ phase' = "ReplaySync"
    /\ UNCHANGED <<durableFile, journal, durableJournal, published, nextVersion, backup>>
ReplaySync ==
    /\ phase = "ReplaySync"
    /\ durableFile' = file /\ phase' = "ReplayRemove"
    /\ UNCHANGED <<file, journal, durableJournal, published, nextVersion, backup>>
ReplayRemove ==
    /\ phase = "ReplayRemove"
    /\ journal' = None /\ phase' = "ReplayRetire"
    /\ UNCHANGED <<file, durableFile, durableJournal, published, nextVersion, backup>>
ReplayRetire ==
    /\ phase = "ReplayRetire"
    /\ durableJournal' = None /\ phase' = "Idle"
    /\ UNCHANGED <<file, durableFile, journal, published, nextVersion, backup>>

Next == Begin \/ Prepare \/ SyncJournal \/ RenameJournal \/ InstallJournal
        \/ FailInstallSync \/ WriteData \/ TornWrite \/ SyncData \/ RemoveJournal
        \/ RetireJournal \/ FailRetireSync \/ RetryCommit \/ Publish
        \/ FlushData \/ FlushDirectory \/ Crash \/ ProcessCrash \/ Replay \/ ReplaySync
        \/ ReplayRemove \/ ReplayRetire

TypeOK ==
    /\ file \in [version : Versions, intact : BOOLEAN]
    /\ durableFile \in [version : Versions, intact : BOOLEAN]
    /\ journal \in Versions \cup {None} /\ durableJournal \in Versions \cup {None}
    /\ published \in Versions /\ nextVersion \in Versions /\ backup \in Versions
    /\ phase \in {"Idle", "Prepare", "JournalWritten", "Rename", "Install",
                  "Write", "RetryWrite", "SyncData", "Remove", "Retire",
                  "RetryCommit", "Publish", "Recover", "ReplaySync",
                  "ReplayRemove", "ReplayRetire"}
PublishedRecoverable ==
    published = 0 \/ (durableFile.intact /\ durableFile.version >= published)
                  \/ (durableJournal # None /\ durableJournal >= published)
ReadyPrefixValid ==
    (phase = "Idle" /\ published > 0) => (file.intact /\ file.version >= published)
RollbackNotOlderThanPublished ==
    durableJournal = None \/ durableJournal >= published

Spec == Init /\ [][Next]_vars
=============================================================================
