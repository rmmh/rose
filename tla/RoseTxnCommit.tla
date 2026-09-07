------------------------- MODULE RoseTxnCommit -------------------------
EXTENDS Integers, FiniteSets

\* Single-metadata-master transaction protocol.  SQLite's metadata commit is
\* represented by Publish: it is the sole visibility/acknowledgement point.
\* Physical records may be orphaned after a crash, but published metadata is
\* created only from fsynced shard records.

CONSTANTS Txns, Disks, TotalShards, MinVerifiedShards, MinCommitShards,
          MaxDiskFailures, AllowDegradedWrites

Shards == 1..TotalShards
Records == Txns \times Shards

VARIABLES disk_state, volatile_records, durable_records,
          txn_state, placement, published, snapshots, orphan_records, bad_publish

vars == <<disk_state, volatile_records, durable_records,
          txn_state, placement, published, snapshots, orphan_records, bad_publish>>

Init ==
    /\ disk_state = [d \in Disks |-> "active"]
    /\ volatile_records = [d \in Disks |-> {}]
    /\ durable_records = [d \in Disks |-> {}]
    /\ txn_state = [t \in Txns |-> "new"]
    /\ placement = [t \in Txns |-> {}]
    /\ published = {}
    /\ snapshots = {}
    /\ orphan_records = {}
    /\ bad_publish = FALSE

ActiveDisks == {d \in Disks : disk_state[d] = "active"}
DurableShards(t) == {s \in Shards : \E d \in ActiveDisks : <<t, s>> \in durable_records[d]}
LivePlacementShards(t) == {s \in Shards : \E d \in ActiveDisks : <<d, s>> \in placement[t] /\ <<t, s>> \in durable_records[d]}
\* This is not a consensus quorum.  Immutable chunk hashes make one verified
\* duplicate sufficient; EC needs N verified, distinct fragments to decode.
Readable(t) == Cardinality(LivePlacementShards(t)) >= MinVerifiedShards
FullyProtected(t) == Cardinality(LivePlacementShards(t)) = TotalShards
RequiredCommitShards == IF AllowDegradedWrites THEN MinCommitShards ELSE TotalShards
HasDegradedPublishedData == \E t \in published : ~FullyProtected(t)
FullPlacementAvailable == Cardinality(ActiveDisks) >= TotalShards
WritesAllowed == IF AllowDegradedWrites
                 THEN Cardinality(ActiveDisks) >= MinCommitShards
                 ELSE ~HasDegradedPublishedData /\ FullPlacementAvailable
DiskFailures == Cardinality({d \in Disks : disk_state[d] = "failed"})

StartTxn(t) ==
    /\ txn_state[t] = "new"
    /\ WritesAllowed
    /\ txn_state' = [txn_state EXCEPT ![t] = "open"]
    /\ UNCHANGED <<disk_state, volatile_records, durable_records, placement, published, snapshots, orphan_records, bad_publish>>

\* A PREPARED record is not visible and may be lost by Crash.
PrepareShard(t, d, s) ==
    /\ txn_state[t] \in {"open", "prepared"}
    /\ disk_state[d] = "active"
    /\ <<t, s>> \notin volatile_records[d] /\ <<t, s>> \notin durable_records[d]
    /\ \A s2 \in Shards : s2 # s => <<t, s2>> \notin volatile_records[d] /\ <<t, s2>> \notin durable_records[d]
    /\ \A d2 \in Disks : d2 # d => <<t, s>> \notin volatile_records[d2] /\ <<t, s>> \notin durable_records[d2]
    /\ volatile_records' = [volatile_records EXCEPT ![d] = @ \cup {<<t, s>>}]
    /\ txn_state' = [txn_state EXCEPT ![t] = "prepared"]
    /\ UNCHANGED <<disk_state, durable_records, placement, published, snapshots, orphan_records, bad_publish>>

\* Fsync turns a prepared record into a durable candidate for publication.
FsyncShard(t, d, s) ==
    /\ disk_state[d] = "active"
    /\ <<t, s>> \in volatile_records[d]
    /\ volatile_records' = [volatile_records EXCEPT ![d] = @ \ {<<t, s>>}]
    /\ durable_records' = [durable_records EXCEPT ![d] = @ \cup {<<t, s>>}]
    /\ UNCHANGED <<disk_state, txn_state, placement, published, snapshots, orphan_records, bad_publish>>

\* Atomic metadata transaction publishes the version/head and exact mapping.
\* Snapshot capture is explicit, matching the selected runtime contract.
Publish(t) ==
    /\ txn_state[t] = "prepared"
    /\ WritesAllowed
    /\ Cardinality(DurableShards(t)) >= RequiredCommitShards
    /\ placement' = [placement EXCEPT ![t] =
          {<<d, s>> \in Disks \times Shards : <<t, s>> \in durable_records[d]}]
    /\ txn_state' = [txn_state EXCEPT ![t] = "published"]
    /\ published' = published \cup {t}
    /\ bad_publish' = (bad_publish \/ (~AllowDegradedWrites /\ HasDegradedPublishedData))
    /\ UNCHANGED snapshots
    /\ UNCHANGED <<disk_state, volatile_records, durable_records, orphan_records>>

\* A process crash loses non-fsynced bytes.  It cannot change SQLite's
\* published set or snapshots, so no partial transaction becomes visible.
Crash ==
    /\ volatile_records' = [d \in Disks |-> {}]
    /\ UNCHANGED <<disk_state, durable_records, txn_state, placement, published, snapshots, orphan_records, bad_publish>>

RecoverOrAbandon(t) ==
    /\ txn_state[t] \in {"open", "prepared"}
    /\ txn_state' = [txn_state EXCEPT ![t] = "abandoned"]
    /\ orphan_records' = orphan_records \cup
          ({t} \times {<<d2, s>> \in Disks \times Shards : <<t, s>> \in durable_records[d2]})
    /\ UNCHANGED <<disk_state, volatile_records, durable_records, placement, published, snapshots, bad_publish>>

FailDisk(d) ==
    /\ disk_state[d] = "active"
    /\ DiskFailures < MaxDiskFailures
    /\ disk_state' = [disk_state EXCEPT ![d] = "failed"]
    /\ volatile_records' = [volatile_records EXCEPT ![d] = {}]
    /\ durable_records' = [durable_records EXCEPT ![d] = {}]
    /\ UNCHANGED <<txn_state, placement, published, snapshots, orphan_records, bad_publish>>

\* Repair requires a readable source quorum.  It restores a missing shard on
\* an active disk; once every published transaction is fully protected, writes
\* are admitted again automatically by WritesAllowed.
RepairShard(t, d, s) ==
    /\ t \in published
    /\ Readable(t)
    /\ disk_state[d] = "active"
    /\ s \notin LivePlacementShards(t)
    /\ \A s2 \in Shards : <<t, s2>> \notin durable_records[d] /\ <<t, s2>> \notin volatile_records[d]
    /\ \A d2 \in Disks : d2 # d => <<t, s>> \notin durable_records[d2]
    /\ durable_records' = [durable_records EXCEPT ![d] = @ \cup {<<t, s>>}]
    /\ placement' = [placement EXCEPT ![t] = (@ \ {<<old, s>> : old \in Disks}) \cup {<<d, s>>}]
    /\ UNCHANGED <<disk_state, volatile_records, txn_state, published, snapshots, orphan_records, bad_publish>>

\* Reclamation models later segment compaction.  It never overwrites a hole
\* in place; it removes an orphan only after copying live records elsewhere.
ReclaimOrphan(t, d, s) ==
    /\ txn_state[t] = "abandoned"
    /\ <<t, <<d, s>>>> \in orphan_records
    /\ <<t, s>> \in durable_records[d]
    /\ durable_records' = [durable_records EXCEPT ![d] = @ \ {<<t, s>>}]
    /\ orphan_records' = orphan_records \ {<<t, <<d, s>>>>}
    /\ UNCHANGED <<disk_state, volatile_records, txn_state, placement, published, snapshots, bad_publish>>

CaptureSnapshot ==
    /\ snapshots # published
    /\ snapshots' = published
    /\ UNCHANGED <<disk_state, volatile_records, durable_records, txn_state,
                    placement, published, orphan_records, bad_publish>>

Next ==
    \/ \E t \in Txns : StartTxn(t) \/ Publish(t) \/ RecoverOrAbandon(t)
    \/ \E t \in Txns, d \in Disks, s \in Shards : PrepareShard(t, d, s) \/ FsyncShard(t, d, s) \/ RepairShard(t, d, s)
    \/ \E t \in Txns, d \in Disks, s \in Shards : ReclaimOrphan(t, d, s)
    \/ \E d \in Disks : FailDisk(d)
    \/ Crash
    \/ CaptureSnapshot

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ published \subseteq Txns /\ snapshots \subseteq Txns /\ bad_publish \in BOOLEAN
    /\ \A d \in Disks : disk_state[d] \in {"active", "failed"} /\ volatile_records[d] \subseteq Records /\ durable_records[d] \subseteq Records
    /\ \A t \in Txns : txn_state[t] \in {"new", "open", "prepared", "published", "abandoned"} /\ placement[t] \subseteq Disks \times Shards
    /\ orphan_records \subseteq Txns \times (Disks \times Shards)
PublishedReadable == \A t \in published : Readable(t)
PublishedOnlyDurable == \A t \in published : \A <<d, s>> \in placement[t] : <<t, s>> \in durable_records[d] \/ disk_state[d] = "failed"
SnapshotsOnlyPublished == snapshots \subseteq published
NoDegradedPublication == ~bad_publish
NoColocatedShards == \A t \in Txns, d \in Disks :
    Cardinality({s \in Shards : <<t, s>> \in durable_records[d] \cup volatile_records[d]}) <= 1
CanonicalShardPlacement == \A t \in Txns, s \in Shards :
    Cardinality({d \in Disks : <<d, s>> \in placement[t]}) <= 1
StrictModeIsReadOnlyWhenDegraded == ~AllowDegradedWrites => (HasDegradedPublishedData => ~WritesAllowed)
StrictModePublishesFullProtection == ~AllowDegradedWrites => \A t \in published : Cardinality({s \in Shards : \E d \in Disks : <<d, s>> \in placement[t]}) = TotalShards
OrphansAreUnpublished == \A t \in Txns, d \in Disks, s \in Shards :
    <<t, <<d, s>>>> \in orphan_records => t \notin published

=============================================================================
