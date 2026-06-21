------------------------- MODULE RoseStorage -------------------------
EXTENDS Integers, FiniteSets

\* A bounded, RPC-level model of Rose's file, virtual-log, physical-log, and
\* disk-maintenance control planes.  A maintenance job is public; its copy
\* steps are worker RPCs that may interleave with all normal I/O actions.

CONSTANTS Nodes, Disks, DiskNodes, ActiveDisks, Objects, Jobs,
          Modes, TotalShards, MinCopies, MinCommitShards, MinRequiredShards,
          MaxDiskFailures, MaxNodeFailures, MaxFailures, NodeLevelDurability

VARIABLES file_state, object_mode, object_state, plog_ready,
          disk_state, node_state, pending, stored,
          job_state, job_kind, job_disk, job_progress, last_rpc

vars == <<file_state, object_mode, object_state, plog_ready,
          disk_state, node_state, pending, stored,
          job_state, job_kind, job_disk, job_progress, last_rpc>>

Pairs == Objects \times (0..TotalShards)
Request(d, o, s) == <<"request", d, o, s>>
Ack(d, o, s) == <<"ack", d, o, s>>
Messages == {Request(d, o, s) : d \in Disks, o \in Objects, s \in 0..TotalShards} \cup
            {Ack(d, o, s) : d \in Disks, o \in Objects, s \in 0..TotalShards}

\* Bounded TLC configurations select one of these relations with
\* `DiskNodes <- ...`; every valid relation assigns exactly one node per disk.
IdentityDiskNodes == {<<d, d>> : d \in Disks}
TwoDisksOnN1 == {<<"d1", "n1">>, <<"d2", "n1">>,
                  <<"d3", "n2">>, <<"d4", "n3">>}

Init ==
    /\ file_state = [o \in Objects |-> "new"]
    /\ object_mode = [o \in Objects |-> "NONE"]
    /\ object_state = [o \in Objects |-> "new"]
    /\ plog_ready = {}
    /\ disk_state = [d \in Disks |-> IF d \in ActiveDisks THEN "active" ELSE "absent"]
    /\ node_state = [n \in Nodes |-> "working"]
    /\ pending = [d \in Disks |-> {}]
    /\ stored = [d \in Disks |-> {}]
    /\ job_state = [j \in Jobs |-> "idle"]
    /\ job_kind = [j \in Jobs |-> "none"]
    /\ job_disk = [j \in Jobs |-> "none"]
    /\ job_progress = [j \in Jobs |-> FALSE]
    /\ last_rpc = "init"

\* DiskNodes assigns each disk to its node fault domain. A node outage makes all
\* of its disks unavailable without changing their lifecycle state or contents.
DiskNode(d) == CHOOSE n \in Nodes : <<d, n>> \in DiskNodes
DiskLive(d) == disk_state[d] = "active" /\ node_state[DiskNode(d)] = "working"
DiskReadable(d) == disk_state[d] \in {"active", "draining"} /\ node_state[DiskNode(d)] = "working"

ValidShard(o, s) ==
    \/ object_mode[o] \in {"NONE", "DUPLICATE"} /\ s = 0
    \/ object_mode[o] = "EC" /\ s \in 1..TotalShards

LiveDisks(o, s) == {d \in Disks : DiskReadable(d) /\ <<o, s>> \in stored[d]}
LiveECShards(o) == {s \in 1..TotalShards : \E d \in Disks : d \in LiveDisks(o, s)}
AcknowledgedDisks(o, s) ==
    {d \in Disks : DiskReadable(d) /\ <<o, s>> \in stored[d] /\ <<o, s>> \notin pending[d]}
AcknowledgedECShards(o) ==
    {s \in 1..TotalShards : \E d \in Disks : d \in AcknowledgedDisks(o, s)}

Readable(o) ==
    \/ object_mode[o] \in {"NONE", "DUPLICATE"} /\ Cardinality(LiveDisks(o, 0)) >= 1
    \/ object_mode[o] = "EC" /\ Cardinality(LiveECShards(o)) >= MinRequiredShards

CommitReady(o) ==
    \/ object_mode[o] = "NONE" /\ Cardinality(LiveDisks(o, 0)) >= 1
    \/ object_mode[o] = "DUPLICATE" /\ Cardinality(LiveDisks(o, 0)) >= MinCopies
    \/ object_mode[o] = "EC" /\ Cardinality(LiveECShards(o)) >= MinCommitShards

AcknowledgedForCommit(o) ==
    \/ object_mode[o] = "NONE" /\ Cardinality(AcknowledgedDisks(o, 0)) >= 1
    \/ object_mode[o] = "DUPLICATE" /\ Cardinality(AcknowledgedDisks(o, 0)) >= MinCopies
    \/ object_mode[o] = "EC" /\ Cardinality(AcknowledgedECShards(o)) >= MinCommitShards

\* The destination may temporarily hold the same EC shard during a move, but
\* it may not collapse distinct EC shards (or duplicate copies) onto a node.
PlacementAllowed(o, s, d) ==
    /\ DiskLive(d)
    /\ <<o, s>> \notin pending[d]
    /\ (object_mode[o] = "EC" =>
          \A s2 \in 1..TotalShards : s2 # s =>
             <<o, s2>> \notin stored[d] /\ <<o, s2>> \notin pending[d])
    /\ IF NodeLevelDurability
       THEN \A d2 \in Disks : DiskNode(d2) = DiskNode(d) =>
              \A s2 \in 0..TotalShards :
                  <<o, s2>> \notin stored[d2] /\ <<o, s2>> \notin pending[d2]
       ELSE TRUE

Open(o) ==
    /\ file_state[o] = "new"
    /\ file_state' = [file_state EXCEPT ![o] = "open"]
    /\ last_rpc' = last_rpc
    /\ UNCHANGED <<object_mode, object_state, plog_ready, disk_state, node_state,
                  pending, stored, job_state, job_kind, job_disk, job_progress>>

Write(o) ==
    /\ file_state[o] = "open"
    /\ file_state' = [file_state EXCEPT ![o] = "buffered"]
    /\ last_rpc' = last_rpc
    /\ UNCHANGED <<object_mode, object_state, plog_ready, disk_state, node_state,
                  pending, stored, job_state, job_kind, job_disk, job_progress>>

Close(o) ==
    /\ file_state[o] = "buffered"
    /\ file_state' = [file_state EXCEPT ![o] = "closed"]
    /\ last_rpc' = last_rpc
    /\ UNCHANGED <<object_mode, object_state, plog_ready, disk_state, node_state,
                  pending, stored, job_state, job_kind, job_disk, job_progress>>

Getattr(o) ==
    /\ last_rpc' = last_rpc
    /\ UNCHANGED <<file_state, object_mode, object_state, plog_ready, disk_state,
                  node_state, pending, stored, job_state, job_kind, job_disk, job_progress>>

MakeVlog(o, mode) ==
    /\ mode \in Modes
    /\ file_state[o] \in {"open", "buffered", "closed"}
    /\ object_mode[o] = "NONE"
    /\ object_mode' = [object_mode EXCEPT ![o] = mode]
    /\ object_state' = [object_state EXCEPT ![o] = "vlog-ready"]
    /\ last_rpc' = last_rpc
    /\ UNCHANGED <<file_state, plog_ready, disk_state, node_state, pending, stored,
                  job_state, job_kind, job_disk, job_progress>>

MakePlog(d) ==
    /\ DiskLive(d)
    /\ d \notin plog_ready
    /\ plog_ready' = plog_ready \cup {d}
    /\ last_rpc' = last_rpc
    /\ UNCHANGED <<file_state, object_mode, object_state, disk_state, node_state,
                  pending, stored, job_state, job_kind, job_disk, job_progress>>

WriteVlog(o) ==
    /\ object_state[o] = "vlog-ready"
    /\ file_state[o] \in {"buffered", "closed"}
    /\ object_state' = [object_state EXCEPT ![o] = "writing"]
    /\ last_rpc' = last_rpc
    /\ UNCHANGED <<file_state, object_mode, plog_ready, disk_state, node_state,
                  pending, stored, job_state, job_kind, job_disk, job_progress>>

WritePlog(d, o, s) ==
    /\ d \in plog_ready
    /\ object_state[o] = "writing"
    /\ ValidShard(o, s)
    /\ PlacementAllowed(o, s, d)
    /\ <<o, s>> \notin stored[d]
    /\ pending' = [pending EXCEPT ![d] = @ \cup {<<o, s>>, Request(d, o, s)}]
    /\ last_rpc' = last_rpc
    /\ UNCHANGED <<file_state, object_mode, object_state, plog_ready, disk_state,
                  node_state, stored, job_state, job_kind, job_disk, job_progress>>

CommitPlog(d, o, s) ==
    /\ DiskLive(d)
    /\ Request(d, o, s) \in pending[d]
    /\ pending' = [pending EXCEPT ![d] = @ \ {Request(d, o, s)}]
    /\ stored' = [stored EXCEPT ![d] = @ \cup {<<o, s>>}]
    /\ last_rpc' = last_rpc
    /\ UNCHANGED <<file_state, object_mode, object_state, plog_ready, disk_state,
                  node_state, job_state, job_kind, job_disk, job_progress>>

SendPlogAck(d, o, s) ==
    /\ DiskLive(d)
    /\ <<o, s>> \in pending[d] \cap stored[d]
    /\ pending' = [pending EXCEPT ![d] = @ \cup {Ack(d, o, s)}]
    /\ last_rpc' = last_rpc
    /\ UNCHANGED <<file_state, object_mode, object_state, plog_ready, disk_state,
                  node_state, stored, job_state, job_kind, job_disk, job_progress>>

ReceivePlogAck(d, o, s) ==
    /\ Ack(d, o, s) \in pending[d]
    /\ pending' = [pending EXCEPT ![d] = @ \ {<<o, s>>, Ack(d, o, s)}]
    /\ last_rpc' = last_rpc
    /\ UNCHANGED <<file_state, object_mode, object_state, plog_ready, disk_state,
                  node_state, stored, job_state, job_kind, job_disk, job_progress>>

RetryPlog(d, o, s) ==
    /\ <<o, s>> \in pending[d]
    /\ pending' = [pending EXCEPT ![d] = @ \cup {Request(d, o, s)}]
    /\ last_rpc' = last_rpc
    /\ UNCHANGED <<file_state, object_mode, object_state, plog_ready, disk_state,
                  node_state, stored, job_state, job_kind, job_disk, job_progress>>

DropPlogMessage(d, o, s) ==
    /\ \E tag \in {"request", "ack"} : <<tag, d, o, s>> \in pending[d]
    /\ pending' = [pending EXCEPT ![d] = @ \ {Request(d, o, s), Ack(d, o, s)}]
    /\ last_rpc' = last_rpc
    /\ UNCHANGED <<file_state, object_mode, object_state, plog_ready, disk_state,
                  node_state, stored, job_state, job_kind, job_disk, job_progress>>

CommitVlog(o) ==
    /\ object_state[o] = "writing"
    /\ CommitReady(o)
    /\ AcknowledgedForCommit(o)
    /\ object_state' = [object_state EXCEPT ![o] = "committed"]
    /\ last_rpc' = last_rpc
    /\ UNCHANGED <<file_state, object_mode, plog_ready, disk_state, node_state,
                  pending, stored, job_state, job_kind, job_disk, job_progress>>

Read(o) ==
    /\ object_state[o] = "committed"
    /\ Readable(o)
    /\ last_rpc' = last_rpc
    /\ UNCHANGED <<file_state, object_mode, object_state, plog_ready, disk_state,
                  node_state, pending, stored, job_state, job_kind, job_disk, job_progress>>

ReadVlog(o) ==
    /\ object_state[o] = "committed"
    /\ Readable(o)
    /\ last_rpc' = last_rpc
    /\ UNCHANGED <<file_state, object_mode, object_state, plog_ready, disk_state,
                  node_state, pending, stored, job_state, job_kind, job_disk, job_progress>>

ReadPlog(d, o, s) ==
    /\ DiskLive(d)
    /\ <<o, s>> \in stored[d]
    /\ last_rpc' = last_rpc
    /\ UNCHANGED <<file_state, object_mode, object_state, plog_ready, disk_state,
                  node_state, pending, stored, job_state, job_kind, job_disk, job_progress>>

AddDisk(d) ==
    /\ disk_state[d] = "absent"
    /\ node_state[DiskNode(d)] = "working"
    /\ disk_state' = [disk_state EXCEPT ![d] = "active"]
    /\ last_rpc' = last_rpc
    /\ UNCHANGED <<file_state, object_mode, object_state, plog_ready, node_state,
                  pending, stored, job_state, job_kind, job_disk, job_progress>>

StartRemove(j, d) ==
    /\ job_state[j] = "idle"
    /\ disk_state[d] = "active"
    /\ pending[d] = {}
    /\ disk_state' = [disk_state EXCEPT ![d] = "draining"]
    /\ job_state' = [job_state EXCEPT ![j] = "running"]
    /\ job_kind' = [job_kind EXCEPT ![j] = "remove"]
    /\ job_disk' = [job_disk EXCEPT ![j] = d]
    /\ last_rpc' = last_rpc
    /\ UNCHANGED <<file_state, object_mode, object_state, plog_ready, node_state,
                  pending, stored, job_progress>>

ReplaceDisk(j, old, new) ==
    /\ job_state[j] = "idle"
    /\ disk_state[old] = "active"
    /\ pending[old] = {}
    /\ disk_state[new] = "absent"
    /\ node_state[DiskNode(new)] = "working"
    /\ disk_state' = [disk_state EXCEPT ![old] = "draining", ![new] = "active"]
    /\ job_state' = [job_state EXCEPT ![j] = "running"]
    /\ job_kind' = [job_kind EXCEPT ![j] = "replace"]
    /\ job_disk' = [job_disk EXCEPT ![j] = old]
    /\ last_rpc' = last_rpc
    /\ UNCHANGED <<file_state, object_mode, object_state, plog_ready, node_state,
                  pending, stored, job_progress>>

StartReprotect(j, d) ==
    /\ job_state[j] = "idle"
    /\ disk_state[d] \in {"failed", "draining"}
    /\ job_state' = [job_state EXCEPT ![j] = "running"]
    /\ job_kind' = [job_kind EXCEPT ![j] = "reprotect"]
    /\ job_disk' = [job_disk EXCEPT ![j] = d]
    /\ last_rpc' = last_rpc
    /\ UNCHANGED <<file_state, object_mode, object_state, plog_ready, disk_state,
                  node_state, pending, stored, job_progress>>

StartRebalance(j) ==
    /\ job_state[j] = "idle"
    /\ job_state' = [job_state EXCEPT ![j] = "running"]
    /\ job_kind' = [job_kind EXCEPT ![j] = "rebalance"]
    /\ last_rpc' = last_rpc
    /\ UNCHANGED <<file_state, object_mode, object_state, plog_ready, disk_state,
                  node_state, pending, stored, job_disk, job_progress>>

DrainStep(j, to, o, s) ==
    /\ job_state[j] = "running"
    /\ job_kind[j] \in {"remove", "replace"}
    /\ DiskReadable(job_disk[j])
    /\ <<o, s>> \in stored[job_disk[j]]
    /\ PlacementAllowed(o, s, to)
    /\ <<o, s>> \notin stored[to]
    /\ to # job_disk[j]
    /\ stored' = [stored EXCEPT ![to] = @ \cup {<<o, s>>},
                                 ![job_disk[j]] = @ \ {<<o, s>>}]
    /\ job_progress' = [job_progress EXCEPT ![j] = TRUE]
    /\ last_rpc' = last_rpc
    /\ UNCHANGED <<file_state, object_mode, object_state, plog_ready, disk_state,
                  node_state, pending, job_state, job_kind, job_disk>>

ReprotectStep(j, to, o, s) ==
    /\ job_state[j] = "running"
    /\ job_kind[j] = "reprotect"
    /\ object_state[o] = "committed"
    /\ ValidShard(o, s)
    /\ Readable(o)
    /\ \A d \in Disks : <<o, s>> \notin stored[d]
    /\ PlacementAllowed(o, s, to)
    /\ stored' = [stored EXCEPT ![to] = @ \cup {<<o, s>>}]
    /\ job_progress' = [job_progress EXCEPT ![j] = TRUE]
    /\ last_rpc' = last_rpc
    /\ UNCHANGED <<file_state, object_mode, object_state, plog_ready, disk_state,
                  node_state, pending, job_state, job_kind, job_disk>>

RebalanceStep(j, from, to, o, s) ==
    /\ job_state[j] = "running"
    /\ job_kind[j] = "rebalance"
    /\ DiskLive(from)
    /\ <<o, s>> \in stored[from]
    /\ <<o, s>> \notin pending[from]
    /\ PlacementAllowed(o, s, to)
    /\ <<o, s>> \notin stored[to]
    /\ to # from
    /\ stored' = [stored EXCEPT ![to] = @ \cup {<<o, s>>},
                                 ![from] = @ \ {<<o, s>>}]
    /\ job_progress' = [job_progress EXCEPT ![j] = TRUE]
    /\ last_rpc' = last_rpc
    /\ UNCHANGED <<file_state, object_mode, object_state, plog_ready, disk_state,
                  node_state, pending, job_state, job_kind, job_disk>>

FinishJob(j) ==
    /\ job_state[j] = "running"
    /\ (job_kind[j] \in {"remove", "replace"} =>
          disk_state[job_disk[j]] = "draining" /\
          stored[job_disk[j]] = {} /\ pending[job_disk[j]] = {})
    /\ (job_kind[j] \in {"reprotect", "rebalance"} => job_progress[j])
    /\ disk_state' = IF job_kind[j] \in {"remove", "replace"}
                     THEN [disk_state EXCEPT ![job_disk[j]] = "detached"]
                     ELSE disk_state
    /\ job_state' = [job_state EXCEPT ![j] = "done"]
    /\ last_rpc' = last_rpc
    /\ UNCHANGED <<file_state, object_mode, object_state, plog_ready, node_state,
                  pending, stored, job_kind, job_disk, job_progress>>

TotalDiskFailures == Cardinality({d \in Disks : disk_state[d] = "failed"})
TotalNodeFailures == Cardinality({n \in Nodes : node_state[n] = "failed"})

FailDisk(d) ==
    /\ disk_state[d] \in {"active", "draining"}
    /\ TotalDiskFailures < MaxDiskFailures
    /\ TotalDiskFailures + TotalNodeFailures < MaxFailures
    /\ disk_state' = [disk_state EXCEPT ![d] = "failed"]
    /\ pending' = [pending EXCEPT ![d] = {}]
    /\ stored' = [stored EXCEPT ![d] = {}]
    /\ last_rpc' = last_rpc
    /\ UNCHANGED <<file_state, object_mode, object_state, plog_ready, node_state,
                  job_state, job_kind, job_disk, job_progress>>

FailNode(n) ==
    /\ node_state[n] = "working"
    /\ TotalNodeFailures < MaxNodeFailures
    /\ TotalDiskFailures + TotalNodeFailures < MaxFailures
    /\ node_state' = [node_state EXCEPT ![n] = "failed"]
    /\ last_rpc' = last_rpc
    /\ UNCHANGED <<file_state, object_mode, object_state, plog_ready, disk_state,
                  pending, stored, job_state, job_kind, job_disk, job_progress>>

\* Node failure is transient: its disks and their contents are available again
\* when the node returns.
RecoverNode(n) ==
    /\ node_state[n] = "failed"
    /\ node_state' = [node_state EXCEPT ![n] = "working"]
    /\ last_rpc' = last_rpc
    /\ UNCHANGED <<file_state, object_mode, object_state, plog_ready, disk_state,
                  pending, stored, job_state, job_kind, job_disk, job_progress>>

Next ==
    \/ \E o \in Objects : Open(o)
    \/ \E o \in Objects : Write(o)
    \/ \E o \in Objects : Close(o)
    \/ \E o \in Objects : Getattr(o)
    \/ \E o \in Objects, mode \in Modes : MakeVlog(o, mode)
    \/ \E d \in Disks : MakePlog(d)
    \/ \E o \in Objects : WriteVlog(o)
    \/ \E d \in Disks, o \in Objects, s \in 0..TotalShards : WritePlog(d, o, s)
    \/ \E d \in Disks, o \in Objects, s \in 0..TotalShards : CommitPlog(d, o, s)
    \/ \E d \in Disks, o \in Objects, s \in 0..TotalShards : SendPlogAck(d, o, s) \/ ReceivePlogAck(d, o, s) \/ RetryPlog(d, o, s) \/ DropPlogMessage(d, o, s)
    \/ \E o \in Objects : CommitVlog(o)
    \/ \E o \in Objects : Read(o) \/ ReadVlog(o)
    \/ \E d \in Disks, o \in Objects, s \in 0..TotalShards : ReadPlog(d, o, s)
    \/ \E d \in Disks : AddDisk(d) \/ FailDisk(d)
    \/ \E j \in Jobs, d \in Disks : StartRemove(j, d) \/ StartReprotect(j, d)
    \/ \E j \in Jobs, old \in Disks, new \in Disks : ReplaceDisk(j, old, new)
    \/ \E j \in Jobs : StartRebalance(j) \/ FinishJob(j)
    \/ \E j \in Jobs, to \in Disks, o \in Objects, s \in 0..TotalShards : DrainStep(j, to, o, s) \/ ReprotectStep(j, to, o, s)
    \/ \E j \in Jobs, from \in Disks, to \in Disks, o \in Objects, s \in 0..TotalShards : RebalanceStep(j, from, to, o, s)
    \/ \E n \in Nodes : FailNode(n) \/ RecoverNode(n)

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ DiskNodes \subseteq Disks \times Nodes
    /\ \A d \in Disks : Cardinality({n \in Nodes : <<d, n>> \in DiskNodes}) = 1
    /\ \A d \in Disks : disk_state[d] \in {"absent", "active", "draining", "failed", "detached"}
    /\ \A n \in Nodes : node_state[n] \in {"working", "failed"}
    /\ \A d \in Disks : pending[d] \subseteq Pairs \cup Messages /\ stored[d] \subseteq Pairs
    /\ \A j \in Jobs : job_state[j] \in {"idle", "running", "done"}

Durability == \A o \in Objects : object_state[o] = "committed" => Readable(o)
NoDetachedData == \A d \in Disks : disk_state[d] = "detached" => stored[d] = {} /\ pending[d] = {}
NoWritesToDraining == \A d \in Disks : disk_state[d] = "draining" => \A p \in pending[d] : FALSE

NodeObjectPlacements(o, n) ==
    {<<d, s>> \in Disks \times (0..TotalShards) :
        DiskNode(d) = n /\ <<o, s>> \in stored[d]}

NoNodeColocation ==
    NodeLevelDurability =>
        \A o \in Objects, n \in Nodes : Cardinality(NodeObjectPlacements(o, n)) <= 1

=============================================================================
