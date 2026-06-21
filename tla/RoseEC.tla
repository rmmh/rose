--------------------------- MODULE RoseEC ---------------------------
EXTENDS Integers, FiniteSets

CONSTANTS
    DataNodes,          \* Set of data node identifiers
    DisksPerNode,       \* Set of disk identifiers
    Txns,               \* Set of transaction identifiers
    TotalShards,        \* N+K total shards (for EC). DUPLICATE mode implies shards = {0}
    MinAcksDuplicate,   \* Quorum of duplicate copies required to commit ragged write
    MinAcksEC,          \* Quorum of distinct shards required to commit EC write
    MinRequiredEC,      \* Number of distinct shards required to read the data (N)
    MaxDiskFailures,
    MaxNodeFailures,
    NodeLevelDurability \* TRUE to require unique nodes, FALSE to require unique disks

VARIABLES
    msgs,               \* Network messages
    dn_state,           \* Node health
    disk_state,         \* Disk health
    disk_data,          \* [n \in DataNodes, d \in DisksPerNode |-> set of <<Txn, ShardID>> written to it]
    mds_committed,      \* Set of Txns successfully committed by the MDS
    txn_mode,           \* [t \in Txns |-> "DUPLICATE", "EC", "NONE"]
    client_state        \* [t \in Txns |-> "working", "committing", "committed"]

vars == <<msgs, dn_state, disk_state, disk_data, mds_committed, txn_mode, client_state>>

Init ==
    /\ msgs = {}
    /\ dn_state = [n \in DataNodes |-> "working"]
    /\ disk_state = [n \in DataNodes, d \in DisksPerNode |-> "working"]
    /\ disk_data = [n \in DataNodes, d \in DisksPerNode |-> {}]
    /\ mds_committed = {}
    /\ txn_mode = [t \in Txns |-> "NONE"]
    /\ client_state = [t \in Txns |-> "working"]

-----------------------------------------------------------------------------
\* CLIENT OPERATIONS

\* Client starts a DUPLICATE broadcast: Shard 0 copied everywhere
ClientWriteDuplicate(t) ==
    /\ client_state[t] = "working"
    /\ txn_mode[t] = "NONE"
    /\ client_state' = [client_state EXCEPT ![t] = "committing"]
    /\ txn_mode' = [txn_mode EXCEPT ![t] = "DUPLICATE"]
    /\ msgs' = msgs \cup {[type |-> "WRITE", txn |-> t, shard |-> 0, dest |-> n] : n \in DataNodes}
    /\ UNCHANGED <<dn_state, disk_state, disk_data, mds_committed>>

\* Client starts an EC broadcast: Distinct Shards 1..TotalShards sent to different nodes
ClientWriteEC(t) ==
    /\ client_state[t] = "working"
    /\ txn_mode[t] = "NONE"
    /\ client_state' = [client_state EXCEPT ![t] = "committing"]
    /\ txn_mode' = [txn_mode EXCEPT ![t] = "EC"]
    /\ \E node_shards \in [1..TotalShards -> DataNodes]: \* Assignment of shards to nodes
        /\ IF NodeLevelDurability THEN
              \A s1, s2 \in 1..TotalShards : s1 # s2 => node_shards[s1] # node_shards[s2]
           ELSE TRUE
        /\ msgs' = msgs \cup {[type |-> "WRITE", txn |-> t, shard |-> s, dest |-> node_shards[s]] : s \in 1..TotalShards}
    /\ UNCHANGED <<dn_state, disk_state, disk_data, mds_committed>>

\* Node writes the shard onto a working disk and acks
DNWrite(n, d, t, s) ==
    /\ dn_state[n] = "working"
    /\ disk_state[n, d] = "working"
    /\ [type |-> "WRITE", txn |-> t, shard |-> s, dest |-> n] \in msgs
    /\ disk_data' = [disk_data EXCEPT ![n, d] = @ \cup {<<t, s>>}]
    /\ msgs' = msgs \cup {[type |-> "ACK", txn |-> t, shard |-> s, node |-> n, disk |-> d]}
    /\ UNCHANGED <<dn_state, disk_state, mds_committed, txn_mode, client_state>>

\* Client commits if enough distinct ACKs are received
ClientCommit(t) ==
    /\ client_state[t] = "committing"
    /\ \E ack_set \in SUBSET {m \in msgs : m.type = "ACK" /\ m.txn = t}:
        /\ IF NodeLevelDurability
           THEN \A m1, m2 \in ack_set : m1 # m2 => m1.node # m2.node
           ELSE \A m1, m2 \in ack_set : m1 # m2 => <<m1.node, m1.disk>> # <<m2.node, m2.disk>>

        /\ \/ /\ txn_mode[t] = "DUPLICATE"
              /\ Cardinality(ack_set) >= MinAcksDuplicate
           \/ /\ txn_mode[t] = "EC"
              /\ Cardinality(ack_set) >= MinAcksEC
              /\ \A m1, m2 \in ack_set : m1 # m2 => m1.shard # m2.shard \* EC acks must be for distinct shards!

        /\ msgs' = msgs \cup {[type |-> "COMMIT", txn |-> t, mode |-> txn_mode[t]]}
        /\ client_state' = [client_state EXCEPT ![t] = "committed"]
    /\ UNCHANGED <<dn_state, disk_state, disk_data, mds_committed, txn_mode>>

\* MDS commits the transaction globally
MDSCommit(t) ==
    /\ \E mode \in {"DUPLICATE", "EC"} : [type |-> "COMMIT", txn |-> t, mode |-> mode] \in msgs
    /\ t \notin mds_committed
    /\ mds_committed' = mds_committed \cup {t}
    /\ UNCHANGED <<msgs, dn_state, disk_state, disk_data, txn_mode, client_state>>

-----------------------------------------------------------------------------
\* INVARIANTS AND PROPERTIES HELPERS

SurvivingDistinctShards(t) ==
    IF NodeLevelDurability THEN
        \* Count distinct working nodes that hold at least one distinct valid EC shard
        Cardinality({n \in DataNodes :
            dn_state[n] = "working" /\
            \E d \in DisksPerNode, s \in 1..TotalShards :
               disk_state[n, d] = "working" /\ <<t, s>> \in disk_data[n, d]
        })
    ELSE
        \* Count distinct working disks that hold at least one distinct valid EC shard
        Cardinality({<<n, d>> \in DataNodes \times DisksPerNode :
            dn_state[n] = "working" /\ disk_state[n, d] = "working" /\
            \E s \in 1..TotalShards : <<t, s>> \in disk_data[n, d]
        })

SurvivingDuplicates(t) ==
    Cardinality({<<n, d>> \in DataNodes \times DisksPerNode :
        dn_state[n] = "working" /\ disk_state[n, d] = "working" /\ <<t, 0>> \in disk_data[n, d]
    })

-----------------------------------------------------------------------------
\* TRANSITIONS & REPAIR

\* Upgrade a ragged write to a full EC block.
\* The MDS logically marks the transaction EC, and asks the Client to flush EC chunks.
TransitionToEC(t) ==
    /\ t \in mds_committed
    /\ txn_mode[t] = "DUPLICATE"
    /\ txn_mode' = [txn_mode EXCEPT ![t] = "TRANSITIONING"]
    /\ \E node_shards \in [1..TotalShards -> DataNodes]:
        /\ IF NodeLevelDurability THEN
              \A s1, s2 \in 1..TotalShards : s1 # s2 => node_shards[s1] # node_shards[s2]
           ELSE TRUE
        /\ msgs' = msgs \cup {[type |-> "WRITE", txn |-> t, shard |-> s, dest |-> node_shards[s]] : s \in 1..TotalShards}
    /\ UNCHANGED <<dn_state, disk_state, disk_data, mds_committed, client_state>>

\* The system finalizes the transition once enough EC shards are actually written
FinishTransition(t) ==
    /\ t \in mds_committed
    /\ txn_mode[t] = "TRANSITIONING"
    /\ SurvivingDistinctShards(t) >= MinAcksEC
    /\ txn_mode' = [txn_mode EXCEPT ![t] = "EC"]
    /\ UNCHANGED <<msgs, dn_state, disk_state, disk_data, mds_committed, client_state>>

\* A background scrubber repairs a missing shard/copy onto a distinct Node/Disk.
\* This abstractly groups "repair read phase" with "repair write phase."
RepairShard(t, s) ==
    /\ t \in mds_committed
    /\ \/ /\ txn_mode[t] = "EC" /\ s \in 1..TotalShards
       \/ /\ txn_mode[t] \in {"DUPLICATE", "TRANSITIONING"} /\ s = 0
    \* Select a target node and disk to hold the repaired shard
    /\ \E n \in DataNodes, d \in DisksPerNode:
        /\ dn_state[n] = "working"
        /\ disk_state[n, d] = "working"
        /\ <<t, s>> \notin disk_data[n, d]  \* Repairing to a place that doesn't hold it already
        /\ IF NodeLevelDurability THEN
               \A d2 \in DisksPerNode : <<t, s>> \notin disk_data[n, d2] \* Dont repair onto same node if node durability
           ELSE TRUE
        \* If EC, we MUST be able to assemble the payload to run the repair computation.
        /\ (txn_mode[t] = "EC" =>
             SurvivingDistinctShards(t) >= MinRequiredEC)
        \* If DUPLICATE, we MUST read at least 1 surviving duplicate snippet globally.
        /\ (txn_mode[t] \in {"DUPLICATE", "TRANSITIONING"} =>
             SurvivingDuplicates(t) >= 1)

        \* OPTIMIZATION to prevent State Graph Explosion:
        \* Only repair EC shard if it is completely missing.
        /\ (txn_mode[t] = "EC" =>
             ~\E nx \in DataNodes, dx \in DisksPerNode :
                dn_state[nx] = "working" /\ disk_state[nx, dx] = "working" /\ <<t, s>> \in disk_data[nx, dx])
        \* Only repair DUPLICATE if it has fallen below healthy quorum
        /\ (txn_mode[t] \in {"DUPLICATE", "TRANSITIONING"} =>
             SurvivingDuplicates(t) < MinAcksDuplicate)

        \* Perform the actual repair disk write!
        /\ disk_data' = [disk_data EXCEPT ![n, d] = @ \cup {<<t, s>>}]
    /\ UNCHANGED <<msgs, dn_state, disk_state, mds_committed, txn_mode, client_state>>


-----------------------------------------------------------------------------
\* FAILURE INJECTION

TotalNodeFailures == Cardinality({n \in DataNodes : dn_state[n] = "failed"})
TotalDiskFailures == Cardinality({<<n, d>> \in DataNodes \times DisksPerNode : disk_state[n, d] = "failed"})

FailNode(n) ==
    /\ TotalNodeFailures < MaxNodeFailures
    /\ dn_state[n] = "working"
    /\ dn_state' = [dn_state EXCEPT ![n] = "failed"]
    /\ UNCHANGED <<msgs, disk_state, disk_data, mds_committed, txn_mode, client_state>>

FailDisk(n, d) ==
    /\ TotalDiskFailures < MaxDiskFailures
    /\ disk_state[n, d] = "working"
    /\ disk_state' = [disk_state EXCEPT ![n, d] = "failed"]
    /\ disk_data' = [disk_data EXCEPT ![n, d] = {}]
    /\ UNCHANGED <<msgs, dn_state, mds_committed, txn_mode, client_state>>

-----------------------------------------------------------------------------
\* SYSTEM STEP

Next ==
    \/ \E t \in Txns : ClientWriteDuplicate(t)
    \/ \E t \in Txns : ClientWriteEC(t)
    \/ \E n \in DataNodes, d \in DisksPerNode, t \in Txns :
          \E s \in {0} \cup 1..TotalShards : DNWrite(n, d, t, s)
    \/ \E t \in Txns : ClientCommit(t)
    \/ \E t \in Txns : MDSCommit(t)
    \/ \E t \in Txns : TransitionToEC(t)
    \/ \E t \in Txns : FinishTransition(t)
    \/ \E t \in Txns, s \in {0} \cup 1..TotalShards : RepairShard(t, s)
    \/ \E n \in DataNodes : FailNode(n)
    \/ \E n \in DataNodes, d \in DisksPerNode : FailDisk(n, d)

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
\* DURABILITY INVARIANT

Durability ==
    \A t \in mds_committed :
        \/ (txn_mode[t] = "EC" /\ SurvivingDistinctShards(t) >= MinRequiredEC)
        \/ (txn_mode[t] \in {"DUPLICATE", "TRANSITIONING"} /\ SurvivingDuplicates(t) >= 1)

=============================================================================
