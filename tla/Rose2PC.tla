--------------------------- MODULE Rose2PC ---------------------------
EXTENDS Integers, FiniteSets

CONSTANTS
    DataNodes,          \* Set of data node identifiers
    DisksPerNode,       \* Set of disk identifiers inside each node
    Txns,               \* Set of transaction identifiers
    MinAcks,            \* Number of distinct node ACKs required for the client to commit
    MinRequired,        \* Number of surviving chunks necessary to read the data (e.g. EC K-value)
    MaxDiskFailures,    \* Maximum number of disk failures to tolerate in the model
    MaxNodeFailures,    \* Maximum number of node failures to tolerate in the model
    NodeLevelDurability \* TRUE to require unique nodes, FALSE to require unique disks

VARIABLES
    msgs,               \* Network messages: [type, txn, node, etc.]
    dn_state,           \* [n \in DataNodes |-> "working" or "failed"]
    disk_state,         \* [n \in DataNodes, d \in DisksPerNode |-> "working" or "failed"]
    disk_data,          \* [n \in DataNodes, d \in DisksPerNode |-> set of Txns written to it]
    mds_committed,      \* Set of Txns successfully committed by the MDS
    client_state        \* [t \in Txns |-> "working", "committing", "committed"]

vars == <<msgs, dn_state, disk_state, disk_data, mds_committed, client_state>>

Init ==
    /\ msgs = {}
    /\ dn_state = [n \in DataNodes |-> "working"]
    /\ disk_state = [n \in DataNodes, d \in DisksPerNode |-> "working"]
    /\ disk_data = [n \in DataNodes, d \in DisksPerNode |-> {}]
    /\ mds_committed = {}
    /\ client_state = [t \in Txns |-> "working"]

-----------------------------------------------------------------------------
\* RPC AND MESSAGE PASSING OPERATIONS

Send(m) == msgs' = msgs \cup {m}

\* Client Phase 1: Client broadcast WRITE to DataNodes for txn t
ClientWrite(t) ==
    /\ client_state[t] = "working"
    /\ client_state' = [client_state EXCEPT ![t] = "committing"]
    /\ msgs' = msgs \cup {[type |-> "WRITE", txn |-> t, dest |-> n] : n \in DataNodes}
    /\ UNCHANGED <<dn_state, disk_state, disk_data, mds_committed>>

\* Data Node RPC Handler: Node n handles WRITE by writing to a working disk d
DNWrite(n, d, t) ==
    /\ dn_state[n] = "working"                 \* Node is alive
    /\ disk_state[n, d] = "working"            \* Chose a working disk
    /\ [type |-> "WRITE", txn |-> t, dest |-> n] \in msgs
    /\ disk_data' = [disk_data EXCEPT ![n, d] = @ \cup {t}]
    /\ msgs' = msgs \cup {[type |-> "ACK", txn |-> t, node |-> n, disk |-> d]}
    /\ UNCHANGED <<dn_state, disk_state, mds_committed, client_state>>

\* Client Phase 2: Client collects ACKs and sends COMMIT
ClientCommit(t) ==
    /\ client_state[t] = "committing"
    /\ \E ack_set \in SUBSET {m \in msgs : m.type = "ACK" /\ m.txn = t}:
        /\ IF NodeLevelDurability
           THEN \A m1, m2 \in ack_set : m1 # m2 => m1.node # m2.node \* Only count unique nodes
           ELSE \A m1, m2 \in ack_set : m1 # m2 => <<m1.node, m1.disk>> # <<m2.node, m2.disk>> \* Only count unique disks
        /\ Cardinality(ack_set) >= MinAcks                      \* Got enough ACKs
        /\ msgs' = msgs \cup {[type |-> "COMMIT", txn |-> t]}
        /\ client_state' = [client_state EXCEPT ![t] = "committed"]
    /\ UNCHANGED <<dn_state, disk_state, disk_data, mds_committed>>

\* MDS RPC Handler: Commits the transaction globally
MDSCommit(t) ==
    /\ [type |-> "COMMIT", txn |-> t] \in msgs
    /\ t \notin mds_committed
    /\ mds_committed' = mds_committed \cup {t}
    /\ UNCHANGED <<msgs, dn_state, disk_state, disk_data, client_state>>

-----------------------------------------------------------------------------
\* FAILURE INJECTION

TotalNodeFailures == Cardinality({n \in DataNodes : dn_state[n] = "failed"})
TotalDiskFailures == Cardinality({<<n, d>> \in DataNodes \times DisksPerNode : disk_state[n, d] = "failed"})

\* Node crashes: Node memory lost, RPCs interrupted (cannot send ACKs anymore)
FailNode(n) ==
    /\ TotalNodeFailures < MaxNodeFailures
    /\ dn_state[n] = "working"
    /\ dn_state' = [dn_state EXCEPT ![n] = "failed"]
    /\ UNCHANGED <<msgs, disk_state, disk_data, mds_committed, client_state>>

\* Disk crashes: Disk contents are zeroed and disk becomes unavailable
FailDisk(n, d) ==
    /\ TotalDiskFailures < MaxDiskFailures
    /\ disk_state[n, d] = "working"
    /\ disk_state' = [disk_state EXCEPT ![n, d] = "failed"]
    /\ disk_data' = [disk_data EXCEPT ![n, d] = {}]
    /\ UNCHANGED <<msgs, dn_state, mds_committed, client_state>>

-----------------------------------------------------------------------------
\* SYSTEM STEP

Next ==
    \/ \E t \in Txns : ClientWrite(t)
    \/ \E n \in DataNodes, d \in DisksPerNode, t \in Txns : DNWrite(n, d, t)
    \/ \E t \in Txns : ClientCommit(t)
    \/ \E t \in Txns : MDSCommit(t)
    \/ \E n \in DataNodes : FailNode(n)
    \/ \E n \in DataNodes, d \in DisksPerNode : FailDisk(n, d)

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
\* INVARIANTS AND PROPERTIES

\* Count how many distinct working nodes or working disks contain tx t
SurvivingChunks(t) ==
    IF NodeLevelDurability THEN
        Cardinality({n \in DataNodes :
            dn_state[n] = "working" /\
            \E d \in DisksPerNode : disk_state[n, d] = "working" /\ t \in disk_data[n, d]
        })
    ELSE
        Cardinality({<<n, d>> \in DataNodes \times DisksPerNode :
            dn_state[n] = "working" /\ disk_state[n, d] = "working" /\ t \in disk_data[n, d]
        })

\* Durability states that if a transaction is committed, it must be readable.
\* Readable means at least MinRequired chunks are surviving.
Durability ==
    \A t \in mds_committed :
        SurvivingChunks(t) >= MinRequired

=============================================================================
