---- MODULE RoseEC_TTrace_1771635280 ----
EXTENDS Sequences, RoseEC, TLCExt, Toolbox, Naturals, TLC

_expression ==
    LET RoseEC_TEExpression == INSTANCE RoseEC_TEExpression
    IN RoseEC_TEExpression!expression
----

_trace ==
    LET RoseEC_TETrace == INSTANCE RoseEC_TETrace
    IN RoseEC_TETrace!trace
----

_inv ==
    ~(
        TLCGet("level") = Len(_TETrace)
        /\
        msgs = ({[type |-> "COMMIT", txn |-> "t1", mode |-> "EC"], [type |-> "WRITE", txn |-> "t1", shard |-> 1, dest |-> "n1"], [type |-> "WRITE", txn |-> "t1", shard |-> 2, dest |-> "n2"], [type |-> "WRITE", txn |-> "t1", shard |-> 3, dest |-> "n3"], [type |-> "ACK", txn |-> "t1", shard |-> 1, node |-> "n1", disk |-> "d1"], [type |-> "ACK", txn |-> "t1", shard |-> 2, node |-> "n2", disk |-> "d1"], [type |-> "ACK", txn |-> "t1", shard |-> 3, node |-> "n3", disk |-> "d1"]})
        /\
        mds_committed = ({"t1"})
        /\
        client_state = ([t1 |-> "committed"])
        /\
        disk_state = ((<<"n1", "d1">> :> "working" @@ <<"n2", "d1">> :> "failed" @@ <<"n3", "d1">> :> "working"))
        /\
        dn_state = ([n1 |-> "failed", n2 |-> "working", n3 |-> "working"])
        /\
        txn_mode = ([t1 |-> "EC"])
        /\
        disk_data = ((<<"n1", "d1">> :> {<<"t1", 1>>} @@ <<"n2", "d1">> :> {} @@ <<"n3", "d1">> :> {<<"t1", 3>>}))
    )
----

_init ==
    /\ msgs = _TETrace[1].msgs
    /\ dn_state = _TETrace[1].dn_state
    /\ client_state = _TETrace[1].client_state
    /\ txn_mode = _TETrace[1].txn_mode
    /\ disk_state = _TETrace[1].disk_state
    /\ disk_data = _TETrace[1].disk_data
    /\ mds_committed = _TETrace[1].mds_committed
----

_next ==
    /\ \E i,j \in DOMAIN _TETrace:
        /\ \/ /\ j = i + 1
              /\ i = TLCGet("level")
        /\ msgs  = _TETrace[i].msgs
        /\ msgs' = _TETrace[j].msgs
        /\ dn_state  = _TETrace[i].dn_state
        /\ dn_state' = _TETrace[j].dn_state
        /\ client_state  = _TETrace[i].client_state
        /\ client_state' = _TETrace[j].client_state
        /\ txn_mode  = _TETrace[i].txn_mode
        /\ txn_mode' = _TETrace[j].txn_mode
        /\ disk_state  = _TETrace[i].disk_state
        /\ disk_state' = _TETrace[j].disk_state
        /\ disk_data  = _TETrace[i].disk_data
        /\ disk_data' = _TETrace[j].disk_data
        /\ mds_committed  = _TETrace[i].mds_committed
        /\ mds_committed' = _TETrace[j].mds_committed

\* Uncomment the ASSUME below to write the states of the error trace
\* to the given file in Json format. Note that you can pass any tuple
\* to `JsonSerialize`. For example, a sub-sequence of _TETrace.
    \* ASSUME
    \*     LET J == INSTANCE Json
    \*         IN J!JsonSerialize("RoseEC_TTrace_1771635280.json", _TETrace)

=============================================================================

 Note that you can extract this module `RoseEC_TEExpression`
  to a dedicated file to reuse `expression` (the module in the 
  dedicated `RoseEC_TEExpression.tla` file takes precedence 
  over the module `RoseEC_TEExpression` below).

---- MODULE RoseEC_TEExpression ----
EXTENDS Sequences, RoseEC, TLCExt, Toolbox, Naturals, TLC

expression == 
    [
        \* To hide variables of the `RoseEC` spec from the error trace,
        \* remove the variables below.  The trace will be written in the order
        \* of the fields of this record.
        msgs |-> msgs
        ,dn_state |-> dn_state
        ,client_state |-> client_state
        ,txn_mode |-> txn_mode
        ,disk_state |-> disk_state
        ,disk_data |-> disk_data
        ,mds_committed |-> mds_committed
        
        \* Put additional constant-, state-, and action-level expressions here:
        \* ,_stateNumber |-> _TEPosition
        \* ,_msgsUnchanged |-> msgs = msgs'
        
        \* Format the `msgs` variable as Json value.
        \* ,_msgsJson |->
        \*     LET J == INSTANCE Json
        \*     IN J!ToJson(msgs)
        
        \* Lastly, you may build expressions over arbitrary sets of states by
        \* leveraging the _TETrace operator.  For example, this is how to
        \* count the number of times a spec variable changed up to the current
        \* state in the trace.
        \* ,_msgsModCount |->
        \*     LET F[s \in DOMAIN _TETrace] ==
        \*         IF s = 1 THEN 0
        \*         ELSE IF _TETrace[s].msgs # _TETrace[s-1].msgs
        \*             THEN 1 + F[s-1] ELSE F[s-1]
        \*     IN F[_TEPosition - 1]
    ]

=============================================================================



Parsing and semantic processing can take forever if the trace below is long.
 In this case, it is advised to uncomment the module below to deserialize the
 trace from a generated binary file.

\*
\*---- MODULE RoseEC_TETrace ----
\*EXTENDS IOUtils, RoseEC, TLC
\*
\*trace == IODeserialize("RoseEC_TTrace_1771635280.bin", TRUE)
\*
\*=============================================================================
\*

---- MODULE RoseEC_TETrace ----
EXTENDS RoseEC, TLC

trace == 
    <<
    ([msgs |-> {},mds_committed |-> {},client_state |-> [t1 |-> "working"],disk_state |-> (<<"n1", "d1">> :> "working" @@ <<"n2", "d1">> :> "working" @@ <<"n3", "d1">> :> "working"),dn_state |-> [n1 |-> "working", n2 |-> "working", n3 |-> "working"],txn_mode |-> [t1 |-> "NONE"],disk_data |-> (<<"n1", "d1">> :> {} @@ <<"n2", "d1">> :> {} @@ <<"n3", "d1">> :> {})]),
    ([msgs |-> {[type |-> "WRITE", txn |-> "t1", shard |-> 1, dest |-> "n1"], [type |-> "WRITE", txn |-> "t1", shard |-> 2, dest |-> "n2"], [type |-> "WRITE", txn |-> "t1", shard |-> 3, dest |-> "n3"]},mds_committed |-> {},client_state |-> [t1 |-> "committing"],disk_state |-> (<<"n1", "d1">> :> "working" @@ <<"n2", "d1">> :> "working" @@ <<"n3", "d1">> :> "working"),dn_state |-> [n1 |-> "working", n2 |-> "working", n3 |-> "working"],txn_mode |-> [t1 |-> "EC"],disk_data |-> (<<"n1", "d1">> :> {} @@ <<"n2", "d1">> :> {} @@ <<"n3", "d1">> :> {})]),
    ([msgs |-> {[type |-> "WRITE", txn |-> "t1", shard |-> 1, dest |-> "n1"], [type |-> "WRITE", txn |-> "t1", shard |-> 2, dest |-> "n2"], [type |-> "WRITE", txn |-> "t1", shard |-> 3, dest |-> "n3"], [type |-> "ACK", txn |-> "t1", shard |-> 1, node |-> "n1", disk |-> "d1"]},mds_committed |-> {},client_state |-> [t1 |-> "committing"],disk_state |-> (<<"n1", "d1">> :> "working" @@ <<"n2", "d1">> :> "working" @@ <<"n3", "d1">> :> "working"),dn_state |-> [n1 |-> "working", n2 |-> "working", n3 |-> "working"],txn_mode |-> [t1 |-> "EC"],disk_data |-> (<<"n1", "d1">> :> {<<"t1", 1>>} @@ <<"n2", "d1">> :> {} @@ <<"n3", "d1">> :> {})]),
    ([msgs |-> {[type |-> "WRITE", txn |-> "t1", shard |-> 1, dest |-> "n1"], [type |-> "WRITE", txn |-> "t1", shard |-> 2, dest |-> "n2"], [type |-> "WRITE", txn |-> "t1", shard |-> 3, dest |-> "n3"], [type |-> "ACK", txn |-> "t1", shard |-> 1, node |-> "n1", disk |-> "d1"], [type |-> "ACK", txn |-> "t1", shard |-> 2, node |-> "n2", disk |-> "d1"]},mds_committed |-> {},client_state |-> [t1 |-> "committing"],disk_state |-> (<<"n1", "d1">> :> "working" @@ <<"n2", "d1">> :> "working" @@ <<"n3", "d1">> :> "working"),dn_state |-> [n1 |-> "working", n2 |-> "working", n3 |-> "working"],txn_mode |-> [t1 |-> "EC"],disk_data |-> (<<"n1", "d1">> :> {<<"t1", 1>>} @@ <<"n2", "d1">> :> {<<"t1", 2>>} @@ <<"n3", "d1">> :> {})]),
    ([msgs |-> {[type |-> "WRITE", txn |-> "t1", shard |-> 1, dest |-> "n1"], [type |-> "WRITE", txn |-> "t1", shard |-> 2, dest |-> "n2"], [type |-> "WRITE", txn |-> "t1", shard |-> 3, dest |-> "n3"], [type |-> "ACK", txn |-> "t1", shard |-> 1, node |-> "n1", disk |-> "d1"], [type |-> "ACK", txn |-> "t1", shard |-> 2, node |-> "n2", disk |-> "d1"], [type |-> "ACK", txn |-> "t1", shard |-> 3, node |-> "n3", disk |-> "d1"]},mds_committed |-> {},client_state |-> [t1 |-> "committing"],disk_state |-> (<<"n1", "d1">> :> "working" @@ <<"n2", "d1">> :> "working" @@ <<"n3", "d1">> :> "working"),dn_state |-> [n1 |-> "working", n2 |-> "working", n3 |-> "working"],txn_mode |-> [t1 |-> "EC"],disk_data |-> (<<"n1", "d1">> :> {<<"t1", 1>>} @@ <<"n2", "d1">> :> {<<"t1", 2>>} @@ <<"n3", "d1">> :> {<<"t1", 3>>})]),
    ([msgs |-> {[type |-> "COMMIT", txn |-> "t1", mode |-> "EC"], [type |-> "WRITE", txn |-> "t1", shard |-> 1, dest |-> "n1"], [type |-> "WRITE", txn |-> "t1", shard |-> 2, dest |-> "n2"], [type |-> "WRITE", txn |-> "t1", shard |-> 3, dest |-> "n3"], [type |-> "ACK", txn |-> "t1", shard |-> 1, node |-> "n1", disk |-> "d1"], [type |-> "ACK", txn |-> "t1", shard |-> 2, node |-> "n2", disk |-> "d1"], [type |-> "ACK", txn |-> "t1", shard |-> 3, node |-> "n3", disk |-> "d1"]},mds_committed |-> {},client_state |-> [t1 |-> "committed"],disk_state |-> (<<"n1", "d1">> :> "working" @@ <<"n2", "d1">> :> "working" @@ <<"n3", "d1">> :> "working"),dn_state |-> [n1 |-> "working", n2 |-> "working", n3 |-> "working"],txn_mode |-> [t1 |-> "EC"],disk_data |-> (<<"n1", "d1">> :> {<<"t1", 1>>} @@ <<"n2", "d1">> :> {<<"t1", 2>>} @@ <<"n3", "d1">> :> {<<"t1", 3>>})]),
    ([msgs |-> {[type |-> "COMMIT", txn |-> "t1", mode |-> "EC"], [type |-> "WRITE", txn |-> "t1", shard |-> 1, dest |-> "n1"], [type |-> "WRITE", txn |-> "t1", shard |-> 2, dest |-> "n2"], [type |-> "WRITE", txn |-> "t1", shard |-> 3, dest |-> "n3"], [type |-> "ACK", txn |-> "t1", shard |-> 1, node |-> "n1", disk |-> "d1"], [type |-> "ACK", txn |-> "t1", shard |-> 2, node |-> "n2", disk |-> "d1"], [type |-> "ACK", txn |-> "t1", shard |-> 3, node |-> "n3", disk |-> "d1"]},mds_committed |-> {"t1"},client_state |-> [t1 |-> "committed"],disk_state |-> (<<"n1", "d1">> :> "working" @@ <<"n2", "d1">> :> "working" @@ <<"n3", "d1">> :> "working"),dn_state |-> [n1 |-> "working", n2 |-> "working", n3 |-> "working"],txn_mode |-> [t1 |-> "EC"],disk_data |-> (<<"n1", "d1">> :> {<<"t1", 1>>} @@ <<"n2", "d1">> :> {<<"t1", 2>>} @@ <<"n3", "d1">> :> {<<"t1", 3>>})]),
    ([msgs |-> {[type |-> "COMMIT", txn |-> "t1", mode |-> "EC"], [type |-> "WRITE", txn |-> "t1", shard |-> 1, dest |-> "n1"], [type |-> "WRITE", txn |-> "t1", shard |-> 2, dest |-> "n2"], [type |-> "WRITE", txn |-> "t1", shard |-> 3, dest |-> "n3"], [type |-> "ACK", txn |-> "t1", shard |-> 1, node |-> "n1", disk |-> "d1"], [type |-> "ACK", txn |-> "t1", shard |-> 2, node |-> "n2", disk |-> "d1"], [type |-> "ACK", txn |-> "t1", shard |-> 3, node |-> "n3", disk |-> "d1"]},mds_committed |-> {"t1"},client_state |-> [t1 |-> "committed"],disk_state |-> (<<"n1", "d1">> :> "working" @@ <<"n2", "d1">> :> "working" @@ <<"n3", "d1">> :> "working"),dn_state |-> [n1 |-> "failed", n2 |-> "working", n3 |-> "working"],txn_mode |-> [t1 |-> "EC"],disk_data |-> (<<"n1", "d1">> :> {<<"t1", 1>>} @@ <<"n2", "d1">> :> {<<"t1", 2>>} @@ <<"n3", "d1">> :> {<<"t1", 3>>})]),
    ([msgs |-> {[type |-> "COMMIT", txn |-> "t1", mode |-> "EC"], [type |-> "WRITE", txn |-> "t1", shard |-> 1, dest |-> "n1"], [type |-> "WRITE", txn |-> "t1", shard |-> 2, dest |-> "n2"], [type |-> "WRITE", txn |-> "t1", shard |-> 3, dest |-> "n3"], [type |-> "ACK", txn |-> "t1", shard |-> 1, node |-> "n1", disk |-> "d1"], [type |-> "ACK", txn |-> "t1", shard |-> 2, node |-> "n2", disk |-> "d1"], [type |-> "ACK", txn |-> "t1", shard |-> 3, node |-> "n3", disk |-> "d1"]},mds_committed |-> {"t1"},client_state |-> [t1 |-> "committed"],disk_state |-> (<<"n1", "d1">> :> "working" @@ <<"n2", "d1">> :> "failed" @@ <<"n3", "d1">> :> "working"),dn_state |-> [n1 |-> "failed", n2 |-> "working", n3 |-> "working"],txn_mode |-> [t1 |-> "EC"],disk_data |-> (<<"n1", "d1">> :> {<<"t1", 1>>} @@ <<"n2", "d1">> :> {} @@ <<"n3", "d1">> :> {<<"t1", 3>>})])
    >>
----


=============================================================================

---- CONFIG RoseEC_TTrace_1771635280 ----
CONSTANTS
    DataNodes = { "n1" , "n2" , "n3" }
    DisksPerNode = { "d1" }
    Txns = { "t1" }
    TotalShards = 3
    MinAcksDuplicate = 3
    MinAcksEC = 3
    MinRequiredEC = 2
    MaxDiskFailures = 1
    MaxNodeFailures = 1
    NodeLevelDurability = TRUE

INVARIANT
    _inv

CHECK_DEADLOCK
    \* CHECK_DEADLOCK off because of PROPERTY or INVARIANT above.
    FALSE

INIT
    _init

NEXT
    _next

CONSTANT
    _TETrace <- _trace

ALIAS
    _expression
=============================================================================
\* Generated on Fri Feb 20 17:54:40 MST 2026