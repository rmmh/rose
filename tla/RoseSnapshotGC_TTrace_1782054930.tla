---- MODULE RoseSnapshotGC_TTrace_1782054930 ----
EXTENDS Sequences, TLCExt, Toolbox, Naturals, TLC, RoseSnapshotGC

_expression ==
    LET RoseSnapshotGC_TEExpression == INSTANCE RoseSnapshotGC_TEExpression
    IN RoseSnapshotGC_TEExpression!expression
----

_trace ==
    LET RoseSnapshotGC_TETrace == INSTANCE RoseSnapshotGC_TETrace
    IN RoseSnapshotGC_TETrace!trace
----

_inv ==
    ~(
        TLCGet("level") = Len(_TETrace)
        /\
        node_refcount = ([r0 |-> 0, r1 |-> 1, r2 |-> 0, r3 |-> 0, l0 |-> 1, l1 |-> 2, l2 |-> 1, l3 |-> 0, l4 |-> 0, l5 |-> 0])
        /\
        snapshot_time = ([s0 |-> 1, s1 |-> 0, s2 |-> 0])
        /\
        left_child = ([r0 |-> "l0", r1 |-> "l2", r2 |-> "none", r3 |-> "none"])
        /\
        pinned_chunks = ({"c0"})
        /\
        snapshot_state = ([s0 |-> "active", s1 |-> "empty", s2 |-> "empty"])
        /\
        leaf_chunks = ([l0 |-> {"c0"}, l1 |-> {"c1"}, l2 |-> {"c0"}, l3 |-> {}, l4 |-> {}, l5 |-> {}])
        /\
        chunk_state = ()
        /\
        chunk_refcount = ([c0 |-> 2, c1 |-> 1, c2 |-> 0, c3 |-> 0, c4 |-> 0, c5 |-> 0])
        /\
        right_child = ([r0 |-> "l1", r1 |-> "l1", r2 |-> "none", r3 |-> "none"])
        /\
        snapshot_root = ([s0 |-> "r1", s1 |-> "none", s2 |-> "none"])
        /\
        live_root = ("r1")
        /\
        now = (1)
        /\
        node_state = ([r0 |-> "active", r1 |-> "active", r2 |-> "free", r3 |-> "free", l0 |-> "active", l1 |-> "active", l2 |-> "active", l3 |-> "free", l4 |-> "free", l5 |-> "free"])
    )
----

_init ==
    /\ chunk_state = _TETrace[1].chunk_state
    /\ node_refcount = _TETrace[1].node_refcount
    /\ now = _TETrace[1].now
    /\ pinned_chunks = _TETrace[1].pinned_chunks
    /\ snapshot_root = _TETrace[1].snapshot_root
    /\ chunk_refcount = _TETrace[1].chunk_refcount
    /\ left_child = _TETrace[1].left_child
    /\ snapshot_state = _TETrace[1].snapshot_state
    /\ live_root = _TETrace[1].live_root
    /\ snapshot_time = _TETrace[1].snapshot_time
    /\ right_child = _TETrace[1].right_child
    /\ leaf_chunks = _TETrace[1].leaf_chunks
    /\ node_state = _TETrace[1].node_state
----

_next ==
    /\ \E i,j \in DOMAIN _TETrace:
        /\ \/ /\ j = i + 1
              /\ i = TLCGet("level")
        /\ chunk_state  = _TETrace[i].chunk_state
        /\ chunk_state' = _TETrace[j].chunk_state
        /\ node_refcount  = _TETrace[i].node_refcount
        /\ node_refcount' = _TETrace[j].node_refcount
        /\ now  = _TETrace[i].now
        /\ now' = _TETrace[j].now
        /\ pinned_chunks  = _TETrace[i].pinned_chunks
        /\ pinned_chunks' = _TETrace[j].pinned_chunks
        /\ snapshot_root  = _TETrace[i].snapshot_root
        /\ snapshot_root' = _TETrace[j].snapshot_root
        /\ chunk_refcount  = _TETrace[i].chunk_refcount
        /\ chunk_refcount' = _TETrace[j].chunk_refcount
        /\ left_child  = _TETrace[i].left_child
        /\ left_child' = _TETrace[j].left_child
        /\ snapshot_state  = _TETrace[i].snapshot_state
        /\ snapshot_state' = _TETrace[j].snapshot_state
        /\ live_root  = _TETrace[i].live_root
        /\ live_root' = _TETrace[j].live_root
        /\ snapshot_time  = _TETrace[i].snapshot_time
        /\ snapshot_time' = _TETrace[j].snapshot_time
        /\ right_child  = _TETrace[i].right_child
        /\ right_child' = _TETrace[j].right_child
        /\ leaf_chunks  = _TETrace[i].leaf_chunks
        /\ leaf_chunks' = _TETrace[j].leaf_chunks
        /\ node_state  = _TETrace[i].node_state
        /\ node_state' = _TETrace[j].node_state

\* Uncomment the ASSUME below to write the states of the error trace
\* to the given file in Json format. Note that you can pass any tuple
\* to `JsonSerialize`. For example, a sub-sequence of _TETrace.
    \* ASSUME
    \*     LET J == INSTANCE Json
    \*         IN J!JsonSerialize("RoseSnapshotGC_TTrace_1782054930.json", _TETrace)

=============================================================================

 Note that you can extract this module `RoseSnapshotGC_TEExpression`
  to a dedicated file to reuse `expression` (the module in the 
  dedicated `RoseSnapshotGC_TEExpression.tla` file takes precedence 
  over the module `RoseSnapshotGC_TEExpression` below).

---- MODULE RoseSnapshotGC_TEExpression ----
EXTENDS Sequences, TLCExt, Toolbox, Naturals, TLC, RoseSnapshotGC

expression == 
    [
        \* To hide variables of the `RoseSnapshotGC` spec from the error trace,
        \* remove the variables below.  The trace will be written in the order
        \* of the fields of this record.
        chunk_state |-> chunk_state
        ,node_refcount |-> node_refcount
        ,now |-> now
        ,pinned_chunks |-> pinned_chunks
        ,snapshot_root |-> snapshot_root
        ,chunk_refcount |-> chunk_refcount
        ,left_child |-> left_child
        ,snapshot_state |-> snapshot_state
        ,live_root |-> live_root
        ,snapshot_time |-> snapshot_time
        ,right_child |-> right_child
        ,leaf_chunks |-> leaf_chunks
        ,node_state |-> node_state
        
        \* Put additional constant-, state-, and action-level expressions here:
        \* ,_stateNumber |-> _TEPosition
        \* ,_chunk_stateUnchanged |-> chunk_state = chunk_state'
        
        \* Format the `chunk_state` variable as Json value.
        \* ,_chunk_stateJson |->
        \*     LET J == INSTANCE Json
        \*     IN J!ToJson(chunk_state)
        
        \* Lastly, you may build expressions over arbitrary sets of states by
        \* leveraging the _TETrace operator.  For example, this is how to
        \* count the number of times a spec variable changed up to the current
        \* state in the trace.
        \* ,_chunk_stateModCount |->
        \*     LET F[s \in DOMAIN _TETrace] ==
        \*         IF s = 1 THEN 0
        \*         ELSE IF _TETrace[s].chunk_state # _TETrace[s-1].chunk_state
        \*             THEN 1 + F[s-1] ELSE F[s-1]
        \*     IN F[_TEPosition - 1]
    ]

=============================================================================



Parsing and semantic processing can take forever if the trace below is long.
 In this case, it is advised to uncomment the module below to deserialize the
 trace from a generated binary file.

\*
\*---- MODULE RoseSnapshotGC_TETrace ----
\*EXTENDS IOUtils, TLC, RoseSnapshotGC
\*
\*trace == IODeserialize("RoseSnapshotGC_TTrace_1782054930.bin", TRUE)
\*
\*=============================================================================
\*

---- MODULE RoseSnapshotGC_TETrace ----
EXTENDS TLC, RoseSnapshotGC

trace == 
    <<
    ([node_refcount |-> [r0 |-> 1, r1 |-> 0, r2 |-> 0, r3 |-> 0, l0 |-> 1, l1 |-> 1, l2 |-> 0, l3 |-> 0, l4 |-> 0, l5 |-> 0],snapshot_time |-> [s0 |-> 0, s1 |-> 0, s2 |-> 0],left_child |-> [r0 |-> "l0", r1 |-> "none", r2 |-> "none", r3 |-> "none"],pinned_chunks |-> {},snapshot_state |-> [s0 |-> "empty", s1 |-> "empty", s2 |-> "empty"],leaf_chunks |-> [l0 |-> {"c0"}, l1 |-> {"c1"}, l2 |-> {}, l3 |-> {}, l4 |-> {}, l5 |-> {}],chunk_state |-> [c0 |-> "committed", c1 |-> "committed", c2 |-> "new", c3 |-> "new", c4 |-> "new", c5 |-> "new"],chunk_refcount |-> [c0 |-> 1, c1 |-> 1, c2 |-> 0, c3 |-> 0, c4 |-> 0, c5 |-> 0],right_child |-> [r0 |-> "l1", r1 |-> "none", r2 |-> "none", r3 |-> "none"],snapshot_root |-> [s0 |-> "none", s1 |-> "none", s2 |-> "none"],live_root |-> "r0",now |-> 0,node_state |-> [r0 |-> "active", r1 |-> "free", r2 |-> "free", r3 |-> "free", l0 |-> "active", l1 |-> "active", l2 |-> "free", l3 |-> "free", l4 |-> "free", l5 |-> "free"]]),
    ([node_refcount |-> [r0 |-> 1, r1 |-> 0, r2 |-> 0, r3 |-> 0, l0 |-> 1, l1 |-> 1, l2 |-> 0, l3 |-> 0, l4 |-> 0, l5 |-> 0],snapshot_time |-> [s0 |-> 0, s1 |-> 0, s2 |-> 0],left_child |-> [r0 |-> "l0", r1 |-> "none", r2 |-> "none", r3 |-> "none"],pinned_chunks |-> {"c0"},snapshot_state |-> [s0 |-> "empty", s1 |-> "empty", s2 |-> "empty"],leaf_chunks |-> [l0 |-> {"c0"}, l1 |-> {"c1"}, l2 |-> {}, l3 |-> {}, l4 |-> {}, l5 |-> {}],chunk_state |-> [c0 |-> "committed", c1 |-> "committed", c2 |-> "new", c3 |-> "new", c4 |-> "new", c5 |-> "new"],chunk_refcount |-> [c0 |-> 1, c1 |-> 1, c2 |-> 0, c3 |-> 0, c4 |-> 0, c5 |-> 0],right_child |-> [r0 |-> "l1", r1 |-> "none", r2 |-> "none", r3 |-> "none"],snapshot_root |-> [s0 |-> "none", s1 |-> "none", s2 |-> "none"],live_root |-> "r0",now |-> 0,node_state |-> [r0 |-> "active", r1 |-> "free", r2 |-> "free", r3 |-> "free", l0 |-> "active", l1 |-> "active", l2 |-> "free", l3 |-> "free", l4 |-> "free", l5 |-> "free"]]),
    ([node_refcount |-> [r0 |-> 0, r1 |-> 1, r2 |-> 0, r3 |-> 0, l0 |-> 1, l1 |-> 2, l2 |-> 1, l3 |-> 0, l4 |-> 0, l5 |-> 0],snapshot_time |-> [s0 |-> 1, s1 |-> 0, s2 |-> 0],left_child |-> [r0 |-> "l0", r1 |-> "l2", r2 |-> "none", r3 |-> "none"],pinned_chunks |-> {"c0"},snapshot_state |-> [s0 |-> "active", s1 |-> "empty", s2 |-> "empty"],leaf_chunks |-> [l0 |-> {"c0"}, l1 |-> {"c1"}, l2 |-> {"c0"}, l3 |-> {}, l4 |-> {}, l5 |-> {}],chunk_state |-> ,chunk_refcount |-> [c0 |-> 2, c1 |-> 1, c2 |-> 0, c3 |-> 0, c4 |-> 0, c5 |-> 0],right_child |-> [r0 |-> "l1", r1 |-> "l1", r2 |-> "none", r3 |-> "none"],snapshot_root |-> [s0 |-> "r1", s1 |-> "none", s2 |-> "none"],live_root |-> "r1",now |-> 1,node_state |-> [r0 |-> "active", r1 |-> "active", r2 |-> "free", r3 |-> "free", l0 |-> "active", l1 |-> "active", l2 |-> "active", l3 |-> "free", l4 |-> "free", l5 |-> "free"]])
    >>
----


=============================================================================

---- CONFIG RoseSnapshotGC_TTrace_1782054930 ----
CONSTANTS
    RootNodes = { "r0" , "r1" , "r2" , "r3" }
    LeafNodes = { "l0" , "l1" , "l2" , "l3" , "l4" , "l5" }
    Chunks = { "c0" , "c1" , "c2" , "c3" , "c4" , "c5" }
    Snapshots = { "s0" , "s1" , "s2" }
    ContinuousWindow = 1
    DailyWindow = 3
    WeeklyWindow = 6
    DailyPeriod = 2
    WeeklyPeriod = 4

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
\* Generated on Sun Jun 21 09:15:30 MDT 2026