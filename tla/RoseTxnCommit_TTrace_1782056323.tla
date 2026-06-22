---- MODULE RoseTxnCommit_TTrace_1782056323 ----
EXTENDS Sequences, TLCExt, RoseTxnCommit, Toolbox, Naturals, TLC

_expression ==
    LET RoseTxnCommit_TEExpression == INSTANCE RoseTxnCommit_TEExpression
    IN RoseTxnCommit_TEExpression!expression
----

_trace ==
    LET RoseTxnCommit_TETrace == INSTANCE RoseTxnCommit_TETrace
    IN RoseTxnCommit_TETrace!trace
----

_inv ==
    ~(
        TLCGet("level") = Len(_TETrace)
        /\
        snapshots = ({"t2"})
        /\
        disk_state = ([d1 |-> "failed", d2 |-> "active", d3 |-> "active"])
        /\
        durable_records = ([d1 |-> {}, d2 |-> {}, d3 |-> {}])
        /\
        published = ({"t2"})
        /\
        placement = ([t1 |-> {}, t2 |-> {<<"d1", 1>>, <<"d1", 2>>, <<"d1", 3>>}])
        /\
        volatile_records = ([d1 |-> {}, d2 |-> {}, d3 |-> {}])
        /\
        txn_state = ([t1 |-> "new", t2 |-> "published"])
    )
----

_init ==
    /\ txn_state = _TETrace[1].txn_state
    /\ published = _TETrace[1].published
    /\ snapshots = _TETrace[1].snapshots
    /\ durable_records = _TETrace[1].durable_records
    /\ disk_state = _TETrace[1].disk_state
    /\ placement = _TETrace[1].placement
    /\ volatile_records = _TETrace[1].volatile_records
----

_next ==
    /\ \E i,j \in DOMAIN _TETrace:
        /\ \/ /\ j = i + 1
              /\ i = TLCGet("level")
        /\ txn_state  = _TETrace[i].txn_state
        /\ txn_state' = _TETrace[j].txn_state
        /\ published  = _TETrace[i].published
        /\ published' = _TETrace[j].published
        /\ snapshots  = _TETrace[i].snapshots
        /\ snapshots' = _TETrace[j].snapshots
        /\ durable_records  = _TETrace[i].durable_records
        /\ durable_records' = _TETrace[j].durable_records
        /\ disk_state  = _TETrace[i].disk_state
        /\ disk_state' = _TETrace[j].disk_state
        /\ placement  = _TETrace[i].placement
        /\ placement' = _TETrace[j].placement
        /\ volatile_records  = _TETrace[i].volatile_records
        /\ volatile_records' = _TETrace[j].volatile_records

\* Uncomment the ASSUME below to write the states of the error trace
\* to the given file in Json format. Note that you can pass any tuple
\* to `JsonSerialize`. For example, a sub-sequence of _TETrace.
    \* ASSUME
    \*     LET J == INSTANCE Json
    \*         IN J!JsonSerialize("RoseTxnCommit_TTrace_1782056323.json", _TETrace)

=============================================================================

 Note that you can extract this module `RoseTxnCommit_TEExpression`
  to a dedicated file to reuse `expression` (the module in the 
  dedicated `RoseTxnCommit_TEExpression.tla` file takes precedence 
  over the module `RoseTxnCommit_TEExpression` below).

---- MODULE RoseTxnCommit_TEExpression ----
EXTENDS Sequences, TLCExt, RoseTxnCommit, Toolbox, Naturals, TLC

expression == 
    [
        \* To hide variables of the `RoseTxnCommit` spec from the error trace,
        \* remove the variables below.  The trace will be written in the order
        \* of the fields of this record.
        txn_state |-> txn_state
        ,published |-> published
        ,snapshots |-> snapshots
        ,durable_records |-> durable_records
        ,disk_state |-> disk_state
        ,placement |-> placement
        ,volatile_records |-> volatile_records
        
        \* Put additional constant-, state-, and action-level expressions here:
        \* ,_stateNumber |-> _TEPosition
        \* ,_txn_stateUnchanged |-> txn_state = txn_state'
        
        \* Format the `txn_state` variable as Json value.
        \* ,_txn_stateJson |->
        \*     LET J == INSTANCE Json
        \*     IN J!ToJson(txn_state)
        
        \* Lastly, you may build expressions over arbitrary sets of states by
        \* leveraging the _TETrace operator.  For example, this is how to
        \* count the number of times a spec variable changed up to the current
        \* state in the trace.
        \* ,_txn_stateModCount |->
        \*     LET F[s \in DOMAIN _TETrace] ==
        \*         IF s = 1 THEN 0
        \*         ELSE IF _TETrace[s].txn_state # _TETrace[s-1].txn_state
        \*             THEN 1 + F[s-1] ELSE F[s-1]
        \*     IN F[_TEPosition - 1]
    ]

=============================================================================



Parsing and semantic processing can take forever if the trace below is long.
 In this case, it is advised to uncomment the module below to deserialize the
 trace from a generated binary file.

\*
\*---- MODULE RoseTxnCommit_TETrace ----
\*EXTENDS IOUtils, RoseTxnCommit, TLC
\*
\*trace == IODeserialize("RoseTxnCommit_TTrace_1782056323.bin", TRUE)
\*
\*=============================================================================
\*

---- MODULE RoseTxnCommit_TETrace ----
EXTENDS RoseTxnCommit, TLC

trace == 
    <<
    ([snapshots |-> {},disk_state |-> [d1 |-> "active", d2 |-> "active", d3 |-> "active"],durable_records |-> [d1 |-> {}, d2 |-> {}, d3 |-> {}],published |-> {},placement |-> [t1 |-> {}, t2 |-> {}],volatile_records |-> [d1 |-> {}, d2 |-> {}, d3 |-> {}],txn_state |-> [t1 |-> "new", t2 |-> "new"]]),
    ([snapshots |-> {},disk_state |-> [d1 |-> "active", d2 |-> "active", d3 |-> "active"],durable_records |-> [d1 |-> {}, d2 |-> {}, d3 |-> {}],published |-> {},placement |-> [t1 |-> {}, t2 |-> {}],volatile_records |-> [d1 |-> {}, d2 |-> {}, d3 |-> {}],txn_state |-> [t1 |-> "new", t2 |-> "open"]]),
    ([snapshots |-> {},disk_state |-> [d1 |-> "active", d2 |-> "active", d3 |-> "active"],durable_records |-> [d1 |-> {}, d2 |-> {}, d3 |-> {}],published |-> {},placement |-> [t1 |-> {}, t2 |-> {}],volatile_records |-> [d1 |-> {<<"t2", 1>>}, d2 |-> {}, d3 |-> {}],txn_state |-> [t1 |-> "new", t2 |-> "prepared"]]),
    ([snapshots |-> {},disk_state |-> [d1 |-> "active", d2 |-> "active", d3 |-> "active"],durable_records |-> [d1 |-> {<<"t2", 1>>}, d2 |-> {}, d3 |-> {}],published |-> {},placement |-> [t1 |-> {}, t2 |-> {}],volatile_records |-> [d1 |-> {}, d2 |-> {}, d3 |-> {}],txn_state |-> [t1 |-> "new", t2 |-> "prepared"]]),
    ([snapshots |-> {},disk_state |-> [d1 |-> "active", d2 |-> "active", d3 |-> "active"],durable_records |-> [d1 |-> {<<"t2", 1>>}, d2 |-> {}, d3 |-> {}],published |-> {},placement |-> [t1 |-> {}, t2 |-> {}],volatile_records |-> [d1 |-> {<<"t2", 2>>}, d2 |-> {}, d3 |-> {}],txn_state |-> [t1 |-> "new", t2 |-> "prepared"]]),
    ([snapshots |-> {},disk_state |-> [d1 |-> "active", d2 |-> "active", d3 |-> "active"],durable_records |-> [d1 |-> {<<"t2", 1>>, <<"t2", 2>>}, d2 |-> {}, d3 |-> {}],published |-> {},placement |-> [t1 |-> {}, t2 |-> {}],volatile_records |-> [d1 |-> {}, d2 |-> {}, d3 |-> {}],txn_state |-> [t1 |-> "new", t2 |-> "prepared"]]),
    ([snapshots |-> {},disk_state |-> [d1 |-> "active", d2 |-> "active", d3 |-> "active"],durable_records |-> [d1 |-> {<<"t2", 1>>, <<"t2", 2>>}, d2 |-> {}, d3 |-> {}],published |-> {},placement |-> [t1 |-> {}, t2 |-> {}],volatile_records |-> [d1 |-> {<<"t2", 3>>}, d2 |-> {}, d3 |-> {}],txn_state |-> [t1 |-> "new", t2 |-> "prepared"]]),
    ([snapshots |-> {},disk_state |-> [d1 |-> "active", d2 |-> "active", d3 |-> "active"],durable_records |-> [d1 |-> {<<"t2", 1>>, <<"t2", 2>>, <<"t2", 3>>}, d2 |-> {}, d3 |-> {}],published |-> {},placement |-> [t1 |-> {}, t2 |-> {}],volatile_records |-> [d1 |-> {}, d2 |-> {}, d3 |-> {}],txn_state |-> [t1 |-> "new", t2 |-> "prepared"]]),
    ([snapshots |-> {"t2"},disk_state |-> [d1 |-> "active", d2 |-> "active", d3 |-> "active"],durable_records |-> [d1 |-> {<<"t2", 1>>, <<"t2", 2>>, <<"t2", 3>>}, d2 |-> {}, d3 |-> {}],published |-> {"t2"},placement |-> [t1 |-> {}, t2 |-> {<<"d1", 1>>, <<"d1", 2>>, <<"d1", 3>>}],volatile_records |-> [d1 |-> {}, d2 |-> {}, d3 |-> {}],txn_state |-> [t1 |-> "new", t2 |-> "published"]]),
    ([snapshots |-> {"t2"},disk_state |-> [d1 |-> "failed", d2 |-> "active", d3 |-> "active"],durable_records |-> [d1 |-> {}, d2 |-> {}, d3 |-> {}],published |-> {"t2"},placement |-> [t1 |-> {}, t2 |-> {<<"d1", 1>>, <<"d1", 2>>, <<"d1", 3>>}],volatile_records |-> [d1 |-> {}, d2 |-> {}, d3 |-> {}],txn_state |-> [t1 |-> "new", t2 |-> "published"]])
    >>
----


=============================================================================

---- CONFIG RoseTxnCommit_TTrace_1782056323 ----
CONSTANTS
    Txns = { "t1" , "t2" }
    Disks = { "d1" , "d2" , "d3" }
    TotalShards = 3
    MinReadShards = 2
    MinCommitShards = 2
    MaxDiskFailures = 1
    AllowDegradedWrites = FALSE

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
\* Generated on Sun Jun 21 09:38:46 MDT 2026