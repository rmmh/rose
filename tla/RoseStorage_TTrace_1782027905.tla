---- MODULE RoseStorage_TTrace_1782027905 ----
EXTENDS Sequences, TLCExt, Toolbox, Naturals, TLC, RoseStorage

_expression ==
    LET RoseStorage_TEExpression == INSTANCE RoseStorage_TEExpression
    IN RoseStorage_TEExpression!expression
----

_trace ==
    LET RoseStorage_TETrace == INSTANCE RoseStorage_TETrace
    IN RoseStorage_TETrace!trace
----

_inv ==
    ~(
        TLCGet("level") = Len(_TETrace)
        /\
        job_progress = ([j1 |-> FALSE])
        /\
        disk_state = ([d1 |-> "failed", d2 |-> "draining", d3 |-> "active", d4 |-> "absent"])
        /\
        pending = ([d1 |-> {}, d2 |-> {}, d3 |-> {}, d4 |-> {}])
        /\
        job_disk = ([j1 |-> "d2"])
        /\
        job_state = ([j1 |-> "running"])
        /\
        plog_ready = ({"d1", "d2"})
        /\
        job_kind = ([j1 |-> "remove"])
        /\
        last_rpc = ("RemoveDisk")
        /\
        stored = ([d1 |-> {}, d2 |-> {<<"o1", 0>>}, d3 |-> {}, d4 |-> {}])
        /\
        node_state = ([d1 |-> "working", d2 |-> "working", d3 |-> "working", d4 |-> "working"])
        /\
        object_state = ([o1 |-> "committed"])
        /\
        file_state = ([o1 |-> "buffered"])
        /\
        object_mode = ([o1 |-> "DUPLICATE"])
    )
----

_init ==
    /\ job_state = _TETrace[1].job_state
    /\ object_mode = _TETrace[1].object_mode
    /\ plog_ready = _TETrace[1].plog_ready
    /\ file_state = _TETrace[1].file_state
    /\ job_progress = _TETrace[1].job_progress
    /\ pending = _TETrace[1].pending
    /\ stored = _TETrace[1].stored
    /\ disk_state = _TETrace[1].disk_state
    /\ job_disk = _TETrace[1].job_disk
    /\ last_rpc = _TETrace[1].last_rpc
    /\ job_kind = _TETrace[1].job_kind
    /\ object_state = _TETrace[1].object_state
    /\ node_state = _TETrace[1].node_state
----

_next ==
    /\ \E i,j \in DOMAIN _TETrace:
        /\ \/ /\ j = i + 1
              /\ i = TLCGet("level")
        /\ job_state  = _TETrace[i].job_state
        /\ job_state' = _TETrace[j].job_state
        /\ object_mode  = _TETrace[i].object_mode
        /\ object_mode' = _TETrace[j].object_mode
        /\ plog_ready  = _TETrace[i].plog_ready
        /\ plog_ready' = _TETrace[j].plog_ready
        /\ file_state  = _TETrace[i].file_state
        /\ file_state' = _TETrace[j].file_state
        /\ job_progress  = _TETrace[i].job_progress
        /\ job_progress' = _TETrace[j].job_progress
        /\ pending  = _TETrace[i].pending
        /\ pending' = _TETrace[j].pending
        /\ stored  = _TETrace[i].stored
        /\ stored' = _TETrace[j].stored
        /\ disk_state  = _TETrace[i].disk_state
        /\ disk_state' = _TETrace[j].disk_state
        /\ job_disk  = _TETrace[i].job_disk
        /\ job_disk' = _TETrace[j].job_disk
        /\ last_rpc  = _TETrace[i].last_rpc
        /\ last_rpc' = _TETrace[j].last_rpc
        /\ job_kind  = _TETrace[i].job_kind
        /\ job_kind' = _TETrace[j].job_kind
        /\ object_state  = _TETrace[i].object_state
        /\ object_state' = _TETrace[j].object_state
        /\ node_state  = _TETrace[i].node_state
        /\ node_state' = _TETrace[j].node_state

\* Uncomment the ASSUME below to write the states of the error trace
\* to the given file in Json format. Note that you can pass any tuple
\* to `JsonSerialize`. For example, a sub-sequence of _TETrace.
    \* ASSUME
    \*     LET J == INSTANCE Json
    \*         IN J!JsonSerialize("RoseStorage_TTrace_1782027905.json", _TETrace)

=============================================================================

 Note that you can extract this module `RoseStorage_TEExpression`
  to a dedicated file to reuse `expression` (the module in the 
  dedicated `RoseStorage_TEExpression.tla` file takes precedence 
  over the module `RoseStorage_TEExpression` below).

---- MODULE RoseStorage_TEExpression ----
EXTENDS Sequences, TLCExt, Toolbox, Naturals, TLC, RoseStorage

expression == 
    [
        \* To hide variables of the `RoseStorage` spec from the error trace,
        \* remove the variables below.  The trace will be written in the order
        \* of the fields of this record.
        job_state |-> job_state
        ,object_mode |-> object_mode
        ,plog_ready |-> plog_ready
        ,file_state |-> file_state
        ,job_progress |-> job_progress
        ,pending |-> pending
        ,stored |-> stored
        ,disk_state |-> disk_state
        ,job_disk |-> job_disk
        ,last_rpc |-> last_rpc
        ,job_kind |-> job_kind
        ,object_state |-> object_state
        ,node_state |-> node_state
        
        \* Put additional constant-, state-, and action-level expressions here:
        \* ,_stateNumber |-> _TEPosition
        \* ,_job_stateUnchanged |-> job_state = job_state'
        
        \* Format the `job_state` variable as Json value.
        \* ,_job_stateJson |->
        \*     LET J == INSTANCE Json
        \*     IN J!ToJson(job_state)
        
        \* Lastly, you may build expressions over arbitrary sets of states by
        \* leveraging the _TETrace operator.  For example, this is how to
        \* count the number of times a spec variable changed up to the current
        \* state in the trace.
        \* ,_job_stateModCount |->
        \*     LET F[s \in DOMAIN _TETrace] ==
        \*         IF s = 1 THEN 0
        \*         ELSE IF _TETrace[s].job_state # _TETrace[s-1].job_state
        \*             THEN 1 + F[s-1] ELSE F[s-1]
        \*     IN F[_TEPosition - 1]
    ]

=============================================================================



Parsing and semantic processing can take forever if the trace below is long.
 In this case, it is advised to uncomment the module below to deserialize the
 trace from a generated binary file.

\*
\*---- MODULE RoseStorage_TETrace ----
\*EXTENDS IOUtils, TLC, RoseStorage
\*
\*trace == IODeserialize("RoseStorage_TTrace_1782027905.bin", TRUE)
\*
\*=============================================================================
\*

---- MODULE RoseStorage_TETrace ----
EXTENDS TLC, RoseStorage

trace == 
    <<
    ([job_progress |-> [j1 |-> FALSE],disk_state |-> [d1 |-> "active", d2 |-> "active", d3 |-> "active", d4 |-> "absent"],pending |-> [d1 |-> {}, d2 |-> {}, d3 |-> {}, d4 |-> {}],job_disk |-> [j1 |-> "none"],job_state |-> [j1 |-> "idle"],plog_ready |-> {},job_kind |-> [j1 |-> "none"],last_rpc |-> "init",stored |-> [d1 |-> {}, d2 |-> {}, d3 |-> {}, d4 |-> {}],node_state |-> [d1 |-> "working", d2 |-> "working", d3 |-> "working", d4 |-> "working"],object_state |-> [o1 |-> "new"],file_state |-> [o1 |-> "new"],object_mode |-> [o1 |-> "NONE"]]),
    ([job_progress |-> [j1 |-> FALSE],disk_state |-> [d1 |-> "active", d2 |-> "active", d3 |-> "active", d4 |-> "absent"],pending |-> [d1 |-> {}, d2 |-> {}, d3 |-> {}, d4 |-> {}],job_disk |-> [j1 |-> "none"],job_state |-> [j1 |-> "idle"],plog_ready |-> {},job_kind |-> [j1 |-> "none"],last_rpc |-> "Open",stored |-> [d1 |-> {}, d2 |-> {}, d3 |-> {}, d4 |-> {}],node_state |-> [d1 |-> "working", d2 |-> "working", d3 |-> "working", d4 |-> "working"],object_state |-> [o1 |-> "new"],file_state |-> [o1 |-> "open"],object_mode |-> [o1 |-> "NONE"]]),
    ([job_progress |-> [j1 |-> FALSE],disk_state |-> [d1 |-> "active", d2 |-> "active", d3 |-> "active", d4 |-> "absent"],pending |-> [d1 |-> {}, d2 |-> {}, d3 |-> {}, d4 |-> {}],job_disk |-> [j1 |-> "none"],job_state |-> [j1 |-> "idle"],plog_ready |-> {},job_kind |-> [j1 |-> "none"],last_rpc |-> "Write",stored |-> [d1 |-> {}, d2 |-> {}, d3 |-> {}, d4 |-> {}],node_state |-> [d1 |-> "working", d2 |-> "working", d3 |-> "working", d4 |-> "working"],object_state |-> [o1 |-> "new"],file_state |-> [o1 |-> "buffered"],object_mode |-> [o1 |-> "NONE"]]),
    ([job_progress |-> [j1 |-> FALSE],disk_state |-> [d1 |-> "active", d2 |-> "active", d3 |-> "active", d4 |-> "absent"],pending |-> [d1 |-> {}, d2 |-> {}, d3 |-> {}, d4 |-> {}],job_disk |-> [j1 |-> "none"],job_state |-> [j1 |-> "idle"],plog_ready |-> {},job_kind |-> [j1 |-> "none"],last_rpc |-> "MakeVlog",stored |-> [d1 |-> {}, d2 |-> {}, d3 |-> {}, d4 |-> {}],node_state |-> [d1 |-> "working", d2 |-> "working", d3 |-> "working", d4 |-> "working"],object_state |-> [o1 |-> "vlog-ready"],file_state |-> [o1 |-> "buffered"],object_mode |-> [o1 |-> "DUPLICATE"]]),
    ([job_progress |-> [j1 |-> FALSE],disk_state |-> [d1 |-> "active", d2 |-> "active", d3 |-> "active", d4 |-> "absent"],pending |-> [d1 |-> {}, d2 |-> {}, d3 |-> {}, d4 |-> {}],job_disk |-> [j1 |-> "none"],job_state |-> [j1 |-> "idle"],plog_ready |-> {"d1"},job_kind |-> [j1 |-> "none"],last_rpc |-> "MakePlog",stored |-> [d1 |-> {}, d2 |-> {}, d3 |-> {}, d4 |-> {}],node_state |-> [d1 |-> "working", d2 |-> "working", d3 |-> "working", d4 |-> "working"],object_state |-> [o1 |-> "vlog-ready"],file_state |-> [o1 |-> "buffered"],object_mode |-> [o1 |-> "DUPLICATE"]]),
    ([job_progress |-> [j1 |-> FALSE],disk_state |-> [d1 |-> "active", d2 |-> "active", d3 |-> "active", d4 |-> "absent"],pending |-> [d1 |-> {}, d2 |-> {}, d3 |-> {}, d4 |-> {}],job_disk |-> [j1 |-> "none"],job_state |-> [j1 |-> "idle"],plog_ready |-> {"d1", "d2"},job_kind |-> [j1 |-> "none"],last_rpc |-> "MakePlog",stored |-> [d1 |-> {}, d2 |-> {}, d3 |-> {}, d4 |-> {}],node_state |-> [d1 |-> "working", d2 |-> "working", d3 |-> "working", d4 |-> "working"],object_state |-> [o1 |-> "vlog-ready"],file_state |-> [o1 |-> "buffered"],object_mode |-> [o1 |-> "DUPLICATE"]]),
    ([job_progress |-> [j1 |-> FALSE],disk_state |-> [d1 |-> "active", d2 |-> "active", d3 |-> "active", d4 |-> "absent"],pending |-> [d1 |-> {}, d2 |-> {}, d3 |-> {}, d4 |-> {}],job_disk |-> [j1 |-> "none"],job_state |-> [j1 |-> "idle"],plog_ready |-> {"d1", "d2"},job_kind |-> [j1 |-> "none"],last_rpc |-> "WriteVlog",stored |-> [d1 |-> {}, d2 |-> {}, d3 |-> {}, d4 |-> {}],node_state |-> [d1 |-> "working", d2 |-> "working", d3 |-> "working", d4 |-> "working"],object_state |-> [o1 |-> "writing"],file_state |-> [o1 |-> "buffered"],object_mode |-> [o1 |-> "DUPLICATE"]]),
    ([job_progress |-> [j1 |-> FALSE],disk_state |-> [d1 |-> "active", d2 |-> "active", d3 |-> "active", d4 |-> "absent"],pending |-> [d1 |-> {<<"o1", 0>>}, d2 |-> {}, d3 |-> {}, d4 |-> {}],job_disk |-> [j1 |-> "none"],job_state |-> [j1 |-> "idle"],plog_ready |-> {"d1", "d2"},job_kind |-> [j1 |-> "none"],last_rpc |-> "WritePlog",stored |-> [d1 |-> {}, d2 |-> {}, d3 |-> {}, d4 |-> {}],node_state |-> [d1 |-> "working", d2 |-> "working", d3 |-> "working", d4 |-> "working"],object_state |-> [o1 |-> "writing"],file_state |-> [o1 |-> "buffered"],object_mode |-> [o1 |-> "DUPLICATE"]]),
    ([job_progress |-> [j1 |-> FALSE],disk_state |-> [d1 |-> "active", d2 |-> "active", d3 |-> "active", d4 |-> "absent"],pending |-> [d1 |-> {<<"o1", 0>>}, d2 |-> {<<"o1", 0>>}, d3 |-> {}, d4 |-> {}],job_disk |-> [j1 |-> "none"],job_state |-> [j1 |-> "idle"],plog_ready |-> {"d1", "d2"},job_kind |-> [j1 |-> "none"],last_rpc |-> "WritePlog",stored |-> [d1 |-> {}, d2 |-> {}, d3 |-> {}, d4 |-> {}],node_state |-> [d1 |-> "working", d2 |-> "working", d3 |-> "working", d4 |-> "working"],object_state |-> [o1 |-> "writing"],file_state |-> [o1 |-> "buffered"],object_mode |-> [o1 |-> "DUPLICATE"]]),
    ([job_progress |-> [j1 |-> FALSE],disk_state |-> [d1 |-> "active", d2 |-> "active", d3 |-> "active", d4 |-> "absent"],pending |-> [d1 |-> {}, d2 |-> {<<"o1", 0>>}, d3 |-> {}, d4 |-> {}],job_disk |-> [j1 |-> "none"],job_state |-> [j1 |-> "idle"],plog_ready |-> {"d1", "d2"},job_kind |-> [j1 |-> "none"],last_rpc |-> "CommitPlog",stored |-> [d1 |-> {<<"o1", 0>>}, d2 |-> {}, d3 |-> {}, d4 |-> {}],node_state |-> [d1 |-> "working", d2 |-> "working", d3 |-> "working", d4 |-> "working"],object_state |-> [o1 |-> "writing"],file_state |-> [o1 |-> "buffered"],object_mode |-> [o1 |-> "DUPLICATE"]]),
    ([job_progress |-> [j1 |-> FALSE],disk_state |-> [d1 |-> "active", d2 |-> "active", d3 |-> "active", d4 |-> "absent"],pending |-> [d1 |-> {}, d2 |-> {}, d3 |-> {}, d4 |-> {}],job_disk |-> [j1 |-> "none"],job_state |-> [j1 |-> "idle"],plog_ready |-> {"d1", "d2"},job_kind |-> [j1 |-> "none"],last_rpc |-> "CommitPlog",stored |-> [d1 |-> {<<"o1", 0>>}, d2 |-> {<<"o1", 0>>}, d3 |-> {}, d4 |-> {}],node_state |-> [d1 |-> "working", d2 |-> "working", d3 |-> "working", d4 |-> "working"],object_state |-> [o1 |-> "writing"],file_state |-> [o1 |-> "buffered"],object_mode |-> [o1 |-> "DUPLICATE"]]),
    ([job_progress |-> [j1 |-> FALSE],disk_state |-> [d1 |-> "active", d2 |-> "active", d3 |-> "active", d4 |-> "absent"],pending |-> [d1 |-> {}, d2 |-> {}, d3 |-> {}, d4 |-> {}],job_disk |-> [j1 |-> "none"],job_state |-> [j1 |-> "idle"],plog_ready |-> {"d1", "d2"},job_kind |-> [j1 |-> "none"],last_rpc |-> "CommitVlog",stored |-> [d1 |-> {<<"o1", 0>>}, d2 |-> {<<"o1", 0>>}, d3 |-> {}, d4 |-> {}],node_state |-> [d1 |-> "working", d2 |-> "working", d3 |-> "working", d4 |-> "working"],object_state |-> [o1 |-> "committed"],file_state |-> [o1 |-> "buffered"],object_mode |-> [o1 |-> "DUPLICATE"]]),
    ([job_progress |-> [j1 |-> FALSE],disk_state |-> [d1 |-> "failed", d2 |-> "active", d3 |-> "active", d4 |-> "absent"],pending |-> [d1 |-> {}, d2 |-> {}, d3 |-> {}, d4 |-> {}],job_disk |-> [j1 |-> "none"],job_state |-> [j1 |-> "idle"],plog_ready |-> {"d1", "d2"},job_kind |-> [j1 |-> "none"],last_rpc |-> "FailDisk",stored |-> [d1 |-> {}, d2 |-> {<<"o1", 0>>}, d3 |-> {}, d4 |-> {}],node_state |-> [d1 |-> "working", d2 |-> "working", d3 |-> "working", d4 |-> "working"],object_state |-> [o1 |-> "committed"],file_state |-> [o1 |-> "buffered"],object_mode |-> [o1 |-> "DUPLICATE"]]),
    ([job_progress |-> [j1 |-> FALSE],disk_state |-> [d1 |-> "failed", d2 |-> "draining", d3 |-> "active", d4 |-> "absent"],pending |-> [d1 |-> {}, d2 |-> {}, d3 |-> {}, d4 |-> {}],job_disk |-> [j1 |-> "d2"],job_state |-> [j1 |-> "running"],plog_ready |-> {"d1", "d2"},job_kind |-> [j1 |-> "remove"],last_rpc |-> "RemoveDisk",stored |-> [d1 |-> {}, d2 |-> {<<"o1", 0>>}, d3 |-> {}, d4 |-> {}],node_state |-> [d1 |-> "working", d2 |-> "working", d3 |-> "working", d4 |-> "working"],object_state |-> [o1 |-> "committed"],file_state |-> [o1 |-> "buffered"],object_mode |-> [o1 |-> "DUPLICATE"]])
    >>
----


=============================================================================

---- CONFIG RoseStorage_TTrace_1782027905 ----
CONSTANTS
    Nodes = { "d1" , "d2" , "d3" , "d4" }
    Disks = { "d1" , "d2" , "d3" , "d4" }
    ActiveDisks = { "d1" , "d2" , "d3" }
    Objects = { "o1" }
    Jobs = { "j1" }
    Modes = { "DUPLICATE" }
    TotalShards = 1
    MinCopies = 2
    MinCommitShards = 1
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
\* Generated on Sun Jun 21 01:45:25 MDT 2026