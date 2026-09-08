------------------------- MODULE RoseMaintenance -------------------------
EXTENDS Naturals, FiniteSets, TLC

\* Two durable rewrite jobs, three never-reused vlog identities, one published
\* content identity, and two independent in-flight I/O holders. Copy abstracts
\* successful physical sync/verification; prefix crash behavior belongs to the
\* separate RosePrefixRecovery model. There is no disk-loss action here.
Jobs == {1, 2}
Vlogs == {1, 2, 3}
Readers == {1, 2}
Phases == {"idle", "copy", "repoint", "finish", "done"}
VARIABLES mounted, allocated, durable, location, stage, source, dest, holds
vars == <<mounted, allocated, durable, location, stage, source, dest, holds>>
Running(j) == stage[j] \in {"copy", "repoint", "finish"}
DestinationHeld(v) == \E j \in Jobs : Running(j) /\ dest[j] = v
ReaderHeld(v) == \E h \in Readers : holds[h] = v

Init ==
    /\ mounted = {1} /\ allocated = {1} /\ durable = {1} /\ location = 1
    /\ stage = [j \in Jobs |-> "idle"]
    /\ source = [j \in Jobs |-> 0] /\ dest = [j \in Jobs |-> 0]
    /\ holds = [h \in Readers |-> 0]

\* The jobs can represent different rewrite kinds sharing a source. A new
\* destination is provisioned and assigned atomically at this layer; separate
\* runtime crash tests cover partial provisioning and ambiguous assignment.
Begin(j, s, d) ==
    /\ stage[j] = "idle" /\ s \in mounted /\ ~DestinationHeld(s)
    /\ d \in Vlogs \ allocated
    /\ mounted' = mounted \cup {d} /\ allocated' = allocated \cup {d}
    /\ source' = [source EXCEPT ![j] = s] /\ dest' = [dest EXCEPT ![j] = d]
    /\ stage' = [stage EXCEPT ![j] = "copy"]
    /\ UNCHANGED <<durable, location, holds>>

Copy(j) ==
    /\ stage[j] = "copy"
    /\ IF location = source[j]
          THEN /\ source[j] \in mounted /\ source[j] \in durable
               /\ durable' = durable \cup {dest[j]}
          ELSE /\ UNCHANGED durable
    /\ stage' = [stage EXCEPT ![j] = "repoint"]
    /\ UNCHANGED <<mounted, allocated, location, source, dest, holds>>

\* Re-resolve the canonical location: another rewrite may already have moved
\* the content away. A no-longer-live source needs no further copy or repoint.
Repoint(j) ==
    /\ stage[j] = "repoint"
    /\ IF location = source[j]
          THEN /\ dest[j] \in durable /\ dest[j] \in mounted
               /\ location' = dest[j]
          ELSE /\ UNCHANGED location
    /\ stage' = [stage EXCEPT ![j] = "finish"]
    /\ UNCHANGED <<mounted, allocated, durable, source, dest, holds>>

Finish(j) ==
    /\ stage[j] = "finish"
    \* Job 1 is compaction (source retirement precedes completion); job 2
    \* is promotion (completion may precede retirement of an empty source).
    /\ (j = 2 \/ source[j] \notin mounted)
    /\ stage' = [stage EXCEPT ![j] = "done"]
    /\ UNCHANGED <<mounted, allocated, durable, location, source, dest, holds>>

Retire(v) ==
    /\ v \in mounted /\ location # v
    /\ ~DestinationHeld(v)
    /\ ~ReaderHeld(v)
    /\ mounted' = mounted \ {v} /\ durable' = durable \ {v}
    /\ UNCHANGED <<allocated, location, stage, source, dest, holds>>

ReadBegin(h) ==
    /\ holds[h] = 0
    /\ holds' = [holds EXCEPT ![h] = location]
    /\ UNCHANGED <<mounted, allocated, durable, location, stage, source, dest>>

ReadEnd(h) ==
    /\ holds[h] # 0
    /\ holds' = [holds EXCEPT ![h] = 0]
    /\ UNCHANGED <<mounted, allocated, durable, location, stage, source, dest>>

Crash ==
    /\ holds' = [h \in Readers |-> 0]
    /\ UNCHANGED <<mounted, allocated, durable, location, stage, source, dest>>

Next ==
    \/ \E j \in Jobs, s \in Vlogs, d \in Vlogs : Begin(j, s, d)
    \/ \E j \in Jobs : Copy(j) \/ Repoint(j) \/ Finish(j)
    \/ \E v \in Vlogs : Retire(v)
    \/ \E h \in Readers : ReadBegin(h) \/ ReadEnd(h)
    \/ Crash

TypeOK ==
    /\ mounted \subseteq allocated /\ allocated \subseteq Vlogs
    /\ durable \subseteq Vlogs /\ location \in Vlogs
    /\ stage \in [Jobs -> Phases]
    /\ source \in [Jobs -> (Vlogs \cup {0})]
    /\ dest \in [Jobs -> (Vlogs \cup {0})]
    /\ holds \in [Readers -> (Vlogs \cup {0})]
PublishedReadable == location \in mounted /\ location \in durable
ReaderReadable == \A h \in Readers : holds[h] = 0 \/ (holds[h] \in mounted /\ holds[h] \in durable)
DestinationPreserved == \A j \in Jobs : Running(j) => dest[j] \in mounted
DistinctDestinations == \A i, j \in Jobs : (i # j /\ stage[i] # "idle" /\ stage[j] # "idle") => dest[i] # dest[j]
NoSelfRewrite == \A j \in Jobs : stage[j] # "idle" => source[j] # dest[j]

Spec == Init /\ [][Next]_vars
LiveSpec == Spec
    /\ \A j \in Jobs : WF_vars(Copy(j)) /\ WF_vars(Repoint(j)) /\ WF_vars(Finish(j))
    /\ \A h \in Readers : WF_vars(ReadEnd(h))
    /\ \A v \in Vlogs : WF_vars(Retire(v))
JobsFinish == \A j \in Jobs : Running(j) ~> (stage[j] = "done")
UnusedEventuallyRetires == \A v \in Vlogs :
    (v \in mounted /\ location # v /\ ~DestinationHeld(v)) ~> (v \notin mounted)
=============================================================================
