------------------------ MODULE RoseRepairRecovery ------------------------
EXTENDS Naturals, FiniteSets, TLC

\* One source (0), one legitimate raw plog (1), and one fresh repair (2).
\* Generation admission belongs to RoseRepairEpoch. Copy abstracts verified,
\* synced bytes. Catalog commits are atomic; this is not a SQLite/VFS model.
VARIABLES catalog, owned, files, mapped, phase, reachable, faults, crashes
vars == <<catalog, owned, files, mapped, phase, reachable, faults, crashes>>
Init ==
    /\ catalog = {0,1} /\ owned = {} /\ files = {0,1} /\ mapped = 0
    /\ phase = "allocate" /\ reachable = TRUE /\ faults = 0 /\ crashes = 0
Allocate ==
    /\ phase = "allocate"
    /\ catalog' = catalog \cup {2} /\ owned' = owned \cup {2}
    /\ phase' = "copy"
    /\ UNCHANGED <<files, mapped, reachable, faults, crashes>>
Copy ==
    /\ phase = "copy" /\ reachable
    /\ files' = files \cup {2} /\ phase' = "publish"
    /\ UNCHANGED <<catalog, owned, mapped, reachable, faults, crashes>>
Publish ==
    /\ phase = "publish" /\ reachable /\ 2 \in files
    /\ catalog' = catalog \ {0} /\ mapped' = 2 /\ phase' = "cleanup"
    /\ UNCHANGED <<owned, files, reachable, faults, crashes>>
Reject ==
    /\ phase \in {"copy", "publish"}
    /\ phase' = "cleanup"
    /\ UNCHANGED <<catalog, owned, files, mapped, reachable, faults, crashes>>
Retirable(p) == p \in owned /\ p # mapped
Cleanup ==
    /\ phase = "cleanup"
    /\ catalog' = {p \in catalog : ~Retirable(p)}
    /\ phase' = "done"
    /\ UNCHANGED <<owned, files, mapped, reachable, faults, crashes>>
Crash ==
    /\ phase # "allocate" /\ crashes < 2
    /\ crashes' = crashes + 1 /\ phase' = "recover"
    /\ UNCHANGED <<catalog, owned, files, mapped, reachable, faults>>
Recover ==
    /\ phase = "recover"
    /\ catalog' = {p \in catalog : ~Retirable(p)}
    /\ phase' = "done"
    /\ UNCHANGED <<owned, files, mapped, reachable, faults, crashes>>
\* Physical deletion follows the catalog decision and can be interrupted by a
\* crash or deferred while media is inaccessible. Sweep consults current rows.
Sweep(p) ==
    /\ reachable /\ p \in files /\ p \notin catalog
    /\ files' = files \ {p}
    /\ UNCHANGED <<catalog, owned, mapped, phase, reachable, faults, crashes>>
Fail ==
    /\ reachable /\ faults < 2
    /\ reachable' = FALSE /\ faults' = faults + 1
    /\ UNCHANGED <<catalog, owned, files, mapped, phase, crashes>>
Return ==
    /\ ~reachable /\ reachable' = TRUE
    /\ UNCHANGED <<catalog, owned, files, mapped, phase, faults, crashes>>
Next == Allocate \/ Copy \/ Publish \/ Reject \/ Cleanup \/ Crash \/ Recover
    \/ Fail \/ Return \/ (\E p \in 0..2 : Sweep(p))
TypeOK ==
    /\ catalog \subseteq 0..2 /\ owned \subseteq {2} /\ files \subseteq 0..2
    /\ mapped \in {0,2} /\ reachable \in BOOLEAN
    /\ phase \in {"allocate","copy","publish","cleanup","done","recover"}
    /\ faults \in 0..2 /\ crashes \in 0..2
PublishedPreserved == mapped \in catalog /\ mapped \in files
RawPreserved == 1 \in catalog /\ 1 \in files
NoAbandonedCatalog == phase = "done" => (2 \in catalog => mapped = 2)
Spec == Init /\ [][Next]_vars
LiveSpec == Spec /\ WF_vars(Allocate) /\ WF_vars(Copy) /\ WF_vars(Publish)
    /\ WF_vars(Cleanup) /\ WF_vars(Recover) /\ WF_vars(Return)
    /\ \A p \in 0..2 : WF_vars(Sweep(p))
EventuallyFinished == <>[](phase = "done")
EventuallyReclaimed == <>[](files = catalog)
=============================================================================
