------------------------- MODULE RoseRepairEpoch -------------------------
EXTENDS Naturals, TLC

\* One serialized repair, at most two fresh destination identities and two
\* lifecycle events. Source reconstruction is abstracted; Write means a fully
\* synced and verified destination. No torn writes or unlocked client lifetime
\* guarantee is assumed. Generations never wrap. Process-crash orphan cleanup is not modeled.
VARIABLES sourceEpoch, destEpoch, sourceUp, diskUp, nodeUp, changes,
          attempt, phase, capturedSource, capturedDest,
          exists, durable, mapped, oldPresent, acceptedSafely, sourceFresh, destFresh
vars == <<sourceEpoch, destEpoch, sourceUp, diskUp, nodeUp, changes,
          attempt, phase, capturedSource, capturedDest,
          exists, durable, mapped, oldPresent, acceptedSafely, sourceFresh, destFresh>>
Phases == {"idle", "capture", "write", "commit", "cleanup", "done"}
Live == diskUp /\ nodeUp

Init ==
    /\ sourceEpoch = 1 /\ destEpoch = 0
    /\ sourceUp = TRUE /\ diskUp = TRUE /\ nodeUp = TRUE /\ changes = 0
    /\ attempt = 0 /\ phase = "idle"
    /\ capturedSource = 0 /\ capturedDest = 0
    /\ exists = FALSE /\ durable = FALSE /\ mapped = FALSE
    /\ oldPresent = TRUE /\ acceptedSafely = TRUE
    /\ sourceFresh = FALSE /\ destFresh = FALSE

Begin ==
    /\ phase = "idle" /\ attempt < 2 /\ ~mapped
    /\ attempt' = attempt + 1 /\ phase' = "capture"
    /\ capturedSource' = sourceEpoch /\ capturedDest' = 0
    /\ exists' = TRUE /\ durable' = FALSE /\ destEpoch' = 1
    /\ sourceFresh' = TRUE /\ destFresh' = FALSE
    /\ UNCHANGED <<sourceEpoch, sourceUp, diskUp, nodeUp, changes,
                    mapped, oldPresent, acceptedSafely>>

Capture ==
    /\ phase = "capture"
    /\ capturedDest' = destEpoch /\ destFresh' = TRUE
    /\ phase' = IF Live THEN "write" ELSE "cleanup"
    /\ UNCHANGED <<sourceEpoch, destEpoch, sourceUp, diskUp, nodeUp, changes,
                    attempt, capturedSource, exists, durable, mapped,
                    oldPresent, acceptedSafely, sourceFresh>>

Write ==
    /\ phase = "write"
    /\ durable' = TRUE /\ phase' = "commit"
    /\ UNCHANGED <<sourceEpoch, destEpoch, sourceUp, diskUp, nodeUp, changes,
                    attempt, capturedSource, capturedDest, exists, mapped,
                    oldPresent, acceptedSafely, sourceFresh, destFresh>>

Commit ==
    /\ phase = "commit"
    /\ IF capturedSource = sourceEpoch
          /\ capturedDest = destEpoch
          /\ Live
       THEN /\ mapped' = TRUE /\ oldPresent' = FALSE
            \* Ghost freshness flags track events independently of generation updates.
            \* Check completion-time validity, not later disk availability.
            /\ acceptedSafely' = (durable /\ exists
                /\ sourceFresh /\ destFresh
                /\ diskUp /\ nodeUp)
       ELSE /\ UNCHANGED <<mapped, oldPresent, acceptedSafely>>
    /\ phase' = "cleanup"
    /\ UNCHANGED <<sourceEpoch, destEpoch, sourceUp, diskUp, nodeUp, changes,
                    attempt, capturedSource, capturedDest, exists, durable, sourceFresh, destFresh>>

Cleanup ==
    /\ phase = "cleanup"
    \* A post-commit error must not delete the now-authoritative destination.
    /\ exists' = IF mapped THEN exists ELSE FALSE
    /\ durable' = IF mapped THEN durable ELSE FALSE
    /\ phase' = IF mapped \/ attempt = 2 THEN "done" ELSE "idle"
    /\ UNCHANGED <<sourceEpoch, destEpoch, sourceUp, diskUp, nodeUp, changes,
                    attempt, capturedSource, capturedDest, mapped,
                    oldPresent, acceptedSafely, sourceFresh, destFresh>>

\* These actions include both outage and return. Identity/ownership changes may
\* invalidate a destination without changing its current availability.
SourceChange ==
    /\ exists /\ ~mapped /\ changes < 2
    /\ sourceEpoch' = sourceEpoch + 1 /\ sourceUp' = ~sourceUp
    /\ sourceFresh' = FALSE
    /\ changes' = changes + 1
    /\ UNCHANGED <<destEpoch, diskUp, nodeUp, attempt, phase,
                    capturedSource, capturedDest, exists, durable, mapped,
                    oldPresent, acceptedSafely, destFresh>>
DestinationChange(kind) ==
    /\ exists /\ ~mapped /\ changes < 2
    /\ destEpoch' = destEpoch + 1 /\ changes' = changes + 1
    /\ destFresh' = FALSE
    /\ diskUp' = IF kind = "disk" THEN ~diskUp ELSE diskUp
    /\ nodeUp' = IF kind = "node" THEN ~nodeUp ELSE nodeUp
    /\ UNCHANGED <<sourceEpoch, sourceUp, attempt, phase,
                    capturedSource, capturedDest, exists, durable, mapped,
                    oldPresent, acceptedSafely, sourceFresh>>

\* Both pre-commit interruption and lost post-commit response reach cleanup.
\* The durable mapping, not a volatile success flag, decides what may be deleted.
Interrupt ==
    /\ phase \in {"capture", "write", "commit", "cleanup"}
    /\ phase' = "cleanup"
    /\ capturedSource' = 0 /\ capturedDest' = 0
    /\ UNCHANGED <<sourceEpoch, destEpoch, sourceUp, diskUp, nodeUp, changes,
                    attempt, exists, durable, mapped, oldPresent, acceptedSafely, sourceFresh, destFresh>>

Next == Begin \/ Capture \/ Write \/ Commit \/ Cleanup \/ Interrupt
    \/ SourceChange \/ (\E k \in {"disk", "node", "identity"} : DestinationChange(k))
TypeOK ==
    /\ sourceEpoch \in 1..3 /\ destEpoch \in 0..3 /\ changes \in 0..2
    /\ attempt \in 0..2 /\ phase \in Phases
    /\ capturedSource \in 0..3 /\ capturedDest \in 0..3
    /\ <<sourceUp, diskUp, nodeUp, exists, durable, mapped, oldPresent,
          acceptedSafely, sourceFresh, destFresh>> \in [1..10 -> BOOLEAN]
NoStalePublication == acceptedSafely
PublishedPreserved == mapped => (exists /\ durable)
RejectedSourcePreserved == ~mapped => oldPresent
NoIdleLeak == phase \in {"idle", "done"} => (exists = mapped)
Spec == Init /\ [][Next]_vars
LiveSpec == Spec /\ WF_vars(Capture) /\ WF_vars(Write)
    /\ WF_vars(Commit) /\ WF_vars(Cleanup)
AttemptTerminates == phase \notin {"idle", "done"} ~> (phase \in {"idle", "done"})
=============================================================================
