---------------------- MODULE RoseRelocationOutcome ----------------------
EXTENDS Naturals, FiniteSets, TLC

\* One plog ID, two physical disks. Copy means verified, synced bytes of the
\* acknowledged prefix. Catalog placement and mounted clients are independent.
\* One relocation attempt, two lifecycle events, and two process crashes.
VARIABLES catalog, files, mounted, phase, epoch, after, rollbackFresh,
          changes, crashes, sourceFresh, destFresh, resolutionSafe
vars == <<catalog, files, mounted, phase, epoch, after, rollbackFresh,
          changes, crashes, sourceFresh, destFresh, resolutionSafe>>
Init ==
    /\ catalog = 0 /\ files = {0} /\ mounted = 0 /\ phase = "copy"
    /\ epoch = 0 /\ after = 0 /\ rollbackFresh = TRUE
    /\ changes = 0 /\ crashes = 0
    /\ sourceFresh = TRUE /\ destFresh = FALSE /\ resolutionSafe = TRUE
Copy ==
    /\ phase = "copy"
    /\ files' = files \cup {1} /\ phase' = "commit"
    /\ UNCHANGED <<catalog, mounted, epoch, after, rollbackFresh, changes, crashes, sourceFresh, destFresh, resolutionSafe>>
\* A rejected commit and an applied-but-error commit have the same caller
\* outcome. The attempted post-update token is available in either case.
Commit ==
    /\ phase = "commit"
    /\ \E applied, uncertain \in BOOLEAN :
        /\ destFresh' = (applied /\ epoch = 0)
        /\ after' = IF epoch = 0 THEN 1 ELSE 0
        /\ catalog' = IF applied /\ epoch = 0 THEN 1 ELSE catalog
        /\ epoch' = IF applied /\ epoch = 0 THEN epoch + 1 ELSE epoch
        /\ phase' = IF uncertain /\ epoch = 0 THEN "resolve"
                     ELSE IF applied /\ epoch = 0 THEN "remount" ELSE "cleanup"
    /\ UNCHANGED <<files, mounted, rollbackFresh, changes, crashes, sourceFresh, resolutionSafe>>
Resolve ==
    /\ phase = "resolve"
    /\ LET sourceOK == catalog = 0 /\ epoch = 0
           destOK == catalog = 1 /\ epoch = after
       IN /\ phase' = IF sourceOK THEN "cleanup"
                      ELSE IF destOK THEN "remount" ELSE "quarantine"
          /\ mounted' = IF sourceOK \/ destOK THEN mounted ELSE 2
          /\ resolutionSafe' = (resolutionSafe /\
                 (phase' = "quarantine" \/ (catalog = 0 /\ sourceFresh) \/ (catalog = 1 /\ destFresh)))
    /\ UNCHANGED <<catalog, files, epoch, after, rollbackFresh, changes, crashes, sourceFresh, destFresh>>
ResolveFailure ==
    /\ phase = "resolve"
    /\ phase' = "quarantine" /\ mounted' = 2
    /\ UNCHANGED <<catalog, files, epoch, after, rollbackFresh, changes, crashes, sourceFresh, destFresh, resolutionSafe>>
Remount ==
    /\ phase = "remount"
    /\ \E success \in BOOLEAN :
        /\ mounted' = IF success THEN catalog ELSE mounted
        /\ phase' = IF success THEN "cleanup" ELSE "rollback"
    /\ UNCHANGED <<catalog, files, epoch, after, rollbackFresh, changes, crashes, sourceFresh, destFresh, resolutionSafe>>
Rollback ==
    /\ phase = "rollback"
    /\ \E applied, uncertain \in BOOLEAN :
        /\ LET succeeds == applied /\ rollbackFresh /\ epoch = after
           IN /\ catalog' = IF succeeds THEN 0 ELSE catalog
              /\ epoch' = IF succeeds THEN epoch + 1 ELSE epoch
              /\ phase' = IF succeeds /\ ~uncertain THEN "cleanup" ELSE "quarantine"
              /\ mounted' = IF succeeds /\ ~uncertain THEN 0 ELSE 2
    /\ UNCHANGED <<files, after, rollbackFresh, changes, crashes, sourceFresh, destFresh, resolutionSafe>>
Cleanup ==
    /\ phase = "cleanup"
    /\ files' = files \ {1 - catalog} /\ phase' = "done"
    /\ UNCHANGED <<catalog, mounted, epoch, after, rollbackFresh, changes, crashes, sourceFresh, destFresh, resolutionSafe>>
\* A source-disk lifecycle change after repoint invalidates the rollback token
\* independently of the moved plog's generation. Events preserve physical bytes.
Change(disk) ==
    /\ changes < 2
    /\ changes' = changes + 1
    /\ sourceFresh' = (sourceFresh /\ ~(disk = 0 /\ catalog = 0))
    /\ destFresh' = (destFresh /\ ~(disk = 1 /\ catalog = 1))
    /\ epoch' = IF disk = catalog THEN epoch + 1 ELSE epoch
    /\ rollbackFresh' = (rollbackFresh /\ disk # 0)
    /\ UNCHANGED <<catalog, files, mounted, phase, after, crashes, resolutionSafe>>
Crash ==
    /\ crashes < 2
    /\ crashes' = crashes + 1 /\ phase' = "recover" /\ mounted' = 2
    /\ UNCHANGED <<catalog, files, epoch, after, rollbackFresh, changes, sourceFresh, destFresh, resolutionSafe>>
Recover ==
    /\ phase \in {"recover", "quarantine"}
    /\ catalog \in files
    /\ mounted' = catalog /\ phase' = "done"
    /\ UNCHANGED <<catalog, files, epoch, after, rollbackFresh, changes, crashes, sourceFresh, destFresh, resolutionSafe>>
\* No sweeper can interleave with the invocation's topology ownership. After
\* quarantine/restart it must distinguish disk locations of the same plog ID.
Sweep(disk) ==
    /\ phase \in {"done", "recover", "quarantine"}
    /\ disk \in files /\ disk # catalog
    /\ files' = files \ {disk}
    /\ UNCHANGED <<catalog, mounted, phase, epoch, after, rollbackFresh, changes, crashes, sourceFresh, destFresh, resolutionSafe>>
Next == Copy \/ Commit \/ Resolve \/ ResolveFailure \/ Remount \/ Rollback
    \/ Cleanup \/ Crash \/ Recover
    \/ (\E disk \in {0,1} : Change(disk) \/ Sweep(disk))
TypeOK ==
    /\ catalog \in {0,1} /\ files \subseteq {0,1}
    /\ sourceFresh \in BOOLEAN /\ destFresh \in BOOLEAN /\ resolutionSafe \in BOOLEAN
    /\ mounted \in {0,1,2} /\ rollbackFresh \in BOOLEAN
    /\ phase \in {"copy","commit","resolve","remount","rollback",
                   "cleanup","done","quarantine","recover"}
    /\ epoch \in 0..4 /\ after \in 0..3 /\ changes \in 0..2 /\ crashes \in 0..2
ResolutionFresh == resolutionSafe
PublishedPreserved == catalog \in files
AccessCoherent == phase \in {"done","quarantine"} => (mounted = 2 \/ mounted = catalog)
QuarantineFenced == phase = "quarantine" => mounted = 2
ServedFilePresent == phase = "done" => mounted \in files
Spec == Init /\ [][Next]_vars
LiveSpec == Spec /\ WF_vars(Copy) /\ WF_vars(Commit) /\ WF_vars(Resolve)
    /\ WF_vars(Remount) /\ WF_vars(Rollback) /\ WF_vars(Cleanup) /\ WF_vars(Recover)
    /\ \A disk \in {0,1} : WF_vars(Sweep(disk))
EventuallyRecovered == <>[](phase = "done" /\ mounted = catalog)
EventuallyReclaimed == <>[](files = {catalog})
=============================================================================
