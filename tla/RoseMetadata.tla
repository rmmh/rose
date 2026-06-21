------------------------- MODULE RoseMetadata -------------------------
EXTENDS FiniteSets, Integers

\* Metadata-plane model.  Chunks are immutable once committed.  A file head is
\* replaced atomically on Close, while snapshots retain immutable copies of all
\* file heads.  The explicit refcount is checked against reachability.

CONSTANTS Files, Handles, Chunks, Snapshots

VARIABLES handle_state, handle_file, handle_snapshot, staged_chunks,
          chunk_state, file_chunks, snapshot_state, snapshot_files,
          chunk_refcount

vars == <<handle_state, handle_file, handle_snapshot, staged_chunks,
          chunk_state, file_chunks, snapshot_state, snapshot_files,
          chunk_refcount>>

Init ==
    /\ handle_state = [h \in Handles |-> "closed"]
    /\ handle_file = [h \in Handles |-> "none"]
    /\ handle_snapshot = [h \in Handles |-> "live"]
    /\ staged_chunks = [h \in Handles |-> {}]
    /\ chunk_state = [c \in Chunks |-> "new"]
    /\ file_chunks = [f \in Files |-> {}]
    /\ snapshot_state = [s \in Snapshots |-> "empty"]
    /\ snapshot_files = [s \in Snapshots |-> [f \in Files |-> {}]]
    /\ chunk_refcount = [c \in Chunks |-> 0]

VisibleChunks(h) ==
    IF handle_snapshot[h] = "live"
    THEN file_chunks[handle_file[h]]
    ELSE snapshot_files[handle_snapshot[h]][handle_file[h]]

ReferenceCount(c, fc, ss, sf) ==
    Cardinality({f \in Files : c \in fc[f]}) +
    Cardinality({s \in Snapshots : ss[s] = "active" /\
                                  \E f \in Files : c \in sf[s][f]})

Refcounts(fc, ss, sf) == [c \in Chunks |-> ReferenceCount(c, fc, ss, sf)]

AllCommitted(cs) == \A c \in cs : chunk_state[c] = "committed"

Open(h, f) ==
    /\ handle_state[h] = "closed"
    /\ handle_state' = [handle_state EXCEPT ![h] = "open"]
    /\ handle_file' = [handle_file EXCEPT ![h] = f]
    /\ handle_snapshot' = [handle_snapshot EXCEPT ![h] = "live"]
    /\ staged_chunks' = [staged_chunks EXCEPT ![h] = {}]
    /\ UNCHANGED <<chunk_state, file_chunks, snapshot_state, snapshot_files, chunk_refcount>>

OpenSnapshot(h, s, f) ==
    /\ handle_state[h] = "closed"
    /\ snapshot_state[s] = "active"
    /\ handle_state' = [handle_state EXCEPT ![h] = "open"]
    /\ handle_file' = [handle_file EXCEPT ![h] = f]
    /\ handle_snapshot' = [handle_snapshot EXCEPT ![h] = s]
    /\ staged_chunks' = [staged_chunks EXCEPT ![h] = {}]
    /\ UNCHANGED <<chunk_state, file_chunks, snapshot_state, snapshot_files, chunk_refcount>>

StartChunk(c) ==
    /\ chunk_state[c] = "new"
    /\ chunk_state' = [chunk_state EXCEPT ![c] = "writing"]
    /\ UNCHANGED <<handle_state, handle_file, handle_snapshot, staged_chunks,
                  file_chunks, snapshot_state, snapshot_files, chunk_refcount>>

CommitChunk(c) ==
    /\ chunk_state[c] = "writing"
    /\ chunk_state' = [chunk_state EXCEPT ![c] = "committed"]
    /\ UNCHANGED <<handle_state, handle_file, handle_snapshot, staged_chunks,
                  file_chunks, snapshot_state, snapshot_files, chunk_refcount>>

Write(h, c) ==
    /\ handle_state[h] = "open"
    /\ handle_snapshot[h] = "live"
    /\ chunk_state[c] \in {"writing", "committed"}
    /\ staged_chunks' = [staged_chunks EXCEPT ![h] = @ \cup {c}]
    /\ UNCHANGED <<handle_state, handle_file, handle_snapshot, chunk_state,
                  file_chunks, snapshot_state, snapshot_files, chunk_refcount>>

\* Close is the metadata transaction: it publishes a complete immutable file
\* head and adjusts every refcount in the same step.
Close(h) ==
    /\ handle_state[h] = "open"
    /\ handle_snapshot[h] = "live"
    /\ AllCommitted(staged_chunks[h])
    /\ LET fc == [file_chunks EXCEPT ![handle_file[h]] = staged_chunks[h]] IN
       /\ file_chunks' = fc
       /\ chunk_refcount' = Refcounts(fc, snapshot_state, snapshot_files)
    /\ handle_state' = [handle_state EXCEPT ![h] = "closed"]
    /\ staged_chunks' = [staged_chunks EXCEPT ![h] = {}]
    /\ UNCHANGED <<handle_file, handle_snapshot, chunk_state, snapshot_state, snapshot_files>>

CloseSnapshot(h) ==
    /\ handle_state[h] = "open"
    /\ handle_snapshot[h] # "live"
    /\ handle_state' = [handle_state EXCEPT ![h] = "closed"]
    /\ staged_chunks' = [staged_chunks EXCEPT ![h] = {}]
    /\ UNCHANGED <<handle_file, handle_snapshot, chunk_state, file_chunks,
                  snapshot_state, snapshot_files, chunk_refcount>>

Read(h) ==
    /\ handle_state[h] = "open"
    /\ AllCommitted(VisibleChunks(h))
    /\ UNCHANGED vars

Getattr(h) ==
    /\ handle_state[h] = "open"
    /\ UNCHANGED vars

CreateSnapshot(s) ==
    /\ snapshot_state[s] = "empty"
    /\ snapshot_state' = [snapshot_state EXCEPT ![s] = "active"]
    /\ snapshot_files' = [snapshot_files EXCEPT ![s] = file_chunks]
    /\ chunk_refcount' = Refcounts(file_chunks,
                                   [snapshot_state EXCEPT ![s] = "active"],
                                   [snapshot_files EXCEPT ![s] = file_chunks])
    /\ UNCHANGED <<handle_state, handle_file, handle_snapshot, staged_chunks,
                  chunk_state, file_chunks>>

DeleteSnapshot(s) ==
    /\ snapshot_state[s] = "active"
    /\ snapshot_state' = [snapshot_state EXCEPT ![s] = "deleted"]
    /\ chunk_refcount' = Refcounts(file_chunks,
                                   [snapshot_state EXCEPT ![s] = "deleted"],
                                   snapshot_files)
    /\ UNCHANGED <<handle_state, handle_file, handle_snapshot, staged_chunks,
                  chunk_state, file_chunks, snapshot_files>>

Unlink(f) ==
    /\ LET fc == [file_chunks EXCEPT ![f] = {}] IN
       /\ file_chunks' = fc
       /\ chunk_refcount' = Refcounts(fc, snapshot_state, snapshot_files)
    /\ UNCHANGED <<handle_state, handle_file, handle_snapshot, staged_chunks,
                  chunk_state, snapshot_state, snapshot_files>>

Rename(from, to) ==
    /\ from # to
    /\ LET fc == [f \in Files |-> IF f = to THEN file_chunks[from]
                               ELSE IF f = from THEN {} ELSE file_chunks[f]] IN
       /\ file_chunks' = fc
       /\ chunk_refcount' = Refcounts(fc, snapshot_state, snapshot_files)
    /\ UNCHANGED <<handle_state, handle_file, handle_snapshot, staged_chunks,
                  chunk_state, snapshot_state, snapshot_files>>

GarbageCollect(c) ==
    /\ chunk_state[c] = "committed"
    /\ chunk_refcount[c] = 0
    /\ chunk_state' = [chunk_state EXCEPT ![c] = "garbage-collected"]
    /\ UNCHANGED <<handle_state, handle_file, handle_snapshot, staged_chunks,
                  file_chunks, snapshot_state, snapshot_files, chunk_refcount>>

Next ==
    \/ \E h \in Handles, f \in Files : Open(h, f)
    \/ \E h \in Handles, s \in Snapshots, f \in Files : OpenSnapshot(h, s, f)
    \/ \E c \in Chunks : StartChunk(c) \/ CommitChunk(c) \/ GarbageCollect(c)
    \/ \E h \in Handles, c \in Chunks : Write(h, c)
    \/ \E h \in Handles : Close(h) \/ CloseSnapshot(h) \/ Read(h) \/ Getattr(h)
    \/ \E s \in Snapshots : CreateSnapshot(s) \/ DeleteSnapshot(s)
    \/ \E f \in Files : Unlink(f)
    \/ \E from \in Files, to \in Files : Rename(from, to)

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ \A h \in Handles : handle_state[h] \in {"closed", "open"}
    /\ \A c \in Chunks : chunk_state[c] \in {"new", "writing", "committed", "garbage-collected"}
    /\ \A s \in Snapshots : snapshot_state[s] \in {"empty", "active", "deleted"}
    /\ \A f \in Files : file_chunks[f] \subseteq Chunks
    /\ \A s \in Snapshots : \A f \in Files : snapshot_files[s][f] \subseteq Chunks

RefcountsCorrect == chunk_refcount = Refcounts(file_chunks, snapshot_state, snapshot_files)
ReachableCommitted == \A c \in Chunks : chunk_refcount[c] > 0 => chunk_state[c] = "committed"
SnapshotsImmutable == \A s \in Snapshots : snapshot_state[s] = "active" =>
    \A f \in Files : snapshot_files[s][f] \subseteq Chunks

=============================================================================
