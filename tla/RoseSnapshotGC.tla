------------------------ MODULE RoseSnapshotGC ------------------------
EXTENDS Integers, FiniteSets

\* A bounded persistent-metadata model.  A namespace/file tree is represented
\* by a root with two immutable extent leaves.  Publishing a mutation copies
\* only the changed leaf and root, sharing the other leaf with older roots.
\* A snapshot is therefore only a timestamped root reference.

CONSTANTS RootNodes, LeafNodes, Chunks, Snapshots,
          ContinuousWindow, DailyWindow, WeeklyWindow,
          DailyPeriod, WeeklyPeriod, MaxTime

Nodes == RootNodes \cup LeafNodes

VARIABLES now, live_root, snapshot_state, snapshot_root, snapshot_time,
          node_state, left_child, right_child, leaf_chunks,
          node_refcount, chunk_state, chunk_refcount, pinned_chunks

vars == <<now, live_root, snapshot_state, snapshot_root, snapshot_time,
          node_state, left_child, right_child, leaf_chunks,
          node_refcount, chunk_state, chunk_refcount, pinned_chunks>>

InitRoot == CHOOSE r \in RootNodes : TRUE
InitLeft == CHOOSE l \in LeafNodes : TRUE
InitRight == CHOOSE l \in LeafNodes \ {InitLeft} : TRUE
InitChunkLeft == CHOOSE c \in Chunks : TRUE
InitChunkRight == CHOOSE c \in Chunks \ {InitChunkLeft} : TRUE

RootPins(r, lr, ss, sr) ==
    (IF r = lr THEN 1 ELSE 0) + Cardinality({s \in Snapshots : ss[s] = "active" /\ sr[s] = r})

ParentPins(n, ns, lc, rc) ==
    Cardinality({r \in RootNodes : ns[r] = "active" /\ (lc[r] = n \/ rc[r] = n)})

NodeRefs(n, lr, ss, sr, ns, lc, rc) ==
    IF n \in RootNodes THEN RootPins(n, lr, ss, sr) ELSE ParentPins(n, ns, lc, rc)

NodeRefcounts(lr, ss, sr, ns, lc, rc) ==
    [n \in Nodes |-> NodeRefs(n, lr, ss, sr, ns, lc, rc)]

ChunkRefcounts(ns, chunks) ==
    [c \in Chunks |-> Cardinality({l \in LeafNodes : ns[l] = "active" /\ c \in chunks[l]})]

Init ==
    /\ now = 0
    /\ live_root = InitRoot
    /\ snapshot_state = [s \in Snapshots |-> "empty"]
    /\ snapshot_root = [s \in Snapshots |-> "none"]
    /\ snapshot_time = [s \in Snapshots |-> 0]
    /\ node_state = [n \in Nodes |-> IF n \in {InitRoot, InitLeft, InitRight} THEN "active" ELSE "free"]
    /\ left_child = [r \in RootNodes |-> IF r = InitRoot THEN InitLeft ELSE "none"]
    /\ right_child = [r \in RootNodes |-> IF r = InitRoot THEN InitRight ELSE "none"]
    /\ leaf_chunks = [l \in LeafNodes |-> IF l = InitLeft THEN {InitChunkLeft}
                                           ELSE IF l = InitRight THEN {InitChunkRight} ELSE {}]
    /\ chunk_state = [c \in Chunks |-> IF c \in {InitChunkLeft, InitChunkRight} THEN "committed" ELSE "new"]
    /\ pinned_chunks = {}
    /\ node_refcount = NodeRefcounts(live_root, snapshot_state, snapshot_root,
                                     node_state, left_child, right_child)
    /\ chunk_refcount = ChunkRefcounts(node_state, leaf_chunks)

Age(s) == now - snapshot_time[s]
DailyBucket(s) == snapshot_time[s] \div DailyPeriod
WeeklyBucket(s) == snapshot_time[s] \div WeeklyPeriod

NewestInDailyBucket(s) ==
    \A t \in Snapshots : snapshot_state[t] = "active" /\ DailyBucket(t) = DailyBucket(s) => snapshot_time[t] <= snapshot_time[s]
NewestInWeeklyBucket(s) ==
    \A t \in Snapshots : snapshot_state[t] = "active" /\ WeeklyBucket(t) = WeeklyBucket(s) => snapshot_time[t] <= snapshot_time[s]

Retained(s) ==
    /\ snapshot_state[s] = "active"
    /\ \/ Age(s) <= ContinuousWindow
       \/ /\ Age(s) <= DailyWindow
          /\ NewestInDailyBucket(s)
       \/ /\ Age(s) <= WeeklyWindow
          /\ NewestInWeeklyBucket(s)

VisibleChunks(r) == leaf_chunks[left_child[r]] \cup leaf_chunks[right_child[r]]
SnapshotReadable(s) == \A c \in VisibleChunks(snapshot_root[s]) : chunk_state[c] = "committed"

CommitChunk(c) ==
    /\ chunk_state[c] = "new"
    /\ chunk_state' = [chunk_state EXCEPT ![c] = "committed"]
    /\ UNCHANGED <<now, live_root, snapshot_state, snapshot_root, snapshot_time,
                  node_state, left_child, right_child, leaf_chunks,
                  node_refcount, chunk_refcount, pinned_chunks>>

\* Publishing one metadata mutation creates exactly two new nodes: a root and
\* one leaf.  The unmodified branch remains shared by every older root.
Publish(s, newRoot, newLeaf, newChunk, side) ==
    /\ s \in Snapshots
    /\ snapshot_state[s] = "empty"
    /\ node_state[newRoot] = "free"
    /\ node_state[newLeaf] = "free"
    /\ newRoot \in RootNodes /\ newLeaf \in LeafNodes
    /\ chunk_state[newChunk] = "committed"
    /\ side \in {"left", "right"}
    /\ now < MaxTime
    /\ LET ns == [node_state EXCEPT ![newRoot] = "active", ![newLeaf] = "active"] IN
       /\ node_state' = ns
       /\ left_child' = [left_child EXCEPT ![newRoot] = IF side = "left" THEN newLeaf ELSE left_child[live_root]]
       /\ right_child' = [right_child EXCEPT ![newRoot] = IF side = "right" THEN newLeaf ELSE right_child[live_root]]
       /\ leaf_chunks' = [leaf_chunks EXCEPT ![newLeaf] = {newChunk}]
       /\ live_root' = newRoot
       /\ snapshot_state' = [snapshot_state EXCEPT ![s] = "active"]
       /\ snapshot_root' = [snapshot_root EXCEPT ![s] = newRoot]
       /\ snapshot_time' = [snapshot_time EXCEPT ![s] = now + 1]
       /\ now' = now + 1
       /\ node_refcount' = NodeRefcounts(newRoot,
                                          [snapshot_state EXCEPT ![s] = "active"],
                                          [snapshot_root EXCEPT ![s] = newRoot],
                                          ns,
                                          [left_child EXCEPT ![newRoot] = IF side = "left" THEN newLeaf ELSE left_child[live_root]],
                                          [right_child EXCEPT ![newRoot] = IF side = "right" THEN newLeaf ELSE right_child[live_root]])
       /\ chunk_refcount' = ChunkRefcounts(ns, [leaf_chunks EXCEPT ![newLeaf] = {newChunk}])
    /\ UNCHANGED <<chunk_state, pinned_chunks>>

Tick ==
    /\ now < MaxTime
    /\ now' = now + 1
    /\ UNCHANGED <<live_root, snapshot_state, snapshot_root, snapshot_time,
                  node_state, left_child, right_child, leaf_chunks,
                  node_refcount, chunk_state, chunk_refcount, pinned_chunks>>

ExpireSnapshot(s) ==
    /\ snapshot_state[s] = "active"
    /\ ~Retained(s)
    /\ snapshot_state' = [snapshot_state EXCEPT ![s] = "expired"]
    /\ node_refcount' = NodeRefcounts(live_root,
                                       [snapshot_state EXCEPT ![s] = "expired"], snapshot_root,
                                       node_state, left_child, right_child)
    /\ UNCHANGED <<now, live_root, snapshot_root, snapshot_time, node_state,
                  left_child, right_child, leaf_chunks, chunk_state,
                  chunk_refcount, pinned_chunks>>

PinChunk(c) ==
    /\ chunk_state[c] = "committed"
    /\ chunk_refcount[c] > 0
    /\ pinned_chunks' = pinned_chunks \cup {c}
    /\ UNCHANGED <<now, live_root, snapshot_state, snapshot_root, snapshot_time,
                  node_state, left_child, right_child, leaf_chunks, node_refcount,
                  chunk_state, chunk_refcount>>

UnpinChunk(c) ==
    /\ c \in pinned_chunks
    /\ pinned_chunks' = pinned_chunks \ {c}
    /\ UNCHANGED <<now, live_root, snapshot_state, snapshot_root, snapshot_time,
                  node_state, left_child, right_child, leaf_chunks, node_refcount,
                  chunk_state, chunk_refcount>>

GCNode(n) ==
    /\ node_state[n] = "active"
    /\ node_refcount[n] = 0
    /\ node_state' = [node_state EXCEPT ![n] = "free"]
    /\ left_child' = IF n \in RootNodes THEN [left_child EXCEPT ![n] = "none"] ELSE left_child
    /\ right_child' = IF n \in RootNodes THEN [right_child EXCEPT ![n] = "none"] ELSE right_child
    /\ leaf_chunks' = IF n \in LeafNodes THEN [leaf_chunks EXCEPT ![n] = {}] ELSE leaf_chunks
    /\ node_refcount' = NodeRefcounts(live_root, snapshot_state, snapshot_root,
                                       [node_state EXCEPT ![n] = "free"],
                                       IF n \in RootNodes THEN [left_child EXCEPT ![n] = "none"] ELSE left_child,
                                       IF n \in RootNodes THEN [right_child EXCEPT ![n] = "none"] ELSE right_child)
    /\ chunk_refcount' = ChunkRefcounts([node_state EXCEPT ![n] = "free"],
                                         IF n \in LeafNodes THEN [leaf_chunks EXCEPT ![n] = {}] ELSE leaf_chunks)
    /\ UNCHANGED <<now, live_root, snapshot_state, snapshot_root, snapshot_time,
                  chunk_state, pinned_chunks>>

GCChunk(c) ==
    /\ chunk_state[c] = "committed"
    /\ chunk_refcount[c] = 0
    /\ c \notin pinned_chunks
    /\ chunk_state' = [chunk_state EXCEPT ![c] = "collected"]
    /\ UNCHANGED <<now, live_root, snapshot_state, snapshot_root, snapshot_time,
                  node_state, left_child, right_child, leaf_chunks, node_refcount,
                  chunk_refcount, pinned_chunks>>

Next ==
    \/ \E c \in Chunks : CommitChunk(c) \/ PinChunk(c) \/ UnpinChunk(c) \/ GCChunk(c)
    \/ \E s \in Snapshots, r \in RootNodes, l \in LeafNodes, c \in Chunks, side \in {"left", "right"} : Publish(s, r, l, c, side)
    \/ \E s \in Snapshots : ExpireSnapshot(s)
    \/ \E n \in Nodes : GCNode(n)
    \/ Tick

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ live_root \in RootNodes
    /\ \A s \in Snapshots : snapshot_state[s] \in {"empty", "active", "expired"}
    /\ \A n \in Nodes : node_state[n] \in {"free", "active"} /\ node_refcount[n] \in Nat
    /\ \A c \in Chunks : chunk_state[c] \in {"new", "committed", "collected"} /\ chunk_refcount[c] \in Nat

NodeRefsCorrect == node_refcount = NodeRefcounts(live_root, snapshot_state, snapshot_root,
                                                   node_state, left_child, right_child)
ChunkRefsCorrect == chunk_refcount = ChunkRefcounts(node_state, leaf_chunks)
RetainedSnapshotsReadable == \A s \in Snapshots : Retained(s) => SnapshotReadable(s)
NoCollectedReachableChunk == \A c \in VisibleChunks(live_root) : chunk_state[c] = "committed"
NoPins == pinned_chunks = {}

=============================================================================
