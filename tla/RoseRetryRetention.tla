---------------------- MODULE RoseRetryRetention ----------------------
EXTENDS Integers, Sequences, FiniteSets

\* Two immutable ordered versions, one namespace head, one snapshot, and two
\* handle owners. Version 1 repeats a chunk: references count occurrences.
Versions == {1, 2}
Handles == {1, 2}
Chunks == {"a", "b"}
Extents(v) == IF v = 1 THEN <<"a", "b", "a">>
              ELSE IF v = 2 THEN <<"b">> ELSE <<>>
Occurrences(v, c) == Cardinality({i \in 1..Len(Extents(v)) : Extents(v)[i] = c})
RootCount(roots, c) == Cardinality(UNION {
    {<<v, i>> : i \in {j \in 1..Len(Extents(v)) : Extents(v)[j] = c}}
    : v \in roots})
VersionBytes(v) == {Extents(v)[i] : i \in 1..Len(Extents(v))}

VARIABLE s
vars == <<s>>

Init == s = [now |-> 0, head |-> 0, snapshot |-> 0,
    state |-> [v \in Versions |-> "new"],
    deadline |-> [v \in Versions |-> 0], roots |-> {},
    refs |-> [c \in Chunks |-> 0], disk |-> {},
    pins |-> [h \in Handles |-> 0], published |-> {},
    requests |-> [h \in Handles |-> 0],
    badAdmission |-> FALSE, badReuse |-> FALSE]

Publish(v) ==
    /\ s.state[v] = "new"
    /\ s.now < 3
    /\ s' = [s EXCEPT !.head = v, !.state[v] = "committed",
        !.deadline[v] = s.now + 1, !.roots = @ \cup {v},
        !.refs = [c \in Chunks |-> s.refs[c] - Occurrences(s.head,c)
                                      + 2 * Occurrences(v,c)],
        !.disk = @ \cup VersionBytes(v), !.published = @ \cup {v},
        !.badReuse = @ \/ v \in s.published]

Unlink ==
    /\ s.head # 0
    /\ s' = [s EXCEPT !.head = 0,
        !.refs = [c \in Chunks |-> s.refs[c] - Occurrences(s.head,c)]]

Snapshot ==
    /\ s.head # 0 /\ s.snapshot = 0
    /\ s' = [s EXCEPT !.snapshot = s.head,
        !.refs = [c \in Chunks |-> s.refs[c] + Occurrences(s.head,c)]]

DropSnapshot ==
    /\ s.snapshot # 0
    /\ s' = [s EXCEPT !.snapshot = 0,
        !.refs = [c \in Chunks |-> s.refs[c] - Occurrences(s.snapshot,c)]]

Tick == /\ s.now < 3 /\ s' = [s EXCEPT !.now = @ + 1]

Expire(v) ==
    /\ v \in s.roots /\ s.now >= s.deadline[v]
    /\ s' = [s EXCEPT !.roots = @ \ {v}, !.state[v] = "expired",
        !.refs = [c \in Chunks |-> s.refs[c] - Occurrences(v,c)]]

OpenRetry(v,h) ==
    /\ s.pins[h] = 0 /\ s.state[v] = "committed"
    /\ s.now < s.deadline[v] \* open admission
    /\ s' = [s EXCEPT !.pins[h] = v, !.requests[h] = v,
        !.badAdmission = @ \/ s.now >= s.deadline[v]]

RetryMutation(h) ==
    /\ s.pins[h] # 0
    /\ s.state[s.pins[h]] = "committed"
    /\ s.now < s.deadline[s.pins[h]] \* mutation admission
    /\ s' = [s EXCEPT !.badAdmission = @ \/ s.now >= s.deadline[s.pins[h]]]

Close(h) == /\ s.pins[h] # 0 /\ s' = [s EXCEPT !.pins[h] = 0, !.requests[h] = 0]
Crash == s' = [s EXCEPT !.pins = [h \in Handles |-> 0], !.requests = [h \in Handles |-> 0]]
Pinned(c) == \E h \in Handles : c \in VersionBytes(s.pins[h])
GC(c) ==
    /\ c \in s.disk /\ s.refs[c] = 0
    /\ ~Pinned(c) \* reader ownership
    /\ s' = [s EXCEPT !.disk = @ \ {c}]

Next == (\E v \in Versions : Publish(v) \/ Expire(v))
    \/ (\E v \in Versions, h \in Handles : OpenRetry(v,h))
    \/ (\E h \in Handles : RetryMutation(h) \/ Close(h))
    \/ (\E c \in Chunks : GC(c))
    \/ Unlink \/ Snapshot \/ DropSnapshot \/ Tick \/ Crash
Spec == Init /\ [][Next]_vars

\* Conditional progress: time reaches each finite deadline and a continuously
\* eligible expiry transaction is eventually scheduled. No fairness is imposed
\* on clients releasing their handles, so eventual chunk reclamation is not claimed.
LiveSpec == Spec /\ WF_vars(Tick) /\ (\A v \in Versions : WF_vars(Expire(v)))
RetentionEventuallyExpires == \A v \in Versions :
    (v \in s.roots) ~> (v \notin s.roots)

TypeOK == s \in [now : 0..3, head : 0..2, snapshot : 0..2,
    state : [Versions -> {"new","committed","expired"}],
    deadline : [Versions -> 0..3], roots : SUBSET Versions,
    refs : [Chunks -> 0..12], disk : SUBSET Chunks,
    pins : [Handles -> 0..2], published : SUBSET Versions,
    requests : [Handles -> 0..2],
    badAdmission : BOOLEAN, badReuse : BOOLEAN]
ExactReferences == \A c \in Chunks : s.refs[c] =
    Occurrences(s.head,c) + Occurrences(s.snapshot,c) + RootCount(s.roots,c)
RootsMatchResults == \A v \in Versions : (v \in s.roots) <=> s.state[v] = "committed"
RetainedReadable == \A v \in s.roots : VersionBytes(v) \subseteq s.disk
PinnedReadable == \A h \in Handles : VersionBytes(s.pins[h]) \subseteq s.disk
NamespaceReadable == VersionBytes(s.head) \cup VersionBytes(s.snapshot) \subseteq s.disk
NoExpiredAdmission == ~s.badAdmission
NoKeyReuse == ~s.badReuse
PinnedResultIdentity == \A h \in Handles : s.pins[h] = s.requests[h]
=============================================================================
