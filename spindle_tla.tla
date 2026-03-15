--------------------------- MODULE spindle_tla ---------------------------
EXTENDS Integers, FiniteSets

CONSTANTS
    Nodes,      \* The set of worker nodes/pods competing for the lock
    Duration    \* The lease duration before a lock is considered expired

VARIABLES
    db_token,        \* The current token (commit timestamp) in the database; 0 = row absent
    db_owner,       \* The current lock holder id in the database; 0 = no holder
    node_token,      \* The token locally held by each node
    now              \* Logical global clock representing Spanner's TrueTime

vars == <<db_token, db_owner, node_token, now>>

\* Initial State: Database row is absent, time starts at 1.
Init ==
    /\ db_token = 0
    /\ db_owner = 0
    /\ node_token = [n \in Nodes |-> 0]
    /\ now = 1

\* Simulate the passage of time.
AdvanceTime ==
    /\ now' = now + 1
    /\ UNCHANGED <<db_token, db_owner, node_token>>

\* Initial Acquisition: A node acquires the lock when the row doesn't exist.
\* Maps to: InsertOrUpdate with spanner.CommitTimestamp when ReadRow returns NotFound.
AttemptInitialLock(n) ==
    /\ db_token = 0
    /\ db_token' = now
    /\ db_owner' = n
    /\ node_token' = [node_token EXCEPT ![n] = now]
    /\ now' = now + 1

\* Heartbeat: The active leader updates the token with a CAS.
\* Maps to: UPDATE SET token = PENDING_COMMIT_TIMESTAMP()
\*          WHERE name = @name AND token = @oldToken AND owner = @owner
Heartbeat(n) ==
    /\ node_token[n] > 0
    /\ db_token = node_token[n]   \* CAS: token must match
    /\ db_owner = n              \* CAS: writer must match
    /\ db_token' = now
    /\ db_owner' = n
    /\ node_token' = [node_token EXCEPT ![n] = now]
    /\ now' = now + 1

\* Takeover: A node notices the lock has expired and takes over.
\* Maps to: InsertOrUpdate when time.Since(lastToken) >= leaseDuration.
AttemptTakeover(n) ==
    /\ db_token > 0
    /\ now - db_token > Duration   \* Lease expired (checked against token)
    /\ db_token' = now
    /\ db_owner' = n
    \* Clear old leader's token, set new leader's token
    /\ node_token' = [m \in Nodes |->
        IF m = n THEN now
        ELSE IF node_token[m] = db_token THEN 0  \* Old leader loses its token
        ELSE node_token[m]]
    /\ now' = now + 1

\* Release: The leader gracefully releases the lock by deleting the row.
\* Maps to: spanner.Delete on context cancellation.
Release(n) ==
    /\ node_token[n] > 0
    /\ db_token = node_token[n]
    /\ db_owner = n
    /\ db_token' = 0
    /\ db_owner' = 0
    /\ node_token' = [node_token EXCEPT ![n] = 0]
    /\ now' = now + 1

\* Define the possible state transitions
Next ==
    \/ AdvanceTime
    \/ \E n \in Nodes : AttemptInitialLock(n)
    \/ \E n \in Nodes : Heartbeat(n)
    \/ \E n \in Nodes : AttemptTakeover(n)
    \/ \E n \in Nodes : Release(n)

-----------------------------------------------------------------------------
\* INVARIANTS

\* 1. Unique Fencing Tokens
\* Spindle never assigns the same token to two different nodes.
UniqueTokens ==
    \A n1, n2 \in Nodes :
        (n1 # n2 /\ node_token[n1] > 0 /\ node_token[n2] > 0)
        => node_token[n1] # node_token[n2]

\* 2. Single Global Leader Authority
\* At any point in time, the database token matches at most ONE node's token.
AtMostOneDBLeader ==
    \A n1, n2 \in Nodes :
        (n1 # n2 /\ node_token[n1] > 0 /\ node_token[n2] > 0)
        => ~(db_token = node_token[n1] /\ db_token = node_token[n2])

\* 3. Token Monotonicity
\* The database token is always greater than or equal to any valid local token.
\* This prevents older, suspended nodes from overwriting the future.
ValidDBToken ==
    \A n \in Nodes :
        node_token[n] > 0 => db_token >= node_token[n]

-----------------------------------------------------------------------------
\* TLC Model Checking Constraints
\* Limits the infinite state space of 'now' so the TLC checker can finish.
MaxTime == now <= 15

=============================================================================
