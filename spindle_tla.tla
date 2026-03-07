--------------------------- MODULE spindle_tla ---------------------------
EXTENDS Integers, FiniteSets

CONSTANTS 
    Nodes,      \* The set of worker nodes/pods competing for the lock
    Duration    \* The lease duration before a lock is considered expired

VARIABLES
    db_token,        \* The current valid fencing token in the database
    db_heartbeat,    \* The last heartbeat timestamp in the database
    node_token,      \* The token locally held by each node
    now              \* Logical global clock representing Spanner's TrueTime

vars == <<db_token, db_heartbeat, node_token, now>>

\* Initial State: Database is empty, time starts at 1.
Init ==
    /\ db_token = 0
    /\ db_heartbeat = 0
    /\ node_token = [n \in Nodes |-> 0]
    /\ now = 1

\* Simulate the passage of time.
AdvanceTime ==
    /\ now' = now + 1
    /\ UNCHANGED <<db_token, db_heartbeat, node_token>>

\* Initial Acquisition: A node attempts the initial lock if the table is completely empty.
\* Maps to: INSERT ... VALUES (..., PENDING_COMMIT_TIMESTAMP(), ...)
AttemptInitialLock(n) ==
    /\ db_token = 0
    /\ db_token' = now
    /\ db_heartbeat' = now
    /\ node_token' = [node_token EXCEPT ![n] = now]
    /\ now' = now + 1

\* Heartbeat: The active leader periodically updates the heartbeat.
\* Maps to: UPDATE ... SET heartbeat = PENDING_COMMIT_TIMESTAMP() WHERE name = @name
Heartbeat(n) ==
    /\ node_token[n] > 0           \* Node locally thinks it holds a valid token
    /\ db_heartbeat' = now         \* In Spindle, this is a blind UPDATE
    /\ db_token' = db_token
    /\ node_token' = node_token
    /\ now' = now + 1

\* Takeover: A node notices the lock has expired and takes over.
\* Maps to: INSERT name_<current_token> ... followed by UPDATE token = PENDING_COMMIT_TIMESTAMP()
AttemptTakeover(n) ==
    /\ db_token > 0
    /\ now - db_heartbeat > Duration
    \* Atomically update the token and heartbeat (simulating the successful Spanner transaction)
    /\ db_token' = now
    /\ db_heartbeat' = now
    /\ node_token' = [node_token EXCEPT ![n] = now]
    /\ now' = now + 1

\* Define the possible state transitions
Next ==
    \/ AdvanceTime
    \/ \E n \in Nodes : AttemptInitialLock(n)
    \/ \E n \in Nodes : Heartbeat(n)
    \/ \E n \in Nodes : AttemptTakeover(n)

-----------------------------------------------------------------------------
\* INVARIANTS

\* 1. Unique Fencing Tokens
\* Proof that Spindle never assigns the same token to two different nodes.
UniqueTokens ==
    \A n1, n2 \in Nodes :
        (n1 # n2 /\ node_token[n1] > 0 /\ node_token[n2] > 0)
        => node_token[n1] # node_token[n2]

\* 2. Single Global Leader Authority
\* At any exact instance in time, the database token matches at most ONE node's token.
\* This guarantees that `HasLock()` will only ever return TRUE for one node at a time.
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