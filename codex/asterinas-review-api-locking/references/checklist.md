# Smell Catalog

Use this list when reviewing Asterinas code. These are high-value patterns that deserve explicit
verification.

## 1. Split snapshot and commit helpers

Examples:

- `make_header()` followed later by `mark_credit_reported()`
- `make_credit_request_header_if_needed()` followed by either `mark_*()` or `rollback_*()`
- `prepare_*()` followed later by a second lock acquisition to decide the table action

Questions:

- Is the “mark” step recording the same snapshot that was actually sent?
- Can another thread mutate the state between the snapshot and the mark?
- Should the helper return a transactional object that carries the snapshot value?

## 2. One-shot or partially-invalid guards

Examples:

- `Option<SpinLockGuard<...>>`
- `take()` from a guard and later `unwrap()` in other methods
- error paths that consume the guard and leave a shell object behind

Questions:

- Why is the type not expressing the state transition directly?
- Would returning an error object that owns the guard be simpler?
- Can a future caller accidentally reuse the half-invalid wrapper?

## 3. Drop-side-effect carrier objects

Examples:

- empty marker traits combined with `Drop`
- completion objects whose only purpose is to release quota or wake someone

Questions:

- Is the object stored in the state that matches the real completion point?
- If a request moves from pending to inflight, does the carrier move too?
- Is the completion dropped at submission time instead of real completion time?

## 4. Notify under spinlock

Examples:

- `notify()` with a named spinlock guard still in scope
- outer table lock held while calling a helper that notifies

Questions:

- Is the guard explicitly dropped before notify?
- Is the notify path guaranteed not to call back into code that wants the same or another lock?
- Can the lock scope be reduced by cloning an `Arc` and notifying outside?

## 5. Helper fragmentation on hot paths

Examples:

- `check_send_ready() -> send_credit_available() -> make_header() -> update_tx_cnt() -> mark_*()`
- repeated `state.lock()` in one send or recv path

Questions:

- Should these become one `prepare_send()` or `commit_send()` API?
- Is the current split only an artifact of convenience?
- Does the split force repeated locking of the same field?

## 6. Dead caches and unnecessary shared mutability

Examples:

- query a value from hardware, then store it into a field nobody reads
- immutable peer metadata hidden behind the main mutable state lock

Questions:

- Is the field actually used?
- Should this be an immutable field instead?
- Is this turning a read-only operation into a contended write path?

## 7. Hidden caller protocols

Examples:

- caller must remember “rollback on send failure”
- caller must remember “mark reported on success”
- caller must remember “remove from table before reset”

Questions:

- Can the API make the safe path the default?
- Can failure cleanup be encoded in `Drop` or a transactional helper instead of caller memory?
- Is a misordered call sequence currently possible?
