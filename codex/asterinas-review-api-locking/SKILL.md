---
name: asterinas-review-api-locking
description: Use when reviewing Asterinas kernel, network, virtio, or driver code for API and concurrency design flaws that are easy to miss in a normal correctness pass: repeated locking on the same object, split snapshot or commit helper APIs, awkward guard ownership such as Option-wrapped guards or one-shot guards, notify or callback under spinlock, manual rollback or mark protocols, Arc or Drop lifetime mismatches, dead cached state, and duplicated or fragmented state-machine helpers.
---

# Asterinas API/Locking Review

Use this skill after the first correctness pass when the task is a code review or design review.

Do not stop at races and lock order. Also review whether the API shape itself is forcing callers
to write fragile code.

## What to look for

- Repeated locking on the same field in one logical operation.
- Helper pairs or triplets that form a hidden transaction.
- Guards that carry invalid states at runtime instead of in the type system.
- `Drop` side effects whose lifetime is not carried across queue or state transitions.
- Notifications or callbacks while holding a spinlock.
- Outer locks held while calling helpers that take inner locks or notify pollers.
- Cached state that is recomputed every call and then written back to a field nobody reads.
- Immutable data stored behind a mutable lock.
- Duplicated state spread across multiple helpers where the caller must remember the protocol.

## Mandatory workflow

1. Enumerate the hot paths.
   For each public or cross-layer operation, write the exact call chain on the same object.

2. Count lock acquisitions per path.
   If one logical send, recv, shutdown, timeout, or queue operation locks the same state more
   than once, treat that as a review target, not as harmless style.

3. Search for split transactions.
   Any pattern like `make/build/prepare -> send/do side effect -> mark/commit/rollback` is
   suspicious. Assume it is wrong until you prove the split is intentional and race-free.

4. Search for guard shape smells.
   `Option<Guard>`, `take()` from a guard, `unwrap()` on guard internals, or a guard that becomes
   partially unusable after one method call usually means the type is fighting the control flow.

5. Search for lifetime-carried side effects.
   If a type exists only for `Drop` side effects, verify that ownership of that object follows the
   real completion point. If the queue state changes from pending to inflight, the `Drop` carrier
   must usually move with it.

6. Search for notify-under-lock.
   Any `notify`, wakeup, callback, or observer call while a spinlock guard is live is a review
   finding unless explicitly justified.

7. Search for dead caches and over-fragmented state.
   Fields updated on every call but not read, or immutable metadata stored behind a frequently
   contended lock, are design problems.

8. Report the issue at the API boundary.
   Do not only say “there is repeated locking here”. Explain which helper boundary is wrong and
   what kind of transactional or guard API should replace it.

## Required questions

For each suspicious call chain, answer all of these:

- Could one helper return a transactional object instead of forcing `make + send + mark`?
- Does any helper reopen the same lock just to finish a step that conceptually belongs to the
  previous helper?
- Can state change between the snapshot helper and the mark or rollback helper?
- Does the caller have to remember a hidden protocol such as “call rollback on failure”?
- Is a guard object still valid after every method on it, or is validity only enforced by
  convention and `unwrap()`?
- Does ownership of a `Drop` side-effect object survive until the real completion point?
- Is any notification performed while a spinlock or outer table lock is still held?

## Grep recipes

Use targeted searches to find fragmented APIs quickly:

```bash
rg -n "make_|build_|prepare_|mark_|rollback_|update_|notify|take\\(|Option<.*Guard|unwrap\\(" path/
rg -n "lock\\(\\).*lock\\(|state\\.lock\\(|sockets\\.lock\\(|timer\\.lock\\(" path/
rg -n "Drop for |impl .*Completion|notify\\(|invalidate\\(" path/
```

Then inspect call chains around the hits, not only the helper bodies.

## Review output requirements

When you find an issue, state:

- The logical operation being split.
- Which helpers or guards make the split necessary.
- The concrete race, redundant lock, invalid lifetime, or API misuse that becomes possible.
- The direction of the fix.

Read [references/checklist.md](references/checklist.md) for the concrete smell catalog derived from
the vsock review.
