// SPDX-License-Identifier: MPL-2.0

use core::{
    borrow::Borrow,
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};

use keyable_arc::{KeyableArc, KeyableWeak};
use ostd::sync::LocalIrqDisabled;

use super::*;
use crate::{
    events::Observer,
    fs::{file_handle::FileLike, utils::IoctlCmd},
    process::signal::{AnyPoller, Pollable, Pollee},
};

/// A file-like object that provides epoll API.
///
/// Conceptually, we maintain two lists: one consists of all interesting files,
/// which can be managed by the epoll ctl commands; the other are for ready files,
/// which are files that have some events. A epoll wait only needs to iterate the
/// ready list and poll each file to see if the file is ready for the interesting
/// I/O.
///
/// To maintain the ready list, we need to monitor interesting events that happen
/// on the files. To do so, the `EpollFile` registers itself as an `Observer` to
/// the monotored files. Thus, we can add a file to the ready list when an interesting
/// event happens on the file.
pub struct EpollFile {
    // All interesting entries.
    interest: Mutex<BTreeSet<EpollEntryHolder>>,
    // A set of ready entries.
    //
    // Keep this in a separate `Arc` to avoid dropping `EpollFile` in the observer callback, which
    // may cause deadlocks.
    ready: Arc<ReadySet>,
}

impl EpollFile {
    /// Creates a new epoll file.
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            interest: Mutex::new(BTreeSet::new()),
            ready: Arc::new(ReadySet::new()),
        })
    }

    /// Control the interest list of the epoll file.
    pub fn control(&self, cmd: &EpollCtl) -> Result<()> {
        let fd = match cmd {
            EpollCtl::Add(fd, ..) => *fd,
            EpollCtl::Del(fd) => *fd,
            EpollCtl::Mod(fd, ..) => *fd,
        };

        let file = {
            let current = current!();
            let file_table = current.file_table().lock();
            file_table.get_file(fd)?.clone()
        };

        match *cmd {
            EpollCtl::Add(fd, ep_event, ep_flags) => {
                self.add_interest(fd, file, ep_event, ep_flags)
            }
            EpollCtl::Del(fd) => self.del_interest(fd, Arc::downgrade(&file).into()),
            EpollCtl::Mod(fd, ep_event, ep_flags) => {
                self.mod_interest(fd, file, ep_event, ep_flags)
            }
        }
    }

    fn add_interest(
        &self,
        fd: FileDesc,
        file: Arc<dyn FileLike>,
        ep_event: EpollEvent,
        ep_flags: EpollFlags,
    ) -> Result<()> {
        self.warn_unsupported_flags(&ep_flags);

        // Add the new entry to the interest list and start monitoring its events
        let ready_entry = {
            let mut interest = self.interest.lock();

            if interest.contains(&EpollEntryKey::from((fd, &file))) {
                return_errno_with_message!(
                    Errno::EEXIST,
                    "the file is already in the interest list"
                );
            }

            let entry = EpollEntry::new(fd, Arc::downgrade(&file).into(), self.ready.clone());
            let events = entry.update(ep_event, ep_flags);

            let ready_entry = if !events.is_empty() {
                Some(entry.clone())
            } else {
                None
            };

            let inserted = interest.insert(entry.into());
            assert!(inserted);

            ready_entry
        };

        // Add the new entry to the ready list if the file is ready
        if let Some(entry) = ready_entry {
            self.ready.push(entry.observer());
        }

        Ok(())
    }

    fn del_interest(&self, fd: FileDesc, file: KeyableWeak<dyn FileLike>) -> Result<()> {
        // If this epoll entry is in the ready list, then we should delete it.
        // But unfortunately, deleting an entry from the ready list has a
        // complexity of O(N).
        //
        // To optimize performance, we postpone the actual deletion to the time
        // when the ready list is scanned in `EpolFile::wait`. This can be done
        // because the strong reference count will reach zero and `Weak::upgrade`
        // will fail.

        if !self
            .interest
            .lock()
            .remove(&EpollEntryKey::from((fd, file)))
        {
            return_errno_with_message!(Errno::ENOENT, "the file is not in the interest list");
        }

        Ok(())
    }

    fn mod_interest(
        &self,
        fd: FileDesc,
        file: Arc<dyn FileLike>,
        new_ep_event: EpollEvent,
        new_ep_flags: EpollFlags,
    ) -> Result<()> {
        self.warn_unsupported_flags(&new_ep_flags);

        // Update the epoll entry
        let ready_entry = {
            let interest = self.interest.lock();

            let EpollEntryHolder(entry) = interest
                .get(&EpollEntryKey::from((fd, &file)))
                .ok_or_else(|| {
                    Error::with_message(Errno::ENOENT, "the file is not in the interest list")
                })?;
            let events = entry.update(new_ep_event, new_ep_flags);

            if !events.is_empty() {
                Some(entry.clone())
            } else {
                None
            }
        };

        // Add the updated entry to the ready list if the file is ready
        if let Some(entry) = ready_entry {
            self.ready.push(entry.observer());
        }

        Ok(())
    }

    /// Wait for interesting events happen on the files in the interest list
    /// of the epoll file.
    ///
    /// This method blocks until either some interesting events happen or
    /// the timeout expires or a signal arrives. The first case returns
    /// `Ok(events)`, where `events` is a `Vec` containing at most `max_events`
    /// number of `EpollEvent`s. The second and third case returns errors.
    ///
    /// When `max_events` equals to zero, the method returns when the timeout
    /// expires or a signal arrives.
    pub fn wait(&self, max_events: usize, timeout: Option<&Duration>) -> Result<Vec<EpollEvent>> {
        let mut ep_events = Vec::new();

        self.wait_events(IoEvents::IN, timeout, || {
            self.pop_multi_ready(max_events, &mut ep_events);

            if ep_events.is_empty() {
                return Err(Error::with_message(
                    Errno::EAGAIN,
                    "there are no available events",
                ));
            }

            Ok(())
        })?;

        Ok(ep_events)
    }

    fn pop_multi_ready(&self, max_events: usize, ep_events: &mut Vec<EpollEvent>) {
        let mut drain = self.ready.drain();

        loop {
            if ep_events.len() >= max_events {
                break;
            }

            // Since we're holding `pop_guard`, no one else can pop the entries from the ready
            // list. This guarantees that `pop_one_ready` will pop the ready entries we see when
            // `pop_multi_ready` starts executing, so that such entries are never duplicated.
            let Some(entry) = drain.next() else {
                break;
            };

            // Poll the events. If the file is dead, we will remove the entry.
            let Some((ep_event, is_still_ready)) = entry.poll() else {
                // We're removing entries whose files are dead. This can only fail if user programs
                // remove the entry at the same time, and we run into some race conditions.
                //
                // However, this has very limited impact because we will never remove a wrong entry. So
                // the error can be silently ignored.
                let _ = self.del_interest(entry.fd(), entry.file_weak().clone());
                continue;
            };

            // Save the event in the output vector, if any.
            if let Some(event) = ep_event {
                ep_events.push(event);
            }

            // Add the entry back to the ready list, if necessary.
            if is_still_ready {
                self.ready.push(entry.observer());
            }
        }
    }

    fn warn_unsupported_flags(&self, flags: &EpollFlags) {
        if flags.intersects(EpollFlags::EXCLUSIVE | EpollFlags::WAKE_UP) {
            warn!("{:?} contains unsupported flags", flags);
        }
    }
}

impl Pollable for EpollFile {
    fn poll(&self, mask: IoEvents, poller: Option<&mut AnyPoller>) -> IoEvents {
        self.ready.poll(mask, poller)
    }
}

// Implement the common methods required by FileHandle
impl FileLike for EpollFile {
    fn read(&self, _writer: &mut VmWriter) -> Result<usize> {
        return_errno_with_message!(Errno::EINVAL, "epoll files do not support read");
    }

    fn write(&self, _reader: &mut VmReader) -> Result<usize> {
        return_errno_with_message!(Errno::EINVAL, "epoll files do not support write");
    }

    fn ioctl(&self, _cmd: IoctlCmd, _arg: usize) -> Result<i32> {
        return_errno_with_message!(Errno::EINVAL, "epoll files do not support ioctl");
    }
}

/// A set of ready epoll entries.
pub struct ReadySet {
    // Entries that are probably ready (having events happened).
    entries: SpinLock<VecDeque<Weak<EpollEntry>>, LocalIrqDisabled>,
    // A guard to ensure that ready entries can be popped by one thread at a time.
    pop_guard: Mutex<PopGuard>,
    // A pollee for the ready set (i.e., for `EpollFile` itself).
    pollee: Pollee,
}

struct PopGuard;

impl ReadySet {
    pub fn new() -> Self {
        Self {
            entries: SpinLock::new(VecDeque::new()),
            pop_guard: Mutex::new(PopGuard),
            pollee: Pollee::new(IoEvents::empty()),
        }
    }

    pub fn push(&self, observer: &EpollEntryObserver) {
        // Note that we cannot take the `EpollEntryInner` lock because we are in the callback of
        // the event observer. Doing so will cause dead locks due to inconsistent locking orders.
        //
        // We don't need to take the lock because
        // - We always call `file.poll()` immediately after calling `self.set_enabled()` and
        //   `file.register_observer()`, so all events are caught either here or by the immediate
        //   poll; in other words, we don't lose any events.
        // - Catching spurious events here is always fine because we always check them later before
        //   returning events to the user (in `EpollEntry::poll`).
        if !observer.is_enabled() {
            return;
        }

        let mut entries = self.entries.lock();

        if !observer.is_ready() {
            observer.set_ready(&entries);
            entries.push_back(observer.weak_entry().clone())
        }

        // Even if the entry is already set to ready,
        // there might be new events that we are interested in.
        // Wake the poller anyway.
        self.pollee.add_events(IoEvents::IN);
    }

    pub fn drain(&self) -> ReadySetDrain {
        ReadySetDrain {
            ready_set: self,
            _pop_guard: self.pop_guard.lock(),
            limit: None,
        }
    }

    pub fn poll(&self, mask: IoEvents, poller: Option<&mut AnyPoller>) -> IoEvents {
        self.pollee.poll(mask, poller)
    }
}

/// A draining iterator for [`ReadySet`].
pub struct ReadySetDrain<'a> {
    ready_set: &'a ReadySet,
    _pop_guard: MutexGuard<'a, PopGuard>,
    limit: Option<usize>,
}

impl Iterator for ReadySetDrain<'_> {
    type Item = Arc<EpollEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.limit == Some(0) {
            return None;
        }

        let mut entries = self.ready_set.entries.lock();
        let mut limit = self.limit.unwrap_or_else(|| entries.len());

        while limit > 0 {
            limit -= 1;

            // Pop the front entry. Note that `_guard` and `limit` guarantee that this entry must
            // exist, so we can just unwrap it.
            let weak_entry = entries.pop_front().unwrap();

            // Clear the epoll file's events if there are no ready entries.
            if entries.len() == 0 {
                self.ready_set.pollee.del_events(IoEvents::IN);
            }

            let Some(entry) = Weak::upgrade(&weak_entry) else {
                // The entry has been deleted.
                continue;
            };

            // Mark the entry as not ready. We can invoke `push_ready` later to add it back to the
            // ready list if we need to.
            entry.observer().reset_ready(&entries);

            self.limit = Some(limit);
            return Some(entry);
        }

        self.limit = None;
        None
    }
}

/// An epoll entry that is contained in an epoll file.
///
/// Each epoll entry can be added, modified, or deleted by the `EpollCtl` command.
pub struct EpollEntry {
    // The file descriptor and the file
    key: EpollEntryKey,
    // The event masks and flags
    inner: Mutex<EpollEntryInner>,
    // The observer that receives events.
    //
    // Keep this in a separate `Arc` to avoid dropping `EpollEntry` in the observer callback, which
    // may cause deadlocks.
    observer: Arc<EpollEntryObserver>,
}

#[derive(PartialEq, Eq, PartialOrd, Ord)]
struct EpollEntryKey {
    fd: FileDesc,
    file: KeyableWeak<dyn FileLike>,
}

impl From<(FileDesc, KeyableWeak<dyn FileLike>)> for EpollEntryKey {
    fn from(value: (FileDesc, KeyableWeak<dyn FileLike>)) -> Self {
        Self {
            fd: value.0,
            file: value.1,
        }
    }
}

impl From<(FileDesc, &Arc<dyn FileLike>)> for EpollEntryKey {
    fn from(value: (FileDesc, &Arc<dyn FileLike>)) -> Self {
        Self {
            fd: value.0,
            file: KeyableWeak::from(Arc::downgrade(value.1)),
        }
    }
}

struct EpollEntryInner {
    event: EpollEvent,
    flags: EpollFlags,
    poller: AnyPoller,
}

impl EpollEntry {
    /// Creates a new epoll entry associated with the given epoll file.
    pub fn new(
        fd: FileDesc,
        file: KeyableWeak<dyn FileLike>,
        ready_set: Arc<ReadySet>,
    ) -> Arc<Self> {
        Arc::new_cyclic(|me| {
            let observer = Arc::new(EpollEntryObserver::new(ready_set, me.clone()));

            let inner = EpollEntryInner {
                event: EpollEvent {
                    events: IoEvents::empty(),
                    user_data: 0,
                },
                flags: EpollFlags::empty(),
                poller: AnyPoller::new(Arc::downgrade(&observer) as _),
            };

            Self {
                key: EpollEntryKey { fd, file },
                inner: Mutex::new(inner),
                observer,
            }
        })
    }

    /// Get the file associated with this epoll entry.
    ///
    /// Since an epoll entry only holds a weak reference to the file,
    /// it is possible (albeit unlikely) that the file has been dropped.
    pub fn file(&self) -> Option<Arc<dyn FileLike>> {
        self.key.file.upgrade().map(KeyableArc::into)
    }

    /// Polls the events of the file associated with this epoll entry.
    ///
    /// This method returns `None` if the file is dead. Otherwise, it returns the epoll event (if
    /// any) and a boolean value indicating whether the entry should be kept in the ready list
    /// (`true`) or removed from the ready list (`false`).
    pub fn poll(&self) -> Option<(Option<EpollEvent>, bool)> {
        let file = self.file()?;
        let inner = self.inner.lock();

        // There are no events if the entry is disabled.
        if !self.observer.is_enabled() {
            return Some((None, false));
        }

        // Check whether the entry's file has some events.
        let io_events = file.poll(inner.event.events, None);

        // If this entry's file has some events, we need to return them.
        let ep_event = if !io_events.is_empty() {
            Some(EpollEvent::new(io_events, inner.event.user_data))
        } else {
            None
        };

        // If there are events and the epoll entry is neither edge-triggered nor one-shot, we need
        // to keep the entry in the ready list.
        let is_still_ready = ep_event.is_some()
            && !inner
                .flags
                .intersects(EpollFlags::EDGE_TRIGGER | EpollFlags::ONE_SHOT);

        // If there are events and the epoll entry is one-shot, we need to disable the entry until
        // the user enables it again via `EpollCtl::Mod`.
        if ep_event.is_some() && inner.flags.contains(EpollFlags::ONE_SHOT) {
            self.observer.reset_enabled(&inner);
        }

        Some((ep_event, is_still_ready))
    }

    /// Updates the epoll entry by the given event masks and flags.
    ///
    /// This method needs to be called in response to `EpollCtl::Add` and `EpollCtl::Mod`.
    pub fn update(&self, event: EpollEvent, flags: EpollFlags) -> IoEvents {
        let file = self.file().unwrap();

        let mut inner = self.inner.lock();

        inner.event = event;
        inner.flags = flags;

        self.observer.set_enabled(&inner);

        file.poll(event.events, Some(&mut inner.poller))
    }

    /// Shuts down the epoll entry.
    ///
    /// This method needs to be called in response to `EpollCtl::Del`.
    pub fn shutdown(&self) {
        let mut inner = self.inner.lock();

        self.observer.reset_enabled(&inner);
        inner.poller.reset();
    }

    /// Gets the underlying observer.
    pub fn observer(&self) -> &EpollEntryObserver {
        &self.observer
    }

    /// Get the file descriptor associated with the epoll entry.
    pub fn fd(&self) -> FileDesc {
        self.key.fd
    }

    /// Get the file associated with this epoll entry.
    pub fn file_weak(&self) -> &KeyableWeak<dyn FileLike> {
        &self.key.file
    }
}

/// A observer for [`EpollEntry`] that can receive events.
pub struct EpollEntryObserver {
    // Whether the entry is enabled
    is_enabled: AtomicBool,
    // Whether the entry is in the ready list
    is_ready: AtomicBool,
    // The ready set of the epoll file that contains this epoll entry
    ready_set: Arc<ReadySet>,
    // The epoll entry itself (always inside an `Arc`)
    weak_entry: Weak<EpollEntry>,
}

impl EpollEntryObserver {
    pub fn new(ready_set: Arc<ReadySet>, weak_entry: Weak<EpollEntry>) -> Self {
        Self {
            is_enabled: AtomicBool::new(false),
            is_ready: AtomicBool::new(false),
            ready_set,
            weak_entry,
        }
    }

    /// Returns whether the epoll entry is in the ready list.
    ///
    /// *Caution:* If this method is called without holding the lock of the ready list, the user
    /// must ensure that the behavior is desired with respect to the way the ready list might be
    /// modified concurrently.
    pub fn is_ready(&self) -> bool {
        self.is_ready.load(Ordering::Relaxed)
    }

    /// Marks the epoll entry as being in the ready list.
    ///
    /// This method must be called while holding the lock of the ready list. This is the only way
    /// to ensure that the "is ready" state matches the fact that the entry is actually in the
    /// ready list.
    pub fn set_ready(&self, _guard: &SpinLockGuard<VecDeque<Weak<EpollEntry>>, LocalIrqDisabled>) {
        self.is_ready.store(true, Ordering::Relaxed);
    }

    /// Marks the epoll entry as not being in the ready list.
    ///
    /// This method must be called while holding the lock of the ready list. This is the only way
    /// to ensure that the "is ready" state matches the fact that the entry is actually in the
    /// ready list.
    pub fn reset_ready(
        &self,
        _guard: &SpinLockGuard<VecDeque<Weak<EpollEntry>>, LocalIrqDisabled>,
    ) {
        self.is_ready.store(false, Ordering::Relaxed)
    }

    /// Returns whether the epoll entry is enabled.
    ///
    /// *Caution:* If this method is called without holding the lock of the event masks and flags,
    /// the user must ensure that the behavior is desired with respect to the way the event masks
    /// and flags might be modified concurrently.
    pub fn is_enabled(&self) -> bool {
        self.is_enabled.load(Ordering::Relaxed)
    }

    /// Marks the epoll entry as enabled.
    ///
    /// This method must be called while holding the lock of the event masks and flags. This is the
    /// only way to ensure that the "is enabled" state describes the correct combination of the
    /// event masks and flags.
    fn set_enabled(&self, _guard: &MutexGuard<EpollEntryInner>) {
        self.is_enabled.store(true, Ordering::Relaxed)
    }

    /// Marks the epoll entry as not enabled.
    ///
    /// This method must be called while holding the lock of the event masks and flags. This is the
    /// only way to ensure that the "is enabled" state describes the correct combination of the
    /// event masks and flags.
    fn reset_enabled(&self, _guard: &MutexGuard<EpollEntryInner>) {
        self.is_enabled.store(false, Ordering::Relaxed)
    }

    /// Gets an instance of `Weak` that refers to the epoll entry.
    pub fn weak_entry(&self) -> &Weak<EpollEntry> {
        &self.weak_entry
    }
}

impl Observer<IoEvents> for EpollEntryObserver {
    fn on_events(&self, _events: &IoEvents) {
        self.ready_set.push(self);
    }
}

struct EpollEntryHolder(pub Arc<EpollEntry>);

impl PartialOrd for EpollEntryHolder {
    fn partial_cmp(&self, other: &Self) -> Option<core::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for EpollEntryHolder {
    fn cmp(&self, other: &Self) -> core::cmp::Ordering {
        self.0.key.cmp(&other.0.key)
    }
}
impl PartialEq for EpollEntryHolder {
    fn eq(&self, other: &Self) -> bool {
        self.0.key.eq(&other.0.key)
    }
}
impl Eq for EpollEntryHolder {}

impl Borrow<EpollEntryKey> for EpollEntryHolder {
    fn borrow(&self) -> &EpollEntryKey {
        &self.0.key
    }
}

impl From<Arc<EpollEntry>> for EpollEntryHolder {
    fn from(value: Arc<EpollEntry>) -> Self {
        Self(value)
    }
}

impl Drop for EpollEntryHolder {
    fn drop(&mut self) {
        self.0.shutdown();
    }
}
