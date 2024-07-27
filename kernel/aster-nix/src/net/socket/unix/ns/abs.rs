// SPDX-License-Identifier: MPL-2.0

use alloc::format;

use keyable_arc::KeyableArc;

use crate::prelude::*;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AbstractHandle(KeyableArc<[u8]>);

impl AbstractHandle {
    fn new(name: Arc<[u8]>) -> Self {
        Self(KeyableArc::from(name))
    }

    pub fn name(&self) -> Arc<[u8]> {
        self.0.clone().into()
    }
}

impl Drop for AbstractHandle {
    fn drop(&mut self) {
        HANDLE_TABLE.remove(&self.0).unwrap();
    }
}

static HANDLE_TABLE: HandleTable = HandleTable::new();

struct HandleTable {
    handles: RwLock<BTreeMap<Arc<[u8]>, Arc<AbstractHandle>>>,
}

impl HandleTable {
    const fn new() -> Self {
        Self {
            handles: RwLock::new(BTreeMap::new()),
        }
    }

    fn create(&self, name: Arc<[u8]>) -> Option<Arc<AbstractHandle>> {
        let mut handles = self.handles.write();

        if handles.contains_key(&name) {
            return None;
        }

        let new_handle = Arc::new(AbstractHandle::new(name.clone()));
        handles.insert(name, new_handle.clone());

        Some(new_handle)
    }

    fn alloc_ephemeral(&self) -> Option<Arc<AbstractHandle>> {
        let mut handles = self.handles.write();

        let name: Arc<[u8]> = (0..(1 << 20))
            .map(|num| format!("{:05x}", num))
            .filter(|name| handles.contains_key(name.as_bytes()))
            .map(|name| Arc::from(name.as_bytes()))
            .next()?;

        let new_handle = Arc::new(AbstractHandle::new(name.clone()));
        handles.insert(name, new_handle.clone());

        Some(new_handle)
    }

    fn lookup(&self, name: &[u8]) -> Option<Arc<AbstractHandle>> {
        let handles = self.handles.read();

        handles.get(name).cloned()
    }

    fn remove(&self, name: &[u8]) -> Option<Arc<AbstractHandle>> {
        let mut handles = self.handles.write();

        handles.remove(name)
    }
}

pub fn create_abstract_name(name: Arc<[u8]>) -> Result<Arc<AbstractHandle>> {
    HANDLE_TABLE.create(name).ok_or_else(|| {
        Error::with_message(Errno::EADDRINUSE, "the abstract name is already in use")
    })
}

pub fn alloc_ephemeral_abstract_name() -> Result<Arc<AbstractHandle>> {
    HANDLE_TABLE.alloc_ephemeral().ok_or_else(|| {
        Error::with_message(Errno::ENOSPC, "no ephemeral abstract name is available")
    })
}

pub fn lookup_abstract_name(name: &[u8]) -> Result<Arc<AbstractHandle>> {
    HANDLE_TABLE
        .lookup(name)
        .ok_or_else(|| Error::with_message(Errno::ECONNREFUSED, "the abstract name does not exist"))
}
