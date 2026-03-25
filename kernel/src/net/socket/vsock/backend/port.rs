// SPDX-License-Identifier: MPL-2.0

use super::space::vsock_space;
use crate::net::socket::vsock::addr::VsockSocketAddr;

/// Represents one ownership of a bound local vsock port.
#[derive(Debug)]
pub(in crate::net::socket::vsock) struct BoundPort {
    port: u32,
}

impl BoundPort {
    pub(super) const fn new(port: u32) -> Self {
        Self { port }
    }

    pub(super) const fn port(&self) -> u32 {
        self.port
    }

    pub(in crate::net::socket::vsock) fn local_addr(&self, guest_cid: u32) -> VsockSocketAddr {
        VsockSocketAddr {
            cid: guest_cid,
            port: self.port,
        }
    }
}

impl Drop for BoundPort {
    fn drop(&mut self) {
        vsock_space().put_bound_port(self.port);
    }
}
