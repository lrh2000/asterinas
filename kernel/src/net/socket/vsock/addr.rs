// SPDX-License-Identifier: MPL-2.0

use crate::{net::socket::util::SocketAddr, prelude::*};

pub const VMADDR_CID_ANY: u32 = u32::MAX;
pub const VMADDR_PORT_ANY: u32 = u32::MAX;
pub const VMADDR_CID_HOST: u32 = 2;

/// Represents a vsock socket address.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Pod)]
#[repr(C)]
pub struct VsockSocketAddr {
    pub cid: u32,
    pub port: u32,
}

impl VsockSocketAddr {
    pub const fn is_any_cid(&self) -> bool {
        self.cid == VMADDR_CID_ANY
    }

    pub const fn is_any_port(&self) -> bool {
        self.port == VMADDR_PORT_ANY
    }
}

impl TryFrom<SocketAddr> for VsockSocketAddr {
    type Error = Error;

    fn try_from(value: SocketAddr) -> Result<Self> {
        let SocketAddr::Vsock(addr) = value else {
            return_errno_with_message!(Errno::EINVAL, "the socket address is not vsock");
        };

        Ok(addr)
    }
}

impl From<VsockSocketAddr> for SocketAddr {
    fn from(value: VsockSocketAddr) -> Self {
        SocketAddr::Vsock(value)
    }
}
