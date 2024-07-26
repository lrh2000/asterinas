// SPDX-License-Identifier: MPL-2.0

use crate::{fs::path::Dentry, net::socket::util::socket_addr::SocketAddr, prelude::*};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum UnixSocketAddr {
    Unnamed,
    Path(String),
    Abstract(Vec<u8>),
}

#[derive(Clone, Debug)]
pub(super) enum UnixSocketAddrBound {
    Path(Arc<Dentry>),
    Abstract(Vec<u8>),
}

impl TryFrom<SocketAddr> for UnixSocketAddr {
    type Error = Error;

    fn try_from(value: SocketAddr) -> Result<Self> {
        match value {
            SocketAddr::Unix(unix_socket_addr) => Ok(unix_socket_addr),
            _ => return_errno_with_message!(Errno::EINVAL, "Invalid unix socket addr"),
        }
    }
}

impl From<UnixSocketAddrBound> for UnixSocketAddr {
    fn from(value: UnixSocketAddrBound) -> Self {
        match value {
            UnixSocketAddrBound::Path(dentry) => {
                let abs_path = dentry.abs_path();
                Self::Path(abs_path)
            }
            UnixSocketAddrBound::Abstract(name) => Self::Abstract(name),
        }
    }
}

impl From<Option<UnixSocketAddrBound>> for UnixSocketAddr {
    fn from(value: Option<UnixSocketAddrBound>) -> Self {
        match value {
            Some(addr) => addr.into(),
            None => Self::Unnamed,
        }
    }
}

impl<T: Into<UnixSocketAddr>> From<T> for SocketAddr {
    fn from(value: T) -> Self {
        SocketAddr::Unix(value.into())
    }
}
