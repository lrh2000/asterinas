// SPDX-License-Identifier: MPL-2.0

//! CPU execution context control.

use core::fmt::Debug;

use crate::{
    arch::trap::{RawUserContext, TrapFrame},
    user::{ReturnReason, UserContextApi, UserContextApiInternal},
};

/// CPU exception type.
#[derive(Clone, Copy, Debug)]
#[repr(C)]
pub enum CpuException {
    Unknown,
}

/// Userspace CPU context, including general-purpose registers and exception information.
#[repr(C)]
#[derive(Clone, Debug, Default)]
pub struct UserContext {
    user_context: RawUserContext,
    exception: Option<CpuException>,
}

/// General registers.
#[expect(missing_docs)]
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct GeneralRegs {
    pub x1: usize,
    pub x2: usize,
    pub x3: usize,
    pub x4: usize,
    pub x5: usize,
    pub x6: usize,
    pub x7: usize,
    pub x8: usize,
    pub x9: usize,
    pub x10: usize,
    pub x11: usize,
    pub x12: usize,
    pub x13: usize,
    pub x14: usize,
    pub x15: usize,
    pub x16: usize,
    pub x17: usize,
    pub x18: usize,
    pub x19: usize,
    pub x20: usize,
    pub x21: usize,
    pub x22: usize,
    pub x23: usize,
    pub x24: usize,
    pub x25: usize,
    pub x26: usize,
    pub x27: usize,
    pub x28: usize,
    pub x29: usize,
    pub __reserved: usize, // for alignment
    pub x30: usize,
    // put here deliberately for ease of asm
    pub x0: usize,
    // x31 means special
}

impl UserContext {
    /// Returns a reference to the general registers.
    pub fn general_regs(&self) -> &GeneralRegs {
        &self.user_context.general
    }

    /// Returns a mutable reference to the general registers
    pub fn general_regs_mut(&mut self) -> &mut GeneralRegs {
        &mut self.user_context.general
    }

    /// Takes the CPU exception out.
    pub fn take_exception(&mut self) -> Option<CpuException> {
        self.exception.take()
    }

    /// Sets the thread-local storage pointer.
    pub fn set_tls_pointer(&mut self, tls: usize) {
        self.user_context.tpidr = tls;
    }

    /// Gets the thread-local storage pointer.
    pub fn tls_pointer(&self) -> usize {
        self.user_context.tpidr
    }
}

impl UserContextApiInternal for UserContext {
    fn execute<F>(&mut self, mut has_kernel_event: F) -> ReturnReason
    where
        F: FnMut() -> bool,
    {
        unimplemented!()
    }

    fn as_trap_frame(&self) -> TrapFrame {
        TrapFrame {
            trap_num: self.user_context.trap_num,
            __reserved: self.user_context.__reserved,
            elr: self.user_context.elr,
            spsr: self.user_context.spsr,
            sp: self.user_context.sp,
            tpidr: self.user_context.tpidr,
            general: self.user_context.general,
        }
    }
}

impl UserContextApi for UserContext {
    fn instruction_pointer(&self) -> usize {
        self.user_context.elr
    }

    fn set_instruction_pointer(&mut self, ip: usize) {
        self.user_context.elr = ip;
    }

    fn stack_pointer(&self) -> usize {
        self.user_context.sp
    }

    fn set_stack_pointer(&mut self, sp: usize) {
        self.user_context.sp = sp;
    }
}

macro_rules! cpu_context_impl_getter_setter {
    ( $( [ $field: ident, $setter_name: ident] ),*) => {
        impl UserContext {
            $(
                #[doc = concat!("Gets the value of ", stringify!($field))]
                #[inline(always)]
                pub fn $field(&self) -> usize {
                    self.user_context.general.$field
                }

                #[doc = concat!("Sets the value of ", stringify!($field))]
                #[inline(always)]
                pub fn $setter_name(&mut self, $field: usize) {
                    self.user_context.general.$field = $field;
                }
            )*
        }
    };
}

cpu_context_impl_getter_setter!(
    [x0, set_x0],
    [x1, set_x1],
    [x2, set_x2],
    [x3, set_x3],
    [x4, set_x4],
    [x5, set_x5],
    [x6, set_x6],
    [x7, set_x7],
    [x8, set_x8],
    [x9, set_x9],
    [x10, set_x10],
    [x11, set_x11],
    [x12, set_x12],
    [x13, set_x13],
    [x14, set_x14],
    [x15, set_x15],
    [x16, set_x16],
    [x17, set_x17],
    [x18, set_x18],
    [x19, set_x19],
    [x20, set_x20],
    [x21, set_x21],
    [x22, set_x22],
    [x23, set_x23],
    [x24, set_x24],
    [x25, set_x25],
    [x26, set_x26],
    [x27, set_x27],
    [x28, set_x28],
    [x29, set_x29],
    [x30, set_x30]
);

/// The FPU context of user task.
#[derive(Clone, Debug)]
pub struct FpuContext {
    _private: (),
}

impl FpuContext {
    /// Creates a new FPU context.
    pub fn new() -> Self {
        Self { _private: () }
    }

    /// Saves CPU's current FPU context to this instance.
    pub fn save(&mut self) {}

    /// Loads CPU's FPU context from this instance.
    pub fn load(&self) {}

    /// Returns the FPU context as a byte slice.
    pub fn as_bytes(&self) -> &[u8] {
        &[]
    }

    /// Returns the FPU context as a mutable byte slice.
    pub fn as_bytes_mut(&mut self) -> &mut [u8] {
        &mut []
    }
}

impl Default for FpuContext {
    fn default() -> Self {
        Self::new()
    }
}
