// SPDX-License-Identifier: MPL-2.0

//! [`Local`] storage for `dyn` objects.
//!
//! Using the [`Local`] type allows one to store a `dyn` object whose size does not exceed `N`
//! bytes (the default value of `N` is 8) in a `N`-byte local storage, thus avoiding unnecessary
//! heap allocation.
//!
//! # Examples
//!
//! Here are two major use cases:
//!
//! ## Pass closure instances without (requiring) heap allocation
//!
//! If a method wants to accept a closure and persist the closure in a global, it may use `Box<dyn
//! Fn()>` or `Box<dyn FnOnce()>`, which invokes the heap allocation. Now, with the help of
//! [`Local`], it can accept a `Local<dyn Fn()>` or `Local<dyn FnOnceDyn()>`.
//! ```
//! # use spin::Once;
//! #
//! static CALLBACK: Once<Local<dyn Fn>>
//!
//! fn register_callback(f: Local<dyn Fn()>) {
//!     CALLBACK.call_once(|| f);
//! }
//! ```
//!
//! Then, it is up to the caller to decide whether heap allocation is needed: if the closure
//! captures a variable whose size is smaller than 8 bytes, the caller can directly put the
//! closure in the local storage.
//! ```
//! # use ostd::prelude::*;
//! #
//! # fn register_callback(_f: Local<dyn Fn()>) {}
//! #
//! fn caller(val: u32) {
//!     register_callback(Local::new(|| println!("val = {}", val)));
//! }
//! ```
//!
//! Otherwise, the caller can wrap the variables to be captured into a `Box` and let the closure
//! to capture the `Box` (whose size is 8 bytes).
//! ```
//! # use ostd::prelude::*;
//! #
//! # fn register_callback(_f: Local<dyn Fn()>) {}
//! #
//! fn caller(val1: u64, val2: u64) {
//!     let vals = Box::new((val1, val2));
//!     register_callback(Local::new(move || println!("vals = {:?}", *vals)));
//! }
//! ```
//!
//! ## Pass `dyn` instances without (requiring) reference counting
//!
//! If a method wants to accept a `dyn` object but it does not know whether the caller wants to
//! reference-count the object, it may just use `Arc`, which forces the caller to reference-count
//! the object, even if it is not necessary. Now, the method can accept a [`Local`] instance.
//! ```
//! # use spin::Once;
//! #
//! trait Handler {
//!     fn handle_event(&self);
//! }
//!
//! static HANDLER: Once<Local<dyn Handler>> = Once::new();
//!
//! fn register_handler(handler: Local<dyn Handler>) {
//!     HANDLER.call_once(|| handler);
//! }
//! ```
//!
//! Then, if it is up to the caller to decide what exactly the type is. Specifically, it can choose
//! one of the following:
//!
//! * With shared (reference-counted) data.
//! ```
//! # trait Handler {
//! #     fn handle_event(&self);
//! # }
//! # fn register_handler(_handler: Local<dyn Handler>) {}
//!
//! struct SharedData;
//! struct SharedHandler(Arc<SharedData>);
//! impl Handler for SharedHandler {
//!     fn handle_event(&self) {}
//! }
//!
//! fn caller(data: Arc<SharedData>) {
//!     register_handler(Local::new(SharedHandler(data)));
//! }
//! ```
//!
//! * With exclusive data.
//! ```
//! # trait Handler {
//! #     fn handle_event(&self);
//! # }
//! # fn register_handler(_handler: Local<dyn Handler>) {}
//!
//! struct ExclusiveData;
//! struct ExclusiveHandler(Box<ExclusiveData>);
//! impl Handler for ExclusiveHandler {
//!     fn handle_event(&self) {}
//! }
//!
//! fn caller(data: Box<ExclusiveData>) {
//!     register_handler(Local::new(ExclusiveHandler(data)));
//! }
//! ```
//!
//! * Without any data.
//! ```
//! # trait Handler {
//! #     fn handle_event(&self);
//! # }
//! # fn register_handler(_handler: Local<dyn Handler>) {}
//!
//! struct UnitHandler;
//! impl Handler for UnitHandler {
//!     fn handle_event(&self) {}
//! }
//!
//! fn caller() {
//!     register_handler(Local::new(UnitHandler));
//! }
//! ```

use core::{
    clone::CloneToUninit,
    marker::Unsize,
    mem::{ManuallyDrop, MaybeUninit},
    ops::{Deref, DerefMut},
    ptr,
};

/// A `dyn` object (`T`) in the `N`-byte _local_ storage.
///
/// For example, we can use `Local<dyn Fn(u32) -> usize>` to create a closure that captures a small
/// variable (as long as its size in bytes is less than `N`), pass it to another method, and store
/// it in a global variable. All of them can be done without invoking heap allocation at all.
pub struct Local<T: ?Sized, const N: usize = 8> {
    data: MaybeUninit<AlignedBytes<N>>,
    meta: <T as ptr::Pointee>::Metadata,
}

#[repr(align(8))]
struct AlignedBytes<const N: usize>([u8; N]);

impl<T: ?Sized, const N: usize> Local<T, N> {
    /// Creates an instance with `val` as the object.
    ///
    /// If the size of the value is greater than `N` or the alignment is greater than `8`, this
    /// method will fail a compile-time assertion and lead to compiler errors.
    pub fn new<U>(val: U) -> Self
    where
        U: Unsize<T>,
    {
        let meta = ptr::metadata(&val as &T);

        const {
            assert!(size_of::<U>() <= size_of::<AlignedBytes<N>>());
            assert!(align_of::<U>() <= align_of::<AlignedBytes<N>>());
        }

        let mut data = MaybeUninit::<AlignedBytes<N>>::uninit();
        // SAFETY: The storage is valid to write. We've checked the size and the alignment above.
        unsafe { data.as_mut_ptr().cast::<U>().write(val) };

        Self { data, meta }
    }
}

impl<T: ?Sized, const N: usize> Deref for Local<T, N> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        let val = ptr::from_raw_parts(self.data.as_ptr(), self.meta);
        // SAFETY:
        // 1. The storage can be immutably borrowed because of `&self`.
        // 2. The storage contains a valid instance of `T` because of `Self::new`.
        // 3. The storage is properly aligned because of `Self::new`.
        unsafe { &*val }
    }
}

impl<T: ?Sized, const N: usize> DerefMut for Local<T, N> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        let val = ptr::from_raw_parts_mut(self.data.as_mut_ptr(), self.meta);
        // SAFETY:
        // 1. The storage can be mutably borrowed because of `&mut self`.
        // 2. The storage contains a valid instance of `T` because of `Self::new`.
        // 3. The storage is properly aligned because of `Self::new`.
        unsafe { &mut *val }
    }
}

impl<T: ?Sized, const N: usize> Drop for Local<T, N> {
    fn drop(&mut self) {
        let to_drop: *mut T = ptr::from_raw_parts_mut(self.data.as_mut_ptr(), self.meta);
        // SAFETY:
        // 1. The storage can be mutably borrowed because of `&mut self`.
        // 2. The storage contains a valid instance of `T` because of `Self::new`.
        // 3. The storage is properly aligned because of `Self::new`.
        // 4. The value in the storage won't be accessed afterwards because we're in `Drop::drop`.
        unsafe { ptr::drop_in_place(to_drop) };
    }
}

// SAFETY: `Local<T, N>` contains a `T` and behaves exactly like a `T`.
unsafe impl<T: Send + ?Sized, const N: usize> Send for Local<T, N> {}
unsafe impl<T: Sync + ?Sized, const N: usize> Sync for Local<T, N> {}

// We cannot implement `Clone` for `Local` directly because `Clone` is not a dyn-compatible trait.
// But we can do so indirectly with the help of the `CloneToUninit` trait.
impl<T: CloneToUninit + ?Sized, const N: usize> Clone for Local<T, N> {
    fn clone(&self) -> Self {
        let mut data = MaybeUninit::<AlignedBytes<N>>::uninit();
        // SAFETY: The storage is valid to write. `Self::new` has checked the size and alignment.
        unsafe { (**self).clone_to_uninit(data.as_mut_ptr().cast()) };

        Self {
            data,
            meta: self.meta,
        }
    }
}

// We cannot implement `call_once` for `Local` directly because `FnOnce` is not a dyn-compatible
// trait. But we can do so indirectly with the help of the `FnOnceDyn` trait.
impl<T: ?Sized, const N: usize> Local<T, N> {
    /// Consumes and invokes the underlying [`FnOnce`] closure.
    pub fn call_once<Arg, Output>(self, arg: Arg) -> Output
    where
        T: FnOnceDyn<Arg, Output = Output>,
    {
        let mut this = ManuallyDrop::new(self);
        // SAFETY: `this` is in `ManuallyDrop` and it won't be accessed or dropped again.
        unsafe { (**this).call_once(arg) }
    }
}

/// A trait that can act as `FnOnce(Arg) -> Output`, but is dyn-compatible.
///
/// [`FnOnce`] provides a safe [`FnOnce::call_once`], which takes the ownership of `self`, so the
/// trait is not a dyn-compatible trait. In contrast, this trait provides an unsafe
/// [`Self::call_once`], which does not take the ownership of `self` and shifts the responsibility
/// for avoiding double drops to the caller, but the trait is a dyn-compatible trait.
///
/// # Safety
///
/// The implementors must implement methods in this trait correctly.
pub unsafe trait FnOnceDyn<Arg> {
    /// The return type of the [`FnOnce`] closure.
    type Output;

    /// Consumes and invokes the underlying [`FnOnce`] closure.
    ///
    /// This method behaves like [`ManuallyDrop::drop`], which takes `&mut self` but consumes
    /// `self`.
    ///
    /// # Safety
    ///
    /// This method will drop `self`, so it is the caller's responsibility to guarantee that `self`
    /// won't be accessed or dropped again after this method returns or this method panics.
    unsafe fn call_once(&mut self, arg: Arg) -> Self::Output;
}

unsafe impl<Arg, Output, T: FnOnce(Arg) -> Output> FnOnceDyn<Arg> for T {
    type Output = Output;

    unsafe fn call_once(&mut self, arg: Arg) -> Self::Output {
        // SAFETY:
        // 1. `self` is valid to read and contains a valid instance of `T`.
        // 2. The caller guarantees that `self` won't be accessed or dropped after this method.
        let func = unsafe { ptr::read(self) };
        func(arg)
    }
}

#[cfg(ktest)]
mod tests {
    use super::*;
    use crate::prelude::*;

    /// Tests `Local<dyn Fn>`.
    #[ktest]
    fn fn_from_u32() {
        fn make_fn_from_u32(val: u32) -> Local<dyn Fn() -> u32> {
            Local::new(move || val + 123)
        }

        let f = make_fn_from_u32(321);
        let v = f();
        assert_eq!(v, 444);
    }

    /// Tests `Local<dyn FnOnceDyn>`.
    #[ktest]
    fn fn_once_from_box() {
        use alloc::boxed::Box;

        fn make_fn_once_from_box(val: Box<(usize, u64)>) -> Local<dyn FnOnceDyn<(), Output = u64>> {
            Local::new(move |_| val.0 as u64 + val.1)
        }

        let f = make_fn_once_from_box(Box::new((111, 222)));
        let v = f.call_once(());
        assert_eq!(v, 333);
    }

    trait Greet: CloneToUninit {
        fn greet(&self) -> usize;
    }

    /// Tests `Local<dyn Greet>`, where the actual object is a unit type.
    #[ktest]
    fn dyn_unit() {
        use core::sync::atomic::{AtomicUsize, Ordering};

        static COUNT: AtomicUsize = AtomicUsize::new(0);
        struct Data;
        impl Data {
            fn new() -> Self {
                COUNT.fetch_add(1, Ordering::Relaxed);
                Data
            }
        }
        impl Clone for Data {
            fn clone(&self) -> Self {
                Self::new()
            }
        }
        impl Drop for Data {
            fn drop(&mut self) {
                COUNT.fetch_sub(1, Ordering::Relaxed);
            }
        }

        impl Greet for Data {
            fn greet(&self) -> usize {
                0xdeadbeef
            }
        }

        let local: Local<dyn Greet> = Local::new(Data::new());
        assert_eq!(COUNT.load(Ordering::Relaxed), 1);
        assert_eq!(local.greet(), 0xdeadbeef);

        let local2 = local.clone();
        assert_eq!(COUNT.load(Ordering::Relaxed), 2);
        assert_eq!(local2.greet(), 0xdeadbeef);

        drop(local);
        assert_eq!(COUNT.load(Ordering::Relaxed), 1);

        drop(local2);
        assert_eq!(COUNT.load(Ordering::Relaxed), 0);
    }

    /// Tests `Local<dyn Greet>`, where the actual object is an `Arc`.
    #[ktest]
    fn dyn_arc() {
        use alloc::sync::Arc;

        #[derive(Clone)]
        #[expect(dead_code)]
        struct Data(Arc<()>);

        impl Greet for Data {
            fn greet(&self) -> usize {
                0xbeefdead
            }
        }

        let arc = Arc::new(());
        let data = Data(arc.clone());

        let local: Local<dyn Greet> = Local::new(data);
        assert_eq!(Arc::strong_count(&arc), 2);
        assert_eq!(local.greet(), 0xbeefdead);

        let local2 = local.clone();
        assert_eq!(Arc::strong_count(&arc), 3);
        assert_eq!(local2.greet(), 0xbeefdead);

        drop(local2);
        assert_eq!(Arc::strong_count(&arc), 2);

        drop(local);
        assert_eq!(Arc::strong_count(&arc), 1);
    }
}
