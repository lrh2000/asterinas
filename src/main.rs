#[expect(dead_code)]
mod bytemuck_ext {
    use bytemuck::TransparentWrapper;
    use std::sync::Weak;

    /// A macro to transmute between two types without requiring knowing size
    /// statically.
    macro_rules! transmute {
        ($val:expr) => {
            ::core::mem::transmute_copy(&::core::mem::ManuallyDrop::new($val))
        };
        // This arm is for use in const contexts, where the borrow required to use
        // transmute_copy poses an issue since the compiler hedges that the type
        // being borrowed could have interior mutability.
        ($srcty:ty; $dstty:ty; $val:expr) => {{
            #[repr(C)]
            union Transmute<A, B> {
                src: ::core::mem::ManuallyDrop<A>,
                dst: ::core::mem::ManuallyDrop<B>,
            }
            ::core::mem::ManuallyDrop::into_inner(
                Transmute::<$srcty, $dstty> {
                    src: ::core::mem::ManuallyDrop::new($val),
                }
                .dst,
            )
        }};
    }

    /// An extension trait for `TransparentWrapper` and alloc types.
    pub trait TransparentWrapperExt<Inner: ?Sized>: TransparentWrapper<Inner> {
        /// Convert an [`Weak`] to the inner type into an `Weak` to the wrapper type.
        #[inline]
        #[cfg(target_has_atomic = "ptr")]
        fn wrap_weak(s: Weak<Inner>) -> Weak<Self> {
            // The unsafe contract requires that these two have
            // identical representations, and thus identical pointer metadata.
            // Assert that Self and Inner have the same pointer size,
            // which is the best we can do to assert their metadata is the same type
            // on stable.
            assert!(size_of::<*mut Inner>() == size_of::<*mut Self>());

            unsafe {
                // A pointer cast doesn't work here because rustc can't tell that
                // the vtables match (because of the `?Sized` restriction relaxation).
                // A `transmute` doesn't work because the layout of Weak is unspecified.
                //
                // SAFETY:
                // * The unsafe contract requires that pointers to Inner and Self have
                //   identical representations, and that the size and alignment of Inner
                //   and Self are the same, which meets the safety requirements of
                //   Weak::from_raw
                let inner_ptr: *const Inner = Weak::into_raw(s);
                let wrapper_ptr: *const Self = transmute!(inner_ptr);
                Weak::from_raw(wrapper_ptr)
            }
        }

        /// Convert an [`Weak`] to the wrapper type into an `Weak` to the inner type.
        #[inline]
        #[cfg(target_has_atomic = "ptr")]
        fn peel_weak(s: Weak<Self>) -> Weak<Inner> {
            // The unsafe contract requires that these two have
            // identical representations, and thus identical pointer metadata.
            // Assert that Self and Inner have the same pointer size,
            // which is the best we can do to assert their metadata is the same type
            // on stable.
            assert!(size_of::<*mut Inner>() == size_of::<*mut Self>());

            unsafe {
                // A pointer cast doesn't work here because rustc can't tell that
                // the vtables match (because of the `?Sized` restriction relaxation).
                // A `transmute` doesn't work because the layout of Weak is unspecified.
                //
                // SAFETY:
                // * The unsafe contract requires that pointers to Inner and Self have
                //   identical representations, and that the size and alignment of Inner
                //   and Self are the same, which meets the safety requirements of
                //   Weak::from_raw
                let wrapper_ptr: *const Self = Weak::into_raw(s);
                let inner_ptr: *const Inner = transmute!(wrapper_ptr);
                Weak::from_raw(inner_ptr)
            }
        }
    }

    impl<I: ?Sized, T: ?Sized + TransparentWrapper<I>> TransparentWrapperExt<I> for T {}
}

mod aster_systree {
    use std::{
        collections::HashMap,
        sync::{Arc, RwLock, Weak},
    };

    use bytemuck::{TransparentWrapper, TransparentWrapperAlloc};

    // ================================ Basics

    pub trait SysNode {
        fn name(&self) -> &str;
        #[expect(dead_code)]
        fn as_node(&self) -> Option<Arc<dyn SysNode>>;
        fn as_branch(&self) -> Option<Arc<dyn SysBranch>>;
    }

    pub trait SysBranch: SysNode {
        #[expect(dead_code)]
        fn get_child(&self, name: &str) -> Option<Arc<dyn SysNode>>;
        fn show_attr(&self, name: &str) -> Option<String>;
    }

    // ================================ Branch nodes

    pub struct BranchFields<Self_, Child: ?Sized> {
        name: String,
        children: RwLock<HashMap<String, Arc<Child>>>,
        weak_self: Weak<Self_>,
    }

    impl<Self_, Child: ?Sized> BranchFields<Self_, Child> {
        pub fn new(name: String, weak_self: Weak<Self_>) -> Self {
            Self {
                name,
                children: RwLock::new(HashMap::new()),
                weak_self,
            }
        }
    }

    pub trait HasBranchFields: Sized + 'static {
        type Child: ToSysNode + ?Sized;

        fn branch_fields(&self) -> &BranchFields<Self, Self::Child>;
        fn show_attr(&self, name: &str) -> Option<String>;

        fn wrap(self: Arc<Self>) -> Arc<BranchNode<Self>> {
            BranchNode::wrap_arc(self)
        }
    }

    #[repr(transparent)]
    #[derive(TransparentWrapper)]
    pub struct BranchNode<T>(T);

    impl<T: HasBranchFields> SysNode for BranchNode<T> {
        fn name(&self) -> &str {
            &self.0.branch_fields().name
        }

        fn as_node(&self) -> Option<Arc<dyn SysNode>> {
            let branch_fields = self.0.branch_fields();
            Some(Self::wrap_arc(branch_fields.weak_self.upgrade().unwrap()) as _)
        }

        fn as_branch(&self) -> Option<Arc<dyn SysBranch>> {
            let branch_fields = self.0.branch_fields();
            Some(Self::wrap_arc(branch_fields.weak_self.upgrade().unwrap()) as _)
        }
    }

    impl<T: HasBranchFields> SysBranch for BranchNode<T> {
        fn get_child(&self, name: &str) -> Option<Arc<dyn SysNode>> {
            let children = self.0.branch_fields().children.read().ok()?;
            Some(children.get(name)?.clone().to_node())
        }

        fn show_attr(&self, name: &str) -> Option<String> {
            self.0.show_attr(name)
        }
    }

    pub trait ToSysNode {
        fn to_node(self: Arc<Self>) -> Arc<dyn SysNode>;
    }

    impl<T: SysNode + 'static> ToSysNode for T {
        fn to_node(self: Arc<Self>) -> Arc<dyn SysNode> {
            self as _
        }
    }

    impl ToSysNode for dyn SysNode + 'static {
        fn to_node(self: Arc<Self>) -> Arc<dyn SysNode> {
            self
        }
    }
}

// ================================ Device nodes

mod aster_device {
    use std::sync::{Arc, Weak};

    use bytemuck::{TransparentWrapper, TransparentWrapperAlloc};

    use crate::{
        aster_systree::{BranchFields, HasBranchFields, SysNode, ToSysNode},
        bytemuck_ext::TransparentWrapperExt,
    };

    pub struct DeviceCommon<Self_> {
        base: BranchFields<DeviceNode<Self_>, dyn SysNode>,
        dev: Option<(u16, u16)>,
    }

    impl<Self_> DeviceCommon<Self_> {
        pub fn new(name: String, weak_self: Weak<Self_>) -> Self {
            Self::__new(name, weak_self, None)
        }

        pub fn new_char(name: String, weak_self: Weak<Self_>, major: u16, minor: u16) -> Self {
            Self::__new(name, weak_self, Some((major, minor)))
        }

        fn __new(name: String, weak_self: Weak<Self_>, dev: Option<(u16, u16)>) -> Self {
            let base = BranchFields::new(name, DeviceNode::wrap_weak(weak_self));
            Self { base, dev }
        }
    }

    pub trait HasDeviceCommon: Sized + 'static {
        fn device_common(&self) -> &DeviceCommon<Self>;
        fn show_attr(&self, name: &str) -> Option<String>;

        fn wrap(self: Arc<Self>) -> Arc<DeviceNode<Self>> {
            DeviceNode::wrap_arc(self)
        }
    }

    #[repr(transparent)]
    #[derive(TransparentWrapper)]
    pub struct DeviceNode<T>(T);

    impl<T: HasDeviceCommon> HasBranchFields for DeviceNode<T> {
        type Child = dyn SysNode;

        fn branch_fields(&self) -> &BranchFields<Self, Self::Child> {
            &self.0.device_common().base
        }

        fn show_attr(&self, name: &str) -> Option<String> {
            let device_common = self.0.device_common();
            #[expect(clippy::single_match)]
            match name {
                "dev" => {
                    return device_common
                        .dev
                        .map(|(major, minor)| format!("{}:{}", major, minor));
                }
                _ => (),
            }

            self.0.show_attr(name)
        }
    }

    impl<T: HasDeviceCommon> ToSysNode for DeviceNode<T> {
        fn to_node(self: Arc<Self>) -> Arc<dyn SysNode> {
            self.wrap().to_node()
        }
    }
}

// ================================ Input device nodes

mod aster_input {
    use std::sync::{Arc, Weak};

    use crate::{
        aster_device::{DeviceCommon, HasDeviceCommon},
        aster_systree::{SysNode, ToSysNode},
        bytemuck_ext::TransparentWrapperExt,
    };

    use bytemuck::{TransparentWrapper, TransparentWrapperAlloc};

    pub struct InputDeviceCommon<Self_> {
        base: DeviceCommon<InputDeviceNode<Self_>>,
        name: String,
        phys: String,
        uniq: String,
    }

    impl<Self_> InputDeviceCommon<Self_> {
        pub fn new(
            name: String,
            weak_self: Weak<Self_>,
            input_name: String,
            input_phys: String,
            input_uniq: String,
        ) -> Self {
            let base = DeviceCommon::new(name, InputDeviceNode::wrap_weak(weak_self));
            Self {
                base,
                name: input_name,
                phys: input_phys,
                uniq: input_uniq,
            }
        }
    }

    pub trait HasInputDeviceCommon: Sized + 'static {
        fn input_device_common(&self) -> &InputDeviceCommon<Self>;
        fn show_attr(&self, name: &str) -> Option<String>;

        fn wrap(self: Arc<Self>) -> Arc<InputDeviceNode<Self>> {
            InputDeviceNode::wrap_arc(self)
        }
    }

    #[repr(transparent)]
    #[derive(TransparentWrapper)]
    pub struct InputDeviceNode<T>(T);

    impl<T: HasInputDeviceCommon> HasDeviceCommon for InputDeviceNode<T> {
        fn device_common(&self) -> &DeviceCommon<Self> {
            &self.0.input_device_common().base
        }

        fn show_attr(&self, name: &str) -> Option<String> {
            let input_device_common = self.0.input_device_common();
            match name {
                "name" => return Some(input_device_common.name.clone()),
                "phys" => return Some(input_device_common.phys.clone()),
                "uniq" => return Some(input_device_common.uniq.clone()),
                _ => (),
            }

            self.0.show_attr(name)
        }
    }

    impl<T: HasInputDeviceCommon> ToSysNode for InputDeviceNode<T> {
        fn to_node(self: Arc<Self>) -> Arc<dyn SysNode> {
            self.wrap().to_node()
        }
    }
}

// ================================ Examples

mod examples {
    use std::sync::Arc;

    use crate::{
        aster_device::{DeviceCommon, HasDeviceCommon},
        aster_input::{HasInputDeviceCommon, InputDeviceCommon},
        aster_systree::{SysNode, ToSysNode},
    };

    fn register_node(node: Arc<dyn SysNode>) {
        println!(" - registered node '{}'", node.name());

        let Some(branch) = node.as_branch() else {
            return;
        };

        for attr in ["dev", "name", "phys", "uniq", "my_attr"] {
            if let Some(val) = branch.show_attr(attr) {
                println!("   * attribute '{}' = '{}'", attr, val);
            }
        }
    }

    struct MyDevice {
        base: DeviceCommon<Self>,
    }

    impl MyDevice {
        fn new() -> Arc<Self> {
            Arc::new_cyclic(|weak_self| {
                let base =
                    DeviceCommon::new_char("my_device".to_owned(), weak_self.clone(), 10, 20);
                Self { base }
            })
        }
    }

    impl HasDeviceCommon for MyDevice {
        fn device_common(&self) -> &DeviceCommon<Self> {
            &self.base
        }

        fn show_attr(&self, name: &str) -> Option<String> {
            match name {
                "my_attr" => Some("my_val1".to_owned()),
                _ => None,
            }
        }
    }

    struct MyInputDevice {
        base: InputDeviceCommon<Self>,
    }

    impl MyInputDevice {
        fn new() -> Arc<Self> {
            Arc::new_cyclic(|weak_self| {
                let base = InputDeviceCommon::new(
                    "input3".to_owned(),
                    weak_self.clone(),
                    "AT Translated Set 2 keyboard".to_owned(),
                    "isa0060/serio0/input0".to_owned(),
                    "".to_owned(),
                );
                Self { base }
            })
        }
    }

    impl HasInputDeviceCommon for MyInputDevice {
        fn input_device_common(&self) -> &InputDeviceCommon<Self> {
            &self.base
        }

        fn show_attr(&self, _name: &str) -> Option<String> {
            None
        }
    }

    pub fn main() {
        let my_device = MyDevice::new();
        register_node(my_device.wrap().to_node());

        let my_input_device = MyInputDevice::new();
        register_node(my_input_device.wrap().to_node());
    }
}

fn main() {
    examples::main();
}
