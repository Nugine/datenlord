use std::mem;
use std::ptr::{self, NonNull};

const NR_INLINE_PTR: usize = 7;

pub struct InlineStorage {
    storage: [usize; NR_INLINE_PTR],
    drop: unsafe fn(*mut ()),
}

unsafe impl Send for InlineStorage {}
unsafe impl Sync for InlineStorage {}

unsafe fn drop_data<T: Send>(p: *mut ()) {
    ptr::drop_in_place(p.cast::<T>())
}

impl InlineStorage {
    const SIZE: usize = mem::size_of::<usize>() * NR_INLINE_PTR;

    pub fn empty() -> Self {
        Self {
            storage: [0; NR_INLINE_PTR],
            drop: drop_data::<()>,
        }
    }

    pub fn clear(&mut self) {
        unsafe { ptr::drop_in_place(self) };
        self.drop = drop_data::<()>;
    }

    pub fn put<T: Send>(&mut self, data: T) -> *mut T {
        assert!(mem::align_of::<usize>() % mem::align_of::<T>() == 0);

        unsafe {
            ptr::drop_in_place(self);

            if mem::size_of::<T>() <= Self::SIZE {
                let data_ptr = self.storage.as_mut_ptr().cast();
                ptr::write(data_ptr, data); // <- memcpy here: stack to stack or heap
                self.drop = drop_data::<T>;
                data_ptr
            } else {
                let data_ptr = Box::into_raw(Box::new(data)); // <- memcpy here: stack to heap
                self.storage[0] = data_ptr as usize;
                self.drop = drop_data::<Box<T>>;
                data_ptr
            }
        }
    }
}

impl Drop for InlineStorage {
    fn drop(&mut self) {
        unsafe { (self.drop)(self.storage.as_mut_ptr().cast()) }
    }
}
