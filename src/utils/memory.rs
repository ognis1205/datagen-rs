use once_cell::sync::Lazy;
use rlimit::getrlimit;
use rlimit::Resource;
use std::cell::RefCell;
use std::cmp;
use std::io;
use std::sync::Mutex;
use std::u64;
use sysinfo::{System, SystemExt};

static SYSTEM: Lazy<Mutex<RefCell<System>>> = Lazy::new(|| {
    let mut sys: System = System::new_all();
    sys.refresh_all();
    Mutex::new(RefCell::new(sys))
});

fn get_memory_info() -> (u64, u64) {
    let mtx = SYSTEM.lock().unwrap();
    let sys = mtx.borrow_mut();
    (sys.total_memory() - sys.used_memory(), sys.total_memory())
}

fn get_resource_limit_as() -> io::Result<u64> {
    let lim = getrlimit(Resource::AS)?;
    Ok(lim.0)
}

fn get_resource_limit_data() -> io::Result<u64> {
    let lim = getrlimit(Resource::DATA)?;
    Ok(lim.0)
}

fn get_resource_limit_rss() -> io::Result<u64> {
    let lim = getrlimit(Resource::RSS)?;
    Ok(lim.0)
}

pub fn get_default_sort_size(min_sort_size: u64) -> u64 {
    let mut size = u64::MAX;
    if let Ok(limit) = get_resource_limit_data() {
        size = cmp::min(size, limit);
    };
    if let Ok(limit) = get_resource_limit_as() {
        size = cmp::min(size, limit);
    };
    size /= 2;
    if let Ok(limit) = get_resource_limit_rss() {
        size = cmp::min(size, limit / 16 * 15);
    };
    let (available_memory, total_memory) = get_memory_info();
    let memory = cmp::max(available_memory, total_memory / 8);
    if total_memory / 4 * 3 < size {
        size = total_memory / 4 * 3;
    }
    if memory < size {
        size = memory;
    }
    cmp::max(size, min_sort_size)
}
