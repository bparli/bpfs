extern crate fuse;
extern crate libc;
extern crate time;
extern crate bpfs;
#[macro_use]
extern crate log;
extern crate env_logger;

use bpfs::MemFilesystem;
use std::env;

fn main() {
    env_logger::init();
    let fs = MemFilesystem::new();
    let mountpoint = match env::args().nth(1) {
        Some(path) => path,
        None => {
            error!("Usage: {} <MOUNTPOINT>", env::args().nth(0).unwrap());
            return;
        }
    };
    fuse::mount(fs, &mountpoint, &[]).unwrap();
}
