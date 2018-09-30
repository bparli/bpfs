extern crate fuse;
extern crate libc;
extern crate time;
extern crate bpfs;

use bpfs::MemFilesystem;
use std::env;


fn main() {
    let fs = MemFilesystem::new();
    let mountpoint = match env::args().nth(1) {
        Some(path) => path,
        None => {
            println!("Usage: {} <MOUNTPOINT>", env::args().nth(0).unwrap());
            return;
        }
    };
    fuse::mount(fs, &mountpoint, &[]).unwrap();
}
