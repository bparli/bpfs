# bpfs

## About
bpfs is a an in-memory filesystem written in Rust and built on the [Rust-FUSE](https://github.com/zargony/rust-fuse) library

## Usage
The program takes one argument; the location for the mount.  To mount the filesystem at /tmp/bpfs directory:

``./bpfs /tmp/bpfs`` or 
``cargo run /tmp/bpfs/``

To unmount a filesystem, use any arbitrary unmount/eject method of your OS.

## To-Do
File permissions and ownership aren't implemented
