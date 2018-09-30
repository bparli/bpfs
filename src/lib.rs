extern crate fuse;
extern crate libc;
extern crate time;
#[macro_use]
extern crate log;
extern crate env_logger;

use std::collections::BTreeMap;
use std::ffi::OsStr;
use libc::{ENOENT, EINVAL, EEXIST, ENOTEMPTY};
use time::Timespec;
use fuse::{FileAttr, FileType, Filesystem, Request, ReplyAttr, ReplyData, ReplyEntry, ReplyDirectory, ReplyEmpty, ReplyWrite, ReplyOpen, ReplyCreate};

const TTL: Timespec = Timespec { sec: 1, nsec: 0 };

#[derive(Clone)]
#[derive(Debug)]
pub struct MemFile {
    bytes: Vec<u8>,
}

impl MemFile {
    pub fn new(_new_bytes: &[u8]) -> MemFile {
        MemFile{bytes: _new_bytes.to_vec()}
    }
    pub fn new_empty() -> MemFile {
        MemFile{bytes: Vec::new()}
    }
    fn size(&self) -> u64 {
        self.bytes.len() as u64
    }
    fn update(&mut self, new_bytes: &[u8], offset: i64) -> u32{
        let mut counter = offset as usize;
        for &byte in new_bytes {
            self.bytes.insert(counter, byte);
            counter += 1;
        }
        new_bytes.len() as u32
    }
}

#[derive(Debug, Clone)]
pub struct Inode {
    name: String,
    children: BTreeMap<String, u64>,
    parent: u64,
}

impl Inode {
    fn new(name: String, parent: u64) -> Inode {
        Inode{name: name, children: BTreeMap::new(), parent: parent}
    }
}


pub struct  MemFilesystem {
    files:  BTreeMap<u64, MemFile>,
    attrs: BTreeMap<u64, FileAttr>,
    inodes: BTreeMap<u64, Inode>,
    next_inode: u64,
}

impl MemFilesystem {
    pub fn new() -> MemFilesystem {
        let files = BTreeMap::new();

        let root = Inode::new("/".to_string(), 1 as u64);

        let mut attrs = BTreeMap::new();
        let mut inodes = BTreeMap::new();
        let ts = time::now().to_timespec();
        let attr = FileAttr {
            ino: 1,
            size: 0,
            blocks: 0,
            atime: ts,
            mtime: ts,
            ctime: ts,
            crtime: ts,
            kind: FileType::Directory,
            perm: 0o755,
            nlink: 0,
            uid: 0,
            gid: 0,
            rdev: 0,
            flags: 0,
        };
        attrs.insert(1, attr);
        inodes.insert(1, root);
        MemFilesystem { files: files.clone(), attrs: attrs, inodes: inodes, next_inode: 2 }
    }

    fn get_next_ino(&mut self) -> u64 {
        self.next_inode += 1;
        self.next_inode
    }
}

impl Filesystem for MemFilesystem {
    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        debug!("getattr(ino={})", ino);
        match self.attrs.get(&ino) {
            Some(attr) => {
                let ttl = Timespec::new(1, 0);
                reply.attr(&ttl, attr);
            }
            None => {
                error!("getattr: inode {} is not in filesystem's attributes", ino);
                reply.error(ENOENT)
            },
        };
    }

    fn setattr(&mut self, _req: &Request, ino: u64, _mode: Option<u32>, uid: Option<u32>, gid: Option<u32>, _size: Option<u64>, atime: Option<Timespec>, mtime: Option<Timespec>, _fh: Option<u64>, crtime: Option<Timespec>, _chgtime: Option<Timespec>, _bkuptime: Option<Timespec>, _flags: Option<u32>, reply: ReplyAttr) {
        debug!("setattr(ino={})", ino);
        match self.attrs.get_mut(&ino) {
            Some(fp) => {
                match uid {
                    Some(new_uid) => fp.uid = new_uid,
                    None => {}
                }
                match gid {
                    Some(new_gid) => fp.gid = new_gid,
                    None => {}
                }
                match atime {
                    Some(new_atime) => fp.atime = new_atime,
                    None => {}
                }
                match mtime {
                    Some(new_mtime) => fp.mtime = new_mtime,
                    None => {}
                }
                match crtime {
                    Some(new_crtime) => fp.crtime = new_crtime,
                    None => {}
                }
                reply.attr(&TTL, fp);
            }
            None => {
                error!("setattr: inode {} is not in filesystem's attributes", ino);
                reply.error(ENOENT);
            }
        }
    }

    fn readdir(&mut self, _req: &Request, ino: u64, fh: u64, offset: i64, mut reply: ReplyDirectory) {
        debug!("readdir(ino={}, fh={}, offset={})", ino, fh, offset);
        let mut entries = vec![];
        entries.push((ino, FileType::Directory, "."));
        if let Some(inode) = self.inodes.get(&ino) {
            entries.push((inode.parent, FileType::Directory, ".."));
            for (child, child_ino) in &inode.children {
                let child_attrs = &self.attrs.get(child_ino).unwrap();
                debug!("\t inode={}, child={}", child_ino, child);
                entries.push((child_attrs.ino, child_attrs.kind, &child));
            }

            if entries.len() > 0 {
                // Offset of 0 means no offset.
                // Non-zero offset means the passed offset has already been seen, and we should start after
                // it.
                let to_skip = if offset == 0 { offset } else { offset + 1 } as usize;
                for (i, entry) in entries.into_iter().enumerate().skip(to_skip) {
                    reply.add(entry.0, i as i64, entry.1, entry.2);
                }
            }
            reply.ok();
        } else {
            error!("readdir: inode {} is not in filesystem's inodes", ino);
            reply.error(ENOENT)
        }
    }

    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        debug!("lookup(parent={}, name={})", parent, name.to_str().unwrap());
        match self.inodes.get(&parent) {
            Some(parent_ino) => {
                let inode = match parent_ino.children.get(name.to_str().unwrap()) {
                    Some(inode) => inode,
                    None => {
                        error!("lookup: {} is not in parent's {} children", name.to_str().unwrap(), parent);
                        reply.error(ENOENT);
                        return;
                    }
                };
                match self.attrs.get(inode) {
                    Some(attr) => {
                        reply.entry(&TTL, attr, 0);
                    }
                    None => {
                        error!("lookup: inode {} is not in filesystem's attributes", inode);
                        reply.error(ENOENT);
                    }
                };
            },
            None => {
                error!("lookup: parent inode {} is not in filesystem's attributes", parent);
                reply.error(ENOENT);
            }
        };
    }

    fn rmdir(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        debug!("rmdir(parent={}, name={})", parent, name.to_str().unwrap());
        let mut rmdir_ino = 0;
        if let Some(parent_ino) = self.inodes.get_mut(&parent) {
            match parent_ino.children.get(&name.to_str().unwrap().to_string()) {
                Some(dir_ino) => {
                    rmdir_ino = *dir_ino;
                }
                None => {
                    error!("rmdir: {} is not in parent's {} children", name.to_str().unwrap(), parent);
                    reply.error(ENOENT);
                    return;
                }
            }
        }
        if let Some(dir) = self.inodes.get(&rmdir_ino) {
            if dir.children.is_empty() {
                self.attrs.remove(&rmdir_ino);
            } else {
                reply.error(ENOTEMPTY);
                return;
            }
        }
        if let Some(parent_ino) = self.inodes.get_mut(&parent) {
            parent_ino.children.remove(&name.to_str().unwrap().to_string());
        }
        self.inodes.remove(&rmdir_ino);
        reply.ok();
    }

    fn mkdir(&mut self, _req: &Request, parent: u64, name: &OsStr, _mode: u32, reply: ReplyEntry) {
        debug!("mkdir(parent={}, name={})", parent, name.to_str().unwrap());
        let ts = time::now().to_timespec();
        let attr = FileAttr {
            ino: self.get_next_ino(),
            size: 0,
            blocks: 0,
            atime: ts,
            mtime: ts,
            ctime: ts,
            crtime: ts,
            kind: FileType::Directory,
            perm: 0o644,
            nlink: 0,
            uid: 0,
            gid: 0,
            rdev: 0,
            flags: 0,
        };

        if let Some(parent_ino) = self.inodes.get_mut(&parent) {
            debug!("parent is {} for name={}", parent_ino.name, name.to_str().unwrap());
            if parent_ino.children.contains_key(name.to_str().unwrap()) {
                reply.error(EEXIST);
                return;
            }
            parent_ino.children.insert(name.to_str().unwrap().to_string(), attr.ino);
            self.attrs.insert(attr.ino, attr);
        } else {
            error!("mkdir: parent {} is not in filesystem inodes", parent);
            reply.error(EINVAL);
            return;
        }
        self.inodes.insert(attr.ino, Inode::new(name.to_str().unwrap().to_string(), parent));
        reply.entry(&TTL, &attr, 0)
    }

    fn open(&mut self, _req: &Request, _ino: u64, _flags: u32, reply: ReplyOpen) {
        debug!("open(ino={}, _flags={})", _ino, _flags);
        reply.opened(0, 0);
    }

    fn unlink(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        debug!("unlink(_parent={}, _name={})", parent, name.to_str().unwrap().to_string());
        let mut old_ino = 0;
        if let Some(parent_ino) = self.inodes.get_mut(&parent) {
            debug!("parent is {} for name={}", parent_ino.name, name.to_str().unwrap());
            match parent_ino.children.remove(&name.to_str().unwrap().to_string()) {
                Some(ino) => {
                    match self.attrs.remove(&ino) {
                        Some(attr) => {
                            if attr.kind == FileType::RegularFile{
                                self.files.remove(&ino);
                            }
                            old_ino = ino;
                        },
                        None => {
                            old_ino = ino;
                        },
                    }
                }
                None => {
                    error!("unlink: {} is not in parent's {} children", name.to_str().unwrap(), parent);
                    reply.error(ENOENT);
                    return;
                }
            }
        };
        self.inodes.remove(&old_ino);
        reply.ok();
    }

    fn create(&mut self, _req: &Request, parent: u64, name: &OsStr, _mode: u32, _flags: u32, reply: ReplyCreate) {
        debug!("create( _parent={}, _flags={}, _name={})", parent, _flags, name.to_str().unwrap().to_string());
        let new_ino = self.get_next_ino();
        match self.inodes.get_mut(&parent) {
            Some(parent_ino) => {
                if let Some(ino) = parent_ino.children.get_mut(&name.to_str().unwrap().to_string()) {
                    reply.created(&TTL, self.attrs.get(&ino).unwrap(), 0, 0 ,0);
                    return;
                } else {
                    debug!("create file not found( _parent={}, name={})", parent, name.to_str().unwrap().to_string());
                    let ts = time::now().to_timespec();
                    let attr = FileAttr {
                        ino: new_ino,
                        size: 0,
                        blocks: 0,
                        atime: ts,
                        mtime: ts,
                        ctime: ts,
                        crtime: ts,
                        kind: FileType::RegularFile,
                        perm: 0o644,
                        nlink: 0,
                        uid: 0,
                        gid: 0,
                        rdev: 0,
                        flags: 0,
                    };
                    self.attrs.insert(attr.ino, attr);
                    self.files.insert(attr.ino, MemFile::new_empty());
                    reply.created(&TTL, &attr, 0, 0, 0);
                }
                parent_ino.children.insert(name.to_str().unwrap().to_string(), new_ino);
            }
            None => {
                error!("create: parent {} is not in filesystem's inodes", parent);
                reply.error(EINVAL);
                return;
            }
        }
        self.inodes.insert(new_ino, Inode::new(name.to_str().unwrap().to_string(), parent));
    }

    fn write(&mut self, _req: &Request, ino: u64, _fh: u64, offset: i64, data: &[u8], _flags: u32, reply: ReplyWrite) {
        debug!("write(ino={}, fh={}, offset={})", ino, _fh, offset);
        let ts = time::now().to_timespec();
        match self.files.get_mut(&ino) {
            Some(fp) => {
                let size = fp.update(data, offset);
                match self.attrs.get_mut(&ino) {
                    Some(attr) => {
                        attr.atime = ts;
                        attr.mtime = ts;
                        attr.size = fp.size();
                        reply.written(size);
                    }
                    None => {
                        error!("write: ino {} is not in filesystem's attributes", ino);
                        reply.error(ENOENT);
                    }
                }

            }
            None => reply.error(ENOENT),
        }
    }

    fn read(&mut self, _req: &Request, ino: u64, fh: u64, offset: i64, size: u32, reply: ReplyData) {
        debug!("read(ino={}, fh={}, offset={}, size={})", ino, fh, offset, size);
        match self.files.get(&ino) {
            Some(fp) => {
                reply.data(&fp.bytes[offset as usize..]);
            }
            None => {
                reply.error(ENOENT);
            }
        }
    }

    /// Rename a file.
    fn rename(&mut self, _req: &Request, parent: u64, name: &OsStr, newparent: u64, newname: &OsStr, reply: ReplyEmpty) {
        debug!("rename(parent={}, name={}, newparent={}, newname={})", parent, name.to_str().unwrap().to_string(), newparent, newname.to_str().unwrap().to_string());
        if self.inodes.contains_key(&parent) && self.inodes.contains_key(&newparent) {
            let file_ino;
            match self.inodes.get_mut(&parent) {
                Some(parent_ino) => {
                    if let Some(ino) = parent_ino.children.remove(&name.to_str().unwrap().to_string()) {
                        file_ino = ino;
                    } else {
                        error!("{} not found in parent {}", name.to_str().unwrap().to_string(), parent);
                        reply.error(ENOENT);
                        return;
                    }
                }
                None => {
                    error!("rename: parent {} is not in filesystem inodes", parent);
                    reply.error(EINVAL);
                    return;
                }
            }
            if let Some(newparent_ino) = self.inodes.get_mut(&newparent) {
                newparent_ino.children.insert(newname.to_str().unwrap().to_string(), file_ino);

            }
        }
        reply.ok();
    }
}
