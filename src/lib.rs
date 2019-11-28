extern crate fuse;
extern crate libc;
extern crate time;
#[macro_use]
extern crate log;
extern crate env_logger;
extern crate threadpool;

use threadpool::ThreadPool;
use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::iter;
use libc::{ENOENT, EINVAL, EEXIST, ENOTEMPTY};
use time::Timespec;
use fuse::{FileAttr, FileType, Filesystem, Request,
    ReplyAttr, ReplyData, ReplyEntry, ReplyDirectory,
    ReplyEmpty, ReplyWrite, ReplyOpen, ReplyCreate};

const TTL: Timespec = Timespec { sec: 1, nsec: 0 };

#[derive(Clone)]
#[derive(Debug)]
pub struct MemFile {
    bytes: Vec<u8>,
}

impl MemFile {
    pub fn new() -> MemFile {
        MemFile{bytes: Vec::new()}
    }
    fn size(&self) -> u64 {
        self.bytes.len() as u64
    }
    fn update(&mut self, new_bytes: &[u8], offset: i64) -> u64{
        let offset: usize = offset as usize;

       if offset >= self.bytes.len() {
           // extend with zeroes until we are at least at offset
           self.bytes.extend(iter::repeat(0).take(offset - self.bytes.len()));
       }

       if offset + new_bytes.len() > self.bytes.len() {
           self.bytes.splice(offset.., new_bytes.iter().cloned());
       } else {
           self.bytes.splice(offset..offset + new_bytes.len(), new_bytes.iter().cloned());
       }
        debug!("update(): len of new bytes is {}, total len is {}, offset was {}",
            new_bytes.len(), self.size(), offset);
        new_bytes.len() as u64
    }
    fn truncate(&mut self, size: u64) {
        self.bytes.truncate(size as usize);
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
    workers: Option<ThreadPool>,
}

impl MemFilesystem {
    pub fn new(num_workers: usize) -> MemFilesystem {
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
            perm: 0o777,
            nlink: 0,
            uid: 0,
            gid: 0,
            rdev: 0,
            flags: 0,
        };
        attrs.insert(1, attr);
        inodes.insert(1, root);

        let mut workers = None;
        if num_workers > 0 {
            workers= Some(ThreadPool::new(num_workers))
        }

        MemFilesystem { files: files, attrs: attrs, inodes: inodes, next_inode: 2, workers: workers }
    }

    fn get_next_ino(&mut self) -> u64 {
        self.next_inode += 1;
        self.next_inode
    }

    fn run_worker<F: FnOnce() + Send + 'static>(&mut self, f: F) {
        if self.workers.is_none() {
            f()
        } else {
            self.workers.as_ref().unwrap().execute(f);
        }
    }
}

impl Filesystem for MemFilesystem {
    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        debug!("getattr(ino={})", ino);
        match self.attrs.get_mut(&ino) {
            Some(attr) => {
                reply.attr(&TTL, attr);
            }
            None => {
                error!("getattr: inode {} is not in filesystem's attributes", ino);
                reply.error(ENOENT)
            },
        };
    }

    fn setattr(&mut self, _req: &Request, ino: u64, _mode: Option<u32>, uid: Option<u32>, gid: Option<u32>, size: Option<u64>, atime: Option<Timespec>, mtime: Option<Timespec>, _fh: Option<u64>, crtime: Option<Timespec>, _chgtime: Option<Timespec>, _bkuptime: Option<Timespec>, _flags: Option<u32>, reply: ReplyAttr) {
        debug!("setattr(ino={})", ino);
        match self.attrs.get_mut(&ino) {
            Some(fp) => {
                match uid {
                    Some(new_uid) => {
                        debug!("setattr(ino={}, uid={}, new_uid={})", ino, fp.uid, new_uid);
                        fp.uid = new_uid;
                    }
                    None => {}
                }
                match gid {
                    Some(new_gid) => {
                        debug!("setattr(ino={}, gid={}, new_gid={})", ino, fp.gid, new_gid);
                        fp.gid = new_gid;
                    }
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
                match size {
                    Some(new_size) => {
                        if let Some(memfile) = self.files.get_mut(&ino) {
                            debug!("setattr(ino={}, size={}, new_size={})", ino, fp.size, new_size);
                            memfile.truncate(new_size);
                            fp.size = new_size;
                        } else {
                            return;
                        }
                    }
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
        if let Some(dir) = self.inodes.get_mut(&rmdir_ino) {
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

    fn open(&mut self, _req: &Request, _ino: u64, flags: u32, reply: ReplyOpen) {
        debug!("open(ino={}, _flags={})", _ino, flags);
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
                    self.files.insert(attr.ino, MemFile::new());
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
                match self.attrs.get_mut(&ino) {
                    Some(attr) => {
                        let size = fp.update(&data, offset);
                        attr.atime = ts;
                        attr.mtime = ts;
                        attr.size = fp.size();
                        reply.written(size as u32);
                        debug!("write(ino={}, wrote={}, offset={}, new size={})", ino, size, offset, fp.size());
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
        match self.files.get_mut(&ino) {
            Some(fp) => {
                let mut thread_attrs = self.attrs.clone();
                let thread_fp = fp.clone();
                self.run_worker(move || {
                    match thread_attrs.get_mut(&ino) {
                        Some(attr) => {
                            attr.atime = time::now().to_timespec();
                            reply.data(&thread_fp.bytes[offset as usize..]);
                        },
                        None => {
                            error!("read: ino {} is not in filesystem's attributes", ino);
                            reply.error(ENOENT);
                        },
                    }
                });
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memfile() {
        let mut empty_file = MemFile::new();
        assert_eq!(empty_file.size(), 0);

        let test_str = String::from("testing memfile");
        assert_eq!(empty_file.update(&test_str.into_bytes(), 0), 15);
        assert_eq!(empty_file.size(), 15);
        empty_file.truncate(7 as u64);
        assert_eq!(empty_file.size(), 7);
    }

    #[test]
    fn test_inode() {
        let mut test_inode = Inode::new("dummy-inode".to_string(), 999);
        assert_eq!(test_inode.parent, 999);
        assert_eq!(test_inode.name, "dummy-inode");

        test_inode.children.insert("dummy-child".to_string(), 888);
        assert_eq!(test_inode.children.len(), 1);
        assert_eq!(*test_inode.children.get("dummy-child").unwrap(), 888);
    }

    #[test]
    fn test_init_memfilesystem() {
        let mut testfs = MemFilesystem::new(0);
        assert_eq!(testfs.get_next_ino(), 3);
        assert_eq!(testfs.attrs.len(), 1);
        assert_eq!(testfs.attrs.get(&1).unwrap().ino, 1);
        assert_eq!(testfs.attrs.get(&1).unwrap().kind, FileType::Directory);
        assert_eq!(testfs.inodes.len(), 1);
        assert_eq!(testfs.inodes.get(&1).unwrap().name, "/".to_string());
        assert_eq!(testfs.files.len(), 0);
    }

    #[test]
    fn memfs_update() {
        let mut f = MemFile::new();
        f.update(&[0, 1, 2, 3, 4, 5, 6, 7, 8], 0);
        assert_eq!(f.size(), 9);

        f.update(&[0, 0], 0);
        assert_eq!(f.size(), 9);
        assert_eq!(f.bytes, &[0, 0, 2, 3, 4, 5, 6, 7, 8]);

        f.update(&[1, 1], 8);
        assert_eq!(f.bytes, &[0, 0, 2, 3, 4, 5, 6, 7, 1, 1]);
        assert_eq!(f.size(), 10);

        f.update(&[2, 2], 13);
        assert_eq!(f.bytes, &[0, 0, 2, 3, 4, 5, 6, 7, 1, 1, 0, 0, 0, 2, 2]);
        assert_eq!(f.size(), 15);
    }
}
