//! Random-access reads over an S3 object, with a bounded LRU cache of fixed-size chunks.
//!
//! This is the read half of S3-backed HA: SQLite asks for `(offset, len)` byte ranges (a page, the
//! header), and [`S3Pager`] serves them by range-`GET`ting the covering chunks from S3 and caching
//! them. A hot working set stays in the cache after warmup; a cold read faults to S3. The VFS in
//! `vfs.rs` wraps this so SQLite can open a database whose bytes live in object storage.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use super::{Result, S3Client};

/// Default chunk size — the unit fetched and cached. 64 KiB amortises S3 request overhead over
/// many 4 KiB SQLite pages without wasting much on a point read.
pub const DEFAULT_CHUNK: u64 = 64 * 1024;
/// Default cache ceiling: 16 MiB of chunks per open database.
pub const DEFAULT_CACHE_BYTES: u64 = 16 * 1024 * 1024;

/// A read-only, page-cached view of one S3 object.
pub struct S3Pager {
    client: Arc<S3Client>,
    key: String,
    size: u64,
    chunk: u64,
    cache: Mutex<Cache>,
}

struct Cache {
    map: HashMap<u64, Vec<u8>>,
    /// Chunk indices, least-recently-used at the front.
    lru: VecDeque<u64>,
    max_chunks: usize,
}

impl Cache {
    fn touch(&mut self, c: u64) {
        if let Some(pos) = self.lru.iter().position(|&x| x == c) {
            self.lru.remove(pos);
        }
        self.lru.push_back(c);
    }

    fn get(&mut self, c: u64) -> Option<Vec<u8>> {
        let hit = self.map.get(&c).cloned();
        if hit.is_some() {
            self.touch(c);
        }
        hit
    }

    fn put(&mut self, c: u64, bytes: Vec<u8>) {
        self.map.insert(c, bytes);
        self.touch(c);
        while self.map.len() > self.max_chunks {
            if let Some(old) = self.lru.pop_front() {
                self.map.remove(&old);
            } else {
                break;
            }
        }
    }
}

impl S3Pager {
    /// Open a pager for `key`, learning its size with one `HEAD` (no bytes fetched yet).
    pub fn open(client: Arc<S3Client>, key: impl Into<String>) -> Result<Self> {
        Self::with_limits(client, key, DEFAULT_CHUNK, DEFAULT_CACHE_BYTES)
    }

    pub fn with_limits(
        client: Arc<S3Client>,
        key: impl Into<String>,
        chunk: u64,
        cache_bytes: u64,
    ) -> Result<Self> {
        let key = key.into();
        let size = client.head(&key)?;
        let max_chunks = (cache_bytes / chunk).max(1) as usize;
        Ok(Self {
            client,
            key,
            size,
            chunk,
            cache: Mutex::new(Cache {
                map: HashMap::new(),
                lru: VecDeque::new(),
                max_chunks,
            }),
        })
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    /// Fill `buf` from the object starting at `offset`. Returns the number of bytes read, which is
    /// short only at end-of-object (SQLite treats a short read past EOF as zero-fill).
    pub fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<usize> {
        if offset >= self.size || buf.is_empty() {
            return Ok(0);
        }
        let end = (offset + buf.len() as u64).min(self.size);
        let want = (end - offset) as usize;
        let mut done = 0usize;
        let mut pos = offset;

        while (pos as usize) < offset as usize + want {
            let c = pos / self.chunk;
            let chunk_start = c * self.chunk;
            let chunk_end = (chunk_start + self.chunk).min(self.size);
            let in_chunk = (pos - chunk_start) as usize;
            let copy_len = ((chunk_end - pos) as usize).min(want - done);

            // Cache hit → copy straight out.
            let cached = self.cache.lock().unwrap().get(c);
            let bytes = match cached {
                Some(b) => b,
                None => {
                    // Miss → fetch the whole chunk (outside the lock), then cache it.
                    let fetched =
                        self.client
                            .get_range(&self.key, chunk_start, chunk_end - chunk_start)?;
                    self.cache.lock().unwrap().put(c, fetched.clone());
                    fetched
                }
            };
            buf[done..done + copy_len].copy_from_slice(&bytes[in_chunk..in_chunk + copy_len]);
            done += copy_len;
            pos += copy_len as u64;
        }
        Ok(done)
    }
}
