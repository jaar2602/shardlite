//! WAL format parsing and frame capture.
//!
//! Reconstructs committed transactions from the raw byte writes SQLite makes to the `-wal`
//! file. Format per <https://sqlite.org/walformat.html>; all integers big-endian.
//!
//! ```text
//! WAL header (32 bytes)          Frame (24-byte header + page_size bytes)
//!   0  magic        u32            0  page number        u32
//!   4  format       u32            4  db size in pages   u32  (non-zero => COMMIT)
//!   8  page size    u32            8  salt-1             u32
//!  12  ckpt seq     u32           12  salt-2             u32
//!  16  salt-1       u32           16  checksum-1         u32
//!  20  salt-2       u32           20  checksum-2         u32
//!  24  checksum-1   u32
//!  28  checksum-2   u32
//! ```
//!
//! # The load-bearing assumption
//!
//! SQLite's *WAL file format* is documented and stable. Its **write pattern** — the
//! sequence of offsets and sizes it passes to the VFS — is not a documented API contract.
//! This parser tolerates a frame header and its page arriving as one write or two, and
//! several frames arriving together, but a future SQLite could in principle write in a
//! shape this does not anticipate. That risk is contained by pinning the bundled SQLite
//! version, and it is why `checkpointing_survives_*` tests exist.

/// Both accepted magic values; they differ only in checksum endianness.
const WAL_MAGIC_BE: u32 = 0x377f_0682;
const WAL_MAGIC_LE: u32 = 0x377f_0683;

pub const WAL_HEADER_SIZE: u64 = 32;
pub const FRAME_HEADER_SIZE: u64 = 24;

fn be32(b: &[u8], at: usize) -> u32 {
    u32::from_be_bytes([b[at], b[at + 1], b[at + 2], b[at + 3]])
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalHeader {
    pub page_size: u32,
    pub checkpoint_seq: u32,
    pub salt: [u8; 8],
}

impl WalHeader {
    fn parse(b: &[u8]) -> Option<Self> {
        if b.len() < WAL_HEADER_SIZE as usize {
            return None;
        }
        let magic = be32(b, 0);
        if magic != WAL_MAGIC_BE && magic != WAL_MAGIC_LE {
            return None;
        }
        // A page_size field of 1 encodes 65536, which does not fit in the u32 field.
        let raw = be32(b, 8);
        let page_size = if raw == 1 { 65_536 } else { raw };
        if !page_size.is_power_of_two() || !(512..=65_536).contains(&page_size) {
            return None;
        }
        let mut salt = [0u8; 8];
        salt.copy_from_slice(&b[16..24]);
        Some(Self {
            page_size,
            checkpoint_seq: be32(b, 12),
            salt,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FrameHeader {
    page_no: u32,
    /// Size of the database in pages after this commit; 0 for a non-commit frame.
    db_size_after_commit: u32,
}

impl FrameHeader {
    fn parse(b: &[u8]) -> Self {
        Self {
            page_no: be32(b, 0),
            db_size_after_commit: be32(b, 4),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    pub page_no: u32,
    pub data: Vec<u8>,
}

/// One committed transaction: the pages it wrote, and the database size after it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommittedTxn {
    pub db_size_pages: u32,
    pub page_size: u32,
    pub frames: Vec<Frame>,
    /// Increments whenever the WAL is reset (a checkpoint rotates the salt).
    pub generation: u64,
}

/// One frame slot in the WAL, addressed by its byte offset.
#[derive(Debug, Clone, Default)]
struct Slot {
    page_no: u32,
    db_size_after_commit: u32,
    data: Option<Vec<u8>>,
}

#[derive(Debug, Default)]
struct State {
    header: Option<WalHeader>,
    generation: u64,
    /// Frames of the current, not-yet-committed region, keyed by byte offset.
    ///
    /// **Addressed by offset, not appended.** SQLite rewrites frame headers in place —
    /// `walRewriteChecksums` fixes checksums and stamps the commit marker onto a frame
    /// whose header was already written. Measured on 3.53.2: a churn workload wrote 4059
    /// frame headers of which 1641 were in-place rewrites. An append-based parser treats
    /// each rewrite as a new frame, finds no page data behind it, and silently loses the
    /// commit marker — leaving a transaction's frames pending forever.
    slots: std::collections::BTreeMap<u64, Slot>,
    committed: Vec<CommittedTxn>,
    resets: u64,
    truncations: u64,
    /// Diagnostic only: the raw `(offset, len)` sequence SQLite wrote.
    trace: Vec<(u64, usize)>,
    trace_enabled: bool,
    header_rewrites: u64,
}

/// Accumulates committed transactions observed on one database's WAL.
#[derive(Debug, Default)]
pub struct WalCapture {
    state: std::sync::Mutex<State>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CaptureStats {
    pub committed_txns: usize,
    /// Frames of a transaction that has not yet committed.
    pub pending_frames: usize,
    /// Frame headers SQLite rewrote in place over a page we already held.
    pub header_rewrites: u64,
    pub generation: u64,
    pub resets: u64,
    pub truncations: u64,
    pub page_size: u32,
}

impl WalCapture {
    pub fn new() -> Self {
        Self::default()
    }

    /// Enable raw write tracing, and take whatever has accumulated.
    ///
    /// Diagnostic aid for checking assumptions about SQLite's WAL write pattern, which is
    /// not a documented contract. Comparing the frame headers in a trace against the frames
    /// captured is how the in-place-rewrite behaviour was found; re-run that comparison
    /// when bumping the bundled SQLite version.
    pub fn take_trace(&self) -> Vec<(u64, usize)> {
        let mut st = self.state.lock().expect("capture mutex");
        st.trace_enabled = true;
        std::mem::take(&mut st.trace)
    }

    /// Feed one write that SQLite made to the `-wal` file.
    pub fn on_write(&self, offset: u64, buf: &[u8]) {
        let mut st = self.state.lock().expect("capture mutex");
        if st.trace_enabled {
            st.trace.push((offset, buf.len()));
        }

        // Offset 0 is always a WAL header. SQLite rewrites it when the WAL is reset after
        // a checkpoint, rotating the salt — that starts a new generation, and any pending
        // (uncommitted) frames from the previous one are dead.
        if offset == 0
            && buf.len() >= WAL_HEADER_SIZE as usize
            && let Some(h) = WalHeader::parse(buf)
        {
            let changed = st.header.map(|old| old.salt != h.salt).unwrap_or(false);
            if changed || st.header.is_none() {
                st.generation += 1;
                if changed {
                    st.resets += 1;
                }
            }
            st.header = Some(h);
            st.slots.clear();

            // A header write can carry frames in the same buffer.
            let rest = &buf[WAL_HEADER_SIZE as usize..];
            if !rest.is_empty() {
                Self::consume(&mut st, WAL_HEADER_SIZE, rest);
            }
            return;
        }

        Self::consume(&mut st, offset, buf);
    }

    /// The WAL file was truncated — a `TRUNCATE` checkpoint. Everything not yet committed
    /// is gone, and the next write will be a fresh header.
    pub fn on_truncate(&self, size: u64) {
        let mut st = self.state.lock().expect("capture mutex");
        if size < WAL_HEADER_SIZE {
            st.slots.clear();
            st.header = None;
            st.truncations += 1;
        }
    }

    fn consume(st: &mut State, offset: u64, buf: &[u8]) {
        let Some(header) = st.header else {
            return; // frames before a header are unparseable; wait for the header
        };
        let page_size = header.page_size as usize;
        let frame_size = FRAME_HEADER_SIZE as usize + page_size;

        if offset < WAL_HEADER_SIZE {
            return;
        }
        let rel = offset - WAL_HEADER_SIZE;

        // Page data for a frame whose header arrived in an earlier write. Because slots
        // are addressed by offset, this needs no cross-call pairing state — the slot is
        // simply looked up.
        if rel % frame_size as u64 == FRAME_HEADER_SIZE && buf.len() >= page_size {
            let slot_offset = offset - FRAME_HEADER_SIZE;
            if let Some(slot) = st.slots.get_mut(&slot_offset) {
                slot.data = Some(buf[..page_size].to_vec());
                let commit_at = (slot.db_size_after_commit != 0).then_some(slot_offset);
                if let Some(at) = commit_at {
                    Self::emit_commit(st, at);
                }
            }
            let rest = &buf[page_size..];
            if !rest.is_empty() {
                Self::consume(st, offset + page_size as u64, rest);
            }
            return;
        }

        if !rel.is_multiple_of(frame_size as u64) {
            return; // not on a frame boundary
        }

        // A buffer may hold a header alone, a whole frame, or several frames.
        let mut pos = 0usize;
        let mut at = offset;
        while pos + FRAME_HEADER_SIZE as usize <= buf.len() {
            let fh = FrameHeader::parse(&buf[pos..]);
            let data_start = pos + FRAME_HEADER_SIZE as usize;
            let data = (data_start + page_size <= buf.len())
                .then(|| buf[data_start..data_start + page_size].to_vec());
            let had_data = data.is_some();

            let slot = st.slots.entry(at).or_default();
            let mut rewrite = false;
            if slot.data.is_some() && !had_data {
                if slot.page_no == fh.page_no {
                    // Same page, header only: an in-place rewrite stamping the commit
                    // marker and fixed checksums. Keep the page we already hold.
                    rewrite = true;
                } else {
                    // A different page in this slot means the slot is being reused; the
                    // page we hold is stale and its data will arrive in the next write.
                    slot.data = None;
                }
            }
            slot.page_no = fh.page_no;
            slot.db_size_after_commit = fh.db_size_after_commit;
            if let Some(d) = data {
                slot.data = Some(d);
            }
            let ready = slot.data.is_some();
            if rewrite {
                st.header_rewrites += 1;
            }

            // The commit marker can arrive with the header, or later as a rewrite of a
            // header whose page was written long before.
            if fh.db_size_after_commit != 0 && ready {
                Self::emit_commit(st, at);
            }

            if !had_data {
                return; // page data follows in a later write
            }
            pos = data_start + page_size;
            at += frame_size as u64;
        }
    }

    /// A commit marker landed on the frame at `at`: every slot up to and including it
    /// forms one transaction.
    fn emit_commit(st: &mut State, at: u64) {
        let page_size = st.header.map(|h| h.page_size).unwrap_or(0);
        let generation = st.generation;

        // Everything after this frame belongs to the next transaction.
        let rest = st.slots.split_off(&(at + 1));
        let txn_slots = std::mem::replace(&mut st.slots, rest);

        let db_size_pages = txn_slots
            .get(&at)
            .map(|s| s.db_size_after_commit)
            .unwrap_or(0);

        let frames: Vec<Frame> = txn_slots
            .into_values()
            .filter_map(|s| {
                s.data.map(|data| Frame {
                    page_no: s.page_no,
                    data,
                })
            })
            .collect();

        if frames.is_empty() {
            return;
        }
        st.committed.push(CommittedTxn {
            db_size_pages,
            page_size,
            frames,
            generation,
        });
    }

    /// Remove and return every transaction committed so far.
    pub fn drain_committed(&self) -> Vec<CommittedTxn> {
        let mut st = self.state.lock().expect("capture mutex");
        std::mem::take(&mut st.committed)
    }

    pub fn stats(&self) -> CaptureStats {
        let st = self.state.lock().expect("capture mutex");
        CaptureStats {
            committed_txns: st.committed.len(),
            pending_frames: st.slots.len(),
            header_rewrites: st.header_rewrites,
            generation: st.generation,
            resets: st.resets,
            truncations: st.truncations,
            page_size: st.header.map(|h| h.page_size).unwrap_or(0),
        }
    }
}

/// Apply captured transactions to a follower database file.
///
/// This is physical replication: pages are written at their page offsets, exactly as a
/// checkpoint would have written them. The follower never executes SQL, which is precisely
/// why non-deterministic functions and per-machine errors cannot make it diverge.
pub fn apply_to_db_file(path: &std::path::Path, txns: &[CommittedTxn]) -> std::io::Result<()> {
    use std::io::{Seek, SeekFrom, Write};

    let mut f = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)?;

    for txn in txns {
        for frame in &txn.frames {
            // Page numbers are 1-based.
            let offset = (frame.page_no as u64 - 1) * txn.page_size as u64;
            f.seek(SeekFrom::Start(offset))?;
            f.write_all(&frame.data)?;
        }
        // The commit frame carries the database size, so a transaction that shrank the
        // database truncates the follower too.
        f.set_len(txn.db_size_pages as u64 * txn.page_size as u64)?;
    }

    f.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn header_bytes(page_size: u32, salt: [u8; 8]) -> Vec<u8> {
        let mut b = vec![0u8; 32];
        b[0..4].copy_from_slice(&WAL_MAGIC_BE.to_be_bytes());
        b[4..8].copy_from_slice(&3_007_000u32.to_be_bytes());
        b[8..12].copy_from_slice(&page_size.to_be_bytes());
        b[16..24].copy_from_slice(&salt);
        b
    }

    fn frame_bytes(page_no: u32, db_size: u32, page_size: u32, fill: u8) -> (Vec<u8>, Vec<u8>) {
        let mut h = vec![0u8; 24];
        h[0..4].copy_from_slice(&page_no.to_be_bytes());
        h[4..8].copy_from_slice(&db_size.to_be_bytes());
        (h, vec![fill; page_size as usize])
    }

    #[test]
    fn parses_header_and_detects_commit() {
        let cap = WalCapture::new();
        cap.on_write(0, &header_bytes(4096, [1; 8]));

        let (h1, d1) = frame_bytes(1, 0, 4096, 0xAA);
        cap.on_write(32, &h1);
        cap.on_write(56, &d1);
        assert_eq!(cap.stats().committed_txns, 0, "no commit flag yet");

        let (h2, d2) = frame_bytes(2, 2, 4096, 0xBB);
        cap.on_write(32 + 4120, &h2);
        cap.on_write(32 + 4120 + 24, &d2);

        let txns = cap.drain_committed();
        assert_eq!(txns.len(), 1);
        assert_eq!(txns[0].db_size_pages, 2);
        assert_eq!(
            txns[0].frames.len(),
            2,
            "both frames belong to the transaction"
        );
        assert_eq!(txns[0].frames[0].page_no, 1);
        assert_eq!(txns[0].frames[1].page_no, 2);
    }

    #[test]
    fn handles_header_and_page_in_one_write() {
        let cap = WalCapture::new();
        cap.on_write(0, &header_bytes(4096, [1; 8]));

        let (h, d) = frame_bytes(1, 1, 4096, 0xCC);
        let mut combined = h;
        combined.extend_from_slice(&d);
        cap.on_write(32, &combined);

        let txns = cap.drain_committed();
        assert_eq!(txns.len(), 1);
        assert_eq!(txns[0].frames[0].data[0], 0xCC);
    }

    #[test]
    fn a_salt_change_starts_a_new_generation_and_drops_pending() {
        let cap = WalCapture::new();
        cap.on_write(0, &header_bytes(4096, [1; 8]));

        // An uncommitted frame, then a checkpoint resets the WAL with a new salt.
        let (h, d) = frame_bytes(1, 0, 4096, 0xAA);
        cap.on_write(32, &h);
        cap.on_write(56, &d);
        assert_eq!(cap.stats().pending_frames, 1);

        cap.on_write(0, &header_bytes(4096, [2; 8]));
        let st = cap.stats();
        assert_eq!(
            st.pending_frames, 0,
            "pending frames die with the old generation"
        );
        assert_eq!(st.resets, 1);
        assert_eq!(st.generation, 2);
    }

    #[test]
    fn a_rewritten_header_carrying_the_commit_marker_is_honoured() {
        // Regression: SQLite writes a frame header with db_size = 0, writes the page, then
        // later rewrites *just the header* at the same offset to stamp the commit marker
        // (walRewriteChecksums). An append-based parser sees the rewrite as a new frame,
        // finds no page behind it, and loses the commit — stranding the transaction
        // forever. Measured on 3.53.2: 1641 of 4059 frame headers were such rewrites.
        let cap = WalCapture::new();
        cap.on_write(0, &header_bytes(4096, [1; 8]));

        // Two frames, neither marked as a commit.
        let (h1, d1) = frame_bytes(1, 0, 4096, 0xAA);
        cap.on_write(32, &h1);
        cap.on_write(56, &d1);
        let (h2, d2) = frame_bytes(2, 0, 4096, 0xBB);
        cap.on_write(32 + 4120, &h2);
        cap.on_write(32 + 4120 + 24, &d2);
        assert_eq!(cap.stats().committed_txns, 0);
        assert_eq!(cap.stats().pending_frames, 2);

        // Header-only rewrite of frame 2, now stamped as the commit.
        let (h2_commit, _) = frame_bytes(2, 2, 4096, 0);
        cap.on_write(32 + 4120, &h2_commit);

        let txns = cap.drain_committed();
        assert_eq!(
            txns.len(),
            1,
            "the rewritten commit marker must close the txn"
        );
        assert_eq!(txns[0].db_size_pages, 2);
        assert_eq!(txns[0].frames.len(), 2, "both frames belong to it");
        // The page written before the rewrite must be preserved, not lost.
        assert_eq!(txns[0].frames[1].data[0], 0xBB);
        assert_eq!(cap.stats().header_rewrites, 1);
    }

    #[test]
    fn a_reused_slot_takes_the_newer_page() {
        // A WAL slot can be rewritten with a different page entirely.
        let cap = WalCapture::new();
        cap.on_write(0, &header_bytes(4096, [1; 8]));

        let (h1, d1) = frame_bytes(7, 0, 4096, 0x11);
        cap.on_write(32, &h1);
        cap.on_write(56, &d1);

        let (h2, d2) = frame_bytes(9, 1, 4096, 0x22);
        cap.on_write(32, &h2);
        cap.on_write(56, &d2);

        let txns = cap.drain_committed();
        assert_eq!(txns.len(), 1);
        assert_eq!(txns[0].frames.len(), 1, "one slot holds one frame");
        assert_eq!(txns[0].frames[0].page_no, 9);
        assert_eq!(txns[0].frames[0].data[0], 0x22);
    }

    #[test]
    fn truncation_clears_state() {
        let cap = WalCapture::new();
        cap.on_write(0, &header_bytes(4096, [1; 8]));
        let (h, d) = frame_bytes(1, 0, 4096, 0xAA);
        cap.on_write(32, &h);
        cap.on_write(56, &d);

        cap.on_truncate(0);
        let st = cap.stats();
        assert_eq!(st.pending_frames, 0);
        assert_eq!(st.truncations, 1);
    }

    #[test]
    fn rejects_a_non_wal_buffer_at_offset_zero() {
        let cap = WalCapture::new();
        cap.on_write(0, &[0u8; 32]);
        assert_eq!(
            cap.stats().page_size,
            0,
            "garbage must not be taken as a header"
        );
    }
}
