//! The sync primitives, swappable for loom's model-checked versions.
//!
//! Under `RUSTFLAGS="--cfg loom"` these resolve to `loom::sync`, whose types record every
//! load, store and lock so the model checker can exhaustively explore interleavings. A
//! normal build resolves to `std::sync` and compiles to exactly what it always did.
//!
//! Only the modules whose interleavings are model-checked route through here — `fence` and
//! `mode`, the two places the concurrency audit found real races. Routing everything through
//! would cost nothing at runtime but would imply a coverage loom does not have: it can only
//! check what fits in a model, and a network server does not.

#[cfg(loom)]
pub(crate) use loom::sync::{Mutex, RwLock, RwLockReadGuard, atomic};

#[cfg(not(loom))]
pub(crate) use std::sync::{Mutex, RwLock, RwLockReadGuard, atomic};
