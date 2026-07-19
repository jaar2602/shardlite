//! Storage layer: connection configuration and lifecycle for a single database file.
//!
//! Nothing in this module knows about replication or clustering. That separation is
//! deliberate — it keeps the writer, batching, and crash-safety work independently
//! buildable and testable.

pub mod exec;
pub mod open;
pub mod pragma;
pub mod reader;
pub mod writer;

pub use exec::{Executed, Outcome, QueryResult, Value, WriteOutcome};
pub use open::{WriterOpened, open_reader, open_writer};
pub use reader::{ReaderPool, ReaderStats};
pub use writer::{Writer, WriterHandle, WriterStats};
