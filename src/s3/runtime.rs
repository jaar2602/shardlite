//! Runtime handle to the S3 archival sink, so an operator can attach, reconfigure, or detach it
//! from the console without restarting the node — the piece that turns the stored-but-unapplied S3
//! config into a live feature.
//!
//! It holds the **concrete** [`S3Sink`] (unlike the writer fleet, which sees a type-erased
//! `Arc<dyn FrameSink>`), so status, on-demand snapshots, and flush can reach S3-specific state.
//! The non-secret [`S3Summary`] is kept alongside for the status endpoint; the secret key is never
//! retained here.

use std::sync::{Arc, Mutex};

use super::sink::{S3Sink, S3SinkStatus};

/// The non-secret description of where a sink archives to. Safe to return from a status endpoint.
#[derive(Clone, Debug, serde::Serialize)]
pub struct S3Summary {
    pub bucket: String,
    pub endpoint: String,
    pub region: String,
    pub prefix: String,
}

struct Attached {
    sink: Arc<S3Sink>,
    summary: S3Summary,
}

/// The currently-attached S3 sink, if any. Swapped under a mutex so a config change is atomic.
#[derive(Default)]
pub struct S3Runtime {
    inner: Mutex<Option<Attached>>,
}

impl S3Runtime {
    pub fn new() -> Self {
        Self::default()
    }

    /// Make `sink` the live archival sink. The caller has already attached it to the writer fleet
    /// ([`crate::shard::ShardManager::set_sink`]); this keeps the concrete handle for status/flush.
    pub fn attach(&self, sink: Arc<S3Sink>, summary: S3Summary) {
        *self.inner.lock().unwrap() = Some(Attached { sink, summary });
    }

    /// Forget the sink. The caller has already detached it from the writer fleet.
    pub fn detach(&self) {
        *self.inner.lock().unwrap() = None;
    }

    pub fn configured(&self) -> bool {
        self.inner.lock().unwrap().is_some()
    }

    pub fn summary(&self) -> Option<S3Summary> {
        self.inner
            .lock()
            .unwrap()
            .as_ref()
            .map(|a| a.summary.clone())
    }

    /// The live sink, for on-demand snapshot/flush.
    pub fn sink(&self) -> Option<Arc<S3Sink>> {
        self.inner
            .lock()
            .unwrap()
            .as_ref()
            .map(|a| Arc::clone(&a.sink))
    }

    /// Where it archives to plus its health/progress, if a sink is attached.
    pub fn status(&self) -> Option<(S3Summary, S3SinkStatus)> {
        self.inner
            .lock()
            .unwrap()
            .as_ref()
            .map(|a| (a.summary.clone(), a.sink.status()))
    }
}
