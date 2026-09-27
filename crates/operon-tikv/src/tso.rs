//! The TSO clock: fresh timestamps from PD's timestamp oracle.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tikv_client::{Timestamp, TimestampExt, TransactionClient};

use crate::TikvError;

/// Fetches TSO timestamps and remembers the latest one with the instant it
/// arrived (the anchor of the metastore's synchronous `now_ms`, R1 row R2).
pub(crate) struct TsoClock {
    client: Arc<TransactionClient>,
    timeout: Duration,
    latest: Mutex<Option<(Timestamp, Instant)>>,
}

impl TsoClock {
    pub(crate) fn new(client: Arc<TransactionClient>, timeout: Duration) -> Self {
        TsoClock {
            client,
            timeout,
            latest: Mutex::new(None),
        }
    }

    /// A fresh timestamp, later than every timestamp PD handed out before the
    /// call started.
    pub(crate) async fn now(&self) -> Result<Timestamp, TikvError> {
        let ts = tokio::time::timeout(self.timeout, self.client.current_timestamp())
            .await
            .map_err(|_| TikvError::Timeout {
                op: "TSO",
                after: self.timeout,
            })??;
        let arrived = Instant::now();
        let mut latest = self.latest.lock().unwrap_or_else(|e| e.into_inner());
        if latest
            .as_ref()
            .is_none_or(|(seen, _)| seen.version() < ts.version())
        {
            *latest = Some((ts.clone(), arrived));
        }
        Ok(ts)
    }

    /// The latest timestamp this clock obtained, and when it arrived.
    pub(crate) fn latest(&self) -> Option<(Timestamp, Instant)> {
        self.latest
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }
}
