//! Watch-stream plumbing shared by the services (AP0 Ruling 3).

use std::collections::VecDeque;
use std::time::Duration;

use connectrpc::ConnectError;
use futures::Stream;
use tokio::time::{Instant, Interval, interval_at};

/// A heartbeat timer whose first tick is one period away.
pub(crate) fn heartbeat_timer(period: Duration) -> Interval {
    interval_at(Instant::now() + period, period)
}

/// A stream that yields `first`, then `heartbeat()` every `period` until the
/// client goes away. Used where the mock does not simulate changes yet.
pub(crate) fn snapshot_then_heartbeats<T, F>(
    first: T,
    period: Duration,
    heartbeat: F,
) -> impl Stream<Item = Result<T, ConnectError>> + Send + 'static
where
    T: Send + 'static,
    F: Fn() -> T + Send + 'static,
{
    let queue = VecDeque::from([first]);
    futures::stream::unfold(
        (queue, heartbeat_timer(period), heartbeat),
        |(mut queue, mut timer, heartbeat)| async move {
            if let Some(item) = queue.pop_front() {
                return Some((Ok(item), (queue, timer, heartbeat)));
            }
            timer.tick().await;
            let beat = heartbeat();
            Some((Ok(beat), (queue, timer, heartbeat)))
        },
    )
}
