//! Connection poller: one thread parks every idle connection.
//!
//! The sweep loop peeks each parked socket (non-blocking; no bytes
//! consumed) and queues connections with waiting input to the pool, reaps
//! connections quiet past the idle timeout, and closes everything parked
//! when draining. When a full sweep finds nothing to do it sleeps 1ms, so
//! a quiet server burns ~no CPU while a fresh command is picked up within
//! ~a millisecond plus the sweep remainder.

use std::sync::atomic::Ordering;
use std::sync::{mpsc::SyncSender, Arc};
use std::time::Duration;

use super::{Broker, Job, PeekAction, Shared};

/// Idle-loop sleep when a sweep finds no work (latency vs CPU tradeoff).
const QUIET_SLEEP: Duration = Duration::from_millis(1);

pub fn run_poller(
    broker: Arc<Broker>,
    tx: SyncSender<Job>,
    shared: Arc<Shared>,
    registry: crate::ConnRegistry,
) {
    loop {
        if shared.shutdown.load(Ordering::Relaxed) || shared.global.load(Ordering::Relaxed) {
            break;
        }
        let mut progress = false;
        // Readiness pass: queue every parked connection with input.
        // The in-flight mark suppresses duplicate submits (a second
        // worker taking the same connection would block forever on an
        // empty read); a full queue unmarks for a later retry.
        for (id, action) in broker.snapshot() {
            match action {
                PeekAction::Data => {
                    if super::submit(&tx, id) {
                        progress = true;
                    } else {
                        broker.unmark(id);
                    }
                }
                PeekAction::Close => {
                    if let Some(conn) = broker.remove(id) {
                        // Abnormal close (not idle-reap, not drain): one line
                        // per event — parked connections should only die
                        // this way on real socket errors or peer FIN.
                        let age = conn.last_active.elapsed().as_secs();
                        eprintln!(
                            "poller: reaping {} connection #{id} (idle {age}s, strikes {})",
                            conn.describe(),
                            conn.probe_fails
                        );
                        conn.shutdown_sock();
                        registry.remove(conn.reg_id);
                        progress = true;
                    }
                }
            }
        }
        // Idle reap: parked connections quiet past the deadline close.
        // (Busy connections are worker-owned; their blocking reads enforce
        // the same timeout, so semantics match the old model exactly.)
        for id in broker.reap(shared.idle_timeout, false) {
            if let Some(conn) = broker.remove(id) {
                conn.shutdown_sock();
                registry.remove(conn.reg_id);
                progress = true;
            }
        }
        if !progress {
            std::thread::sleep(QUIET_SLEEP);
        }
    }
    // Draining: nothing new will be queued; close everything still parked
    // and refuse late checkins so no socket outlives the sweep loop.
    broker.set_accepting(false);
    for id in broker.reap(None, true) {
        if let Some(conn) = broker.remove(id) {
            conn.shutdown_sock();
            registry.remove(conn.reg_id);
        }
    }
}
