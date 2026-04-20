//! PeerStreamPump — framing-only bytes pump over a StreamAdapter.
//!
//! Extracted from `transport_manager::run_connected`. Owns no handshake,
//! no auth, no reconnect — just: drain outbox → send, recv → inbox, exit
//! on shutdown control.
//!
//! Note: `TransportManager` continues to own its own select-loop in
//! `run_connected` because the server path decodes length-prefixed
//! `ServerEvent` frames. `PeerStreamPump` is the byte-level alternative
//! used where a frame = one whole message (the worker bridge).

use crate::transport_manager::StreamAdapter;
use futures::channel::mpsc;

pub enum PumpControl {
    Shutdown,
}

pub enum PumpExit {
    NetworkError(String),
    Shutdown,
}

/// Consumer-facing ends of a `PeerStreamPump`. Mirrors `TransportHandle` from
/// `transport_manager::create`.
pub struct PeerStreamPumpHandle {
    pub outbox_tx: mpsc::UnboundedSender<Vec<u8>>,
    pub inbox_rx: mpsc::UnboundedReceiver<Vec<u8>>,
    pub control_tx: mpsc::UnboundedSender<PumpControl>,
}

pub struct PeerStreamPump<S: StreamAdapter> {
    pub(crate) outbox_rx: mpsc::UnboundedReceiver<Vec<u8>>,
    pub(crate) inbox_tx: mpsc::UnboundedSender<Vec<u8>>,
    pub(crate) control_rx: mpsc::UnboundedReceiver<PumpControl>,
    pub(crate) stream: S,
}

/// Create paired `(PeerStreamPumpHandle, PeerStreamPump<S>)`.
///
/// The handle holds the consumer-facing channel ends; the pump holds the
/// internal ends and the stream, ready to be spawned via `.run()`.
pub fn create<S: StreamAdapter>(stream: S) -> (PeerStreamPumpHandle, PeerStreamPump<S>) {
    let (outbox_tx, outbox_rx) = mpsc::unbounded();
    let (inbox_tx, inbox_rx) = mpsc::unbounded();
    let (control_tx, control_rx) = mpsc::unbounded();
    let handle = PeerStreamPumpHandle {
        outbox_tx,
        inbox_rx,
        control_tx,
    };
    let pump = PeerStreamPump {
        outbox_rx,
        inbox_tx,
        control_rx,
        stream,
    };
    (handle, pump)
}

impl<S: StreamAdapter + 'static> PeerStreamPump<S> {
    /// Drive the pump until shutdown or a network error.
    ///
    /// Uses `futures::select!` with `.fuse()` everywhere — compatible with
    /// both tokio runtimes and wasm single-threaded runtimes. No cfg fork
    /// required.
    pub async fn run(mut self) -> PumpExit {
        use futures::{FutureExt as _, StreamExt as _};
        loop {
            futures::select! {
                out = self.outbox_rx.next().fuse() => {
                    let Some(bytes) = out else { return PumpExit::Shutdown; };
                    if self.stream.send(bytes).await.is_err() {
                        return PumpExit::NetworkError("send failed".into());
                    }
                }
                incoming = self.stream.recv().fuse() => {
                    match incoming {
                        Ok(Some(bytes)) => {
                            let _ = self.inbox_tx.unbounded_send(bytes);
                        }
                        Ok(None) => return PumpExit::NetworkError("stream closed".into()),
                        Err(e) => return PumpExit::NetworkError(format!("{e}")),
                    }
                }
                ctrl = self.control_rx.next().fuse() => {
                    match ctrl {
                        None | Some(PumpControl::Shutdown) => {
                            self.stream.close().await;
                            return PumpExit::Shutdown;
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Observable stream: records every `send` call in a shared vec; drives
    /// `recv` from a channel so the pump does not see EOF while the inbound
    /// queue is empty.
    struct ObservableStream {
        sent: std::sync::Arc<std::sync::Mutex<Vec<Vec<u8>>>>,
        inbound_rx: mpsc::UnboundedReceiver<Vec<u8>>,
    }

    impl StreamAdapter for ObservableStream {
        type Error = &'static str;
        async fn connect(_: &str) -> Result<Self, Self::Error> {
            unimplemented!()
        }
        async fn send(&mut self, data: Vec<u8>) -> Result<(), Self::Error> {
            self.sent.lock().unwrap().push(data);
            Ok(())
        }
        async fn recv(&mut self) -> Result<Option<Vec<u8>>, Self::Error> {
            use futures::StreamExt as _;
            Ok(self.inbound_rx.next().await)
        }
        async fn close(&mut self) {}
    }

    /// Helper: construct an `ObservableStream` plus the sender side the test
    /// can use to inject inbound frames.
    fn make_observable_stream() -> (
        ObservableStream,
        std::sync::Arc<std::sync::Mutex<Vec<Vec<u8>>>>,
        mpsc::UnboundedSender<Vec<u8>>,
    ) {
        let sent = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let (inbound_tx, inbound_rx) = mpsc::unbounded::<Vec<u8>>();
        let stream = ObservableStream {
            sent: sent.clone(),
            inbound_rx,
        };
        (stream, sent, inbound_tx)
    }

    /// Push two frames through `outbox_tx`, yield, shut down, assert the
    /// stream received both frames in order.
    #[tokio::test]
    #[cfg(feature = "runtime-tokio")]
    async fn pump_forwards_outbox_to_stream_send() {
        let (stream, sent, _inbound_tx) = make_observable_stream();
        let (handle, pump) = create(stream);

        handle.outbox_tx.unbounded_send(vec![1]).unwrap();
        handle.outbox_tx.unbounded_send(vec![2]).unwrap();

        let task = tokio::spawn(pump.run());

        // Yield enough times for the pump to process both outbox frames.
        for _ in 0..4 {
            tokio::task::yield_now().await;
        }

        handle
            .control_tx
            .unbounded_send(PumpControl::Shutdown)
            .unwrap();
        let exit = task.await.unwrap();
        assert!(matches!(exit, PumpExit::Shutdown));
        assert_eq!(*sent.lock().unwrap(), vec![vec![1u8], vec![2u8]]);
    }

    /// Push two frames via the stream's inbound side, assert `inbox_rx`
    /// yields them in order. Then shutdown cleanly.
    #[tokio::test]
    #[cfg(feature = "runtime-tokio")]
    async fn pump_forwards_stream_recv_to_inbox() {
        let (stream, _sent, inbound_tx) = make_observable_stream();
        let (mut handle, pump) = create(stream);

        inbound_tx.unbounded_send(vec![10]).unwrap();
        inbound_tx.unbounded_send(vec![20]).unwrap();

        let task = tokio::spawn(pump.run());

        // Yield so the pump processes both inbound frames.
        for _ in 0..4 {
            tokio::task::yield_now().await;
        }

        let first = handle.inbox_rx.try_recv().unwrap();
        let second = handle.inbox_rx.try_recv().unwrap();
        assert_eq!(first, vec![10u8]);
        assert_eq!(second, vec![20u8]);

        handle
            .control_tx
            .unbounded_send(PumpControl::Shutdown)
            .unwrap();
        let exit = task.await.unwrap();
        assert!(matches!(exit, PumpExit::Shutdown));
    }

    /// Send a Shutdown control immediately; assert the pump exits with
    /// `PumpExit::Shutdown` and no outbound frames were sent.
    #[tokio::test]
    #[cfg(feature = "runtime-tokio")]
    async fn pump_exits_on_shutdown_control() {
        let (stream, sent, _inbound_tx) = make_observable_stream();
        let (handle, pump) = create(stream);

        let task = tokio::spawn(pump.run());

        tokio::task::yield_now().await;
        handle
            .control_tx
            .unbounded_send(PumpControl::Shutdown)
            .unwrap();

        let exit = task.await.unwrap();
        assert!(matches!(exit, PumpExit::Shutdown));
        assert!(sent.lock().unwrap().is_empty());
    }
}
