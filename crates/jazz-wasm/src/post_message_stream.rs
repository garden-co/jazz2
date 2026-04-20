//! PostMessageStream — StreamAdapter over `Worker` / DedicatedWorkerGlobalScope.
//!
//! `send`: one Rust→JS copy into a `Uint8Array`, then `postMessage(buf,
//! [buf.buffer])` with transfer list → receiving context gets zero-copy
//! ownership of the ArrayBuffer.
//!
//! `send_batch`: a single `postMessage(array, transfer_list)` with N
//! Uint8Arrays in the array and all N backing buffers in the transfer list.
//! The receiver's `onmessage` handler unpacks the array into individual
//! inbox entries.

use futures::channel::mpsc;
use futures::StreamExt as _;
use jazz_tools::transport_manager::StreamAdapter;
use js_sys::{Array, Uint8Array};
use std::cell::RefCell;
use std::fmt;
use std::rc::Rc;
use wasm_bindgen::closure::Closure;
use wasm_bindgen::{JsCast, JsValue};
use web_sys::MessageEvent;

#[derive(Debug)]
pub struct PostMessageError(pub String);
impl fmt::Display for PostMessageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Abstracts over `web_sys::Worker` (main-thread side) and
/// `web_sys::DedicatedWorkerGlobalScope` (in-worker side).
pub trait PostMessageEndpoint {
    fn post_message_with_transfer(&self, msg: &JsValue, transfer: &JsValue) -> Result<(), JsValue>;
    fn install_onmessage(&self, handler: &Closure<dyn FnMut(MessageEvent)>);
}

impl PostMessageEndpoint for web_sys::Worker {
    fn post_message_with_transfer(&self, msg: &JsValue, transfer: &JsValue) -> Result<(), JsValue> {
        web_sys::Worker::post_message_with_transfer(self, msg, transfer)
    }
    fn install_onmessage(&self, handler: &Closure<dyn FnMut(MessageEvent)>) {
        self.set_onmessage(Some(handler.as_ref().unchecked_ref()));
    }
}

impl PostMessageEndpoint for web_sys::DedicatedWorkerGlobalScope {
    fn post_message_with_transfer(&self, msg: &JsValue, transfer: &JsValue) -> Result<(), JsValue> {
        web_sys::DedicatedWorkerGlobalScope::post_message_with_transfer(self, msg, transfer)
    }
    fn install_onmessage(&self, handler: &Closure<dyn FnMut(MessageEvent)>) {
        self.set_onmessage(Some(handler.as_ref().unchecked_ref()));
    }
}

pub struct PostMessageStream<E: PostMessageEndpoint + 'static> {
    endpoint: Rc<E>,
    inbox_rx: mpsc::UnboundedReceiver<Vec<u8>>,
    /// Held for the lifetime of the stream — dropping detaches onmessage.
    _onmessage: Closure<dyn FnMut(MessageEvent)>,
}

impl<E: PostMessageEndpoint + 'static> PostMessageStream<E> {
    /// Build a stream around an endpoint. Installs `onmessage` immediately.
    /// Accepts top-level `Uint8Array` (single-frame `send`) and top-level
    /// `Array` of `Uint8Array` (batched `send_batch`); anything else is
    /// ignored as a protocol error.
    pub fn new(endpoint: Rc<E>) -> Self {
        let (inbox_tx, inbox_rx) = mpsc::unbounded();
        let tx = RefCell::new(inbox_tx);
        let onmessage = Closure::<dyn FnMut(MessageEvent)>::new(move |ev: MessageEvent| {
            let data = ev.data();
            if let Some(arr) = data.dyn_ref::<Uint8Array>() {
                let mut buf = vec![0u8; arr.length() as usize];
                arr.copy_to(&mut buf);
                let _ = tx.borrow().unbounded_send(buf);
                return;
            }
            if let Some(batch) = data.dyn_ref::<Array>() {
                let len = batch.length();
                for i in 0..len {
                    let item = batch.get(i);
                    if let Some(arr) = item.dyn_ref::<Uint8Array>() {
                        let mut buf = vec![0u8; arr.length() as usize];
                        arr.copy_to(&mut buf);
                        let _ = tx.borrow().unbounded_send(buf);
                    }
                }
            }
        });
        endpoint.install_onmessage(&onmessage);
        Self {
            endpoint,
            inbox_rx,
            _onmessage: onmessage,
        }
    }
}

impl<E: PostMessageEndpoint + 'static> StreamAdapter for PostMessageStream<E> {
    type Error = PostMessageError;

    async fn connect(_url: &str) -> Result<Self, Self::Error> {
        Err(PostMessageError(
            "PostMessageStream does not support connect()".into(),
        ))
    }

    async fn send(&mut self, data: Vec<u8>) -> Result<(), Self::Error> {
        let arr = Uint8Array::new_with_length(data.len() as u32);
        arr.copy_from(&data);
        let transfer = Array::new();
        transfer.push(&arr.buffer());
        self.endpoint
            .post_message_with_transfer(&arr, &transfer)
            .map_err(|e| PostMessageError(format!("{e:?}")))
    }

    async fn send_batch(&mut self, frames: Vec<Vec<u8>>) -> Result<(), Self::Error> {
        if frames.is_empty() {
            return Ok(());
        }
        let payload = Array::new();
        let transfer = Array::new();
        for frame in &frames {
            let arr = Uint8Array::new_with_length(frame.len() as u32);
            arr.copy_from(frame);
            transfer.push(&arr.buffer());
            payload.push(&arr);
        }
        self.endpoint
            .post_message_with_transfer(&payload, &transfer)
            .map_err(|e| PostMessageError(format!("{e:?}")))
    }

    async fn recv(&mut self) -> Result<Option<Vec<u8>>, Self::Error> {
        Ok(self.inbox_rx.next().await)
    }

    async fn close(&mut self) {
        // PostMessage has no close semantics; dropping detaches onmessage.
    }
}

#[cfg(all(test, target_arch = "wasm32"))]
mod wasm_tests {
    use super::*;
    use wasm_bindgen_test::*;
    use web_sys::MessageChannel;

    wasm_bindgen_test_configure!(run_in_browser);

    struct PortEndpoint(Rc<web_sys::MessagePort>);
    impl PostMessageEndpoint for PortEndpoint {
        fn post_message_with_transfer(
            &self,
            msg: &JsValue,
            transfer: &JsValue,
        ) -> Result<(), JsValue> {
            self.0.post_message_with_transferable(msg, transfer)
        }
        fn install_onmessage(&self, handler: &Closure<dyn FnMut(MessageEvent)>) {
            self.0.set_onmessage(Some(handler.as_ref().unchecked_ref()));
        }
    }

    fn port_pair() -> (Rc<web_sys::MessagePort>, Rc<web_sys::MessagePort>) {
        let ch = MessageChannel::new().unwrap();
        let a = Rc::new(ch.port1());
        let b = Rc::new(ch.port2());
        a.start();
        b.start();
        (a, b)
    }

    #[wasm_bindgen_test]
    async fn loopback_send_recv_roundtrip() {
        let (a, b) = port_pair();
        let mut sender = PostMessageStream::new(Rc::new(PortEndpoint(a)));
        let mut receiver = PostMessageStream::new(Rc::new(PortEndpoint(b)));
        sender.send(b"hello".to_vec()).await.unwrap();
        assert_eq!(receiver.recv().await.unwrap().unwrap(), b"hello".to_vec());
    }

    #[wasm_bindgen_test]
    async fn loopback_send_batch_fans_out_on_recv() {
        let (a, b) = port_pair();
        let mut sender = PostMessageStream::new(Rc::new(PortEndpoint(a)));
        let mut receiver = PostMessageStream::new(Rc::new(PortEndpoint(b)));
        sender
            .send_batch(vec![b"one".to_vec(), b"two".to_vec(), b"three".to_vec()])
            .await
            .unwrap();
        assert_eq!(receiver.recv().await.unwrap().unwrap(), b"one".to_vec());
        assert_eq!(receiver.recv().await.unwrap().unwrap(), b"two".to_vec());
        assert_eq!(receiver.recv().await.unwrap().unwrap(), b"three".to_vec());
    }
}
