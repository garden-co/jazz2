use futures::{SinkExt, StreamExt};
use jazz_tools::transport_manager::StreamAdapter;
use ws_stream_wasm::{WsMessage, WsMeta, WsStream};

pub struct WasmWsStream {
    inner: WsStream,
}

impl StreamAdapter for WasmWsStream {
    type Error = ws_stream_wasm::WsErr;

    async fn connect(url: &str) -> Result<Self, Self::Error> {
        let (_meta, ws) = WsMeta::connect(url, None).await?;
        Ok(Self { inner: ws })
    }

    async fn send(&mut self, data: Vec<u8>) -> Result<(), Self::Error> {
        self.inner.send(WsMessage::Binary(data)).await
    }

    async fn recv(&mut self) -> Result<Option<Vec<u8>>, Self::Error> {
        loop {
            match self.inner.next().await {
                Some(WsMessage::Binary(b)) => return Ok(Some(b)),
                Some(WsMessage::Text(_)) => continue,
                None => return Ok(None),
            }
        }
    }

    async fn close(&mut self) {
        let _ = self.inner.close().await;
    }
}
