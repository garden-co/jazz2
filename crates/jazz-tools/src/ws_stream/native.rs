use crate::transport_manager::StreamAdapter;
use futures::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};

pub struct NativeWsStream {
    inner: WebSocketStream<MaybeTlsStream<TcpStream>>,
}

impl StreamAdapter for NativeWsStream {
    type Error = tokio_tungstenite::tungstenite::Error;

    async fn connect(url: &str) -> Result<Self, Self::Error> {
        let (ws, _) = connect_async(url).await?;
        Ok(Self { inner: ws })
    }

    async fn send(&mut self, data: Vec<u8>) -> Result<(), Self::Error> {
        self.inner.send(Message::Binary(data)).await
    }

    async fn recv(&mut self) -> Result<Option<Vec<u8>>, Self::Error> {
        loop {
            match self.inner.next().await {
                Some(Ok(Message::Binary(b))) => return Ok(Some(b)),
                Some(Ok(Message::Ping(_))) | Some(Ok(Message::Pong(_))) => continue,
                Some(Ok(Message::Close(_))) | None => return Ok(None),
                Some(Ok(Message::Text(_))) => {
                    tracing::debug!(
                        "received unexpected text frame on binary-only WS connection; ignoring"
                    );
                    continue;
                }
                Some(Ok(Message::Frame(_))) => continue, // raw frame: send-only, cannot arrive on read
                Some(Err(e)) => return Err(e),
            }
        }
    }

    async fn close(&mut self) {
        let _ = self.inner.close(None).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;
    use tokio_tungstenite::accept_async;

    #[tokio::test]
    async fn native_ws_stream_send_recv_roundtrip() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut ws = accept_async(tcp).await.unwrap();
            let _ = ready_tx.send(());
            while let Some(Ok(msg)) = ws.next().await {
                ws.send(msg).await.unwrap();
            }
        });
        let mut stream = NativeWsStream::connect(&format!("ws://{addr}"))
            .await
            .unwrap();
        ready_rx.await.unwrap();
        stream.send(b"hello ws".to_vec()).await.unwrap();
        assert_eq!(stream.recv().await.unwrap().unwrap(), b"hello ws".to_vec());
    }

    #[tokio::test]
    async fn native_ws_stream_server_close_yields_none_on_recv() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut ws = accept_async(tcp).await.unwrap();
            let _ = ws.close(None).await;
        });
        let mut stream = NativeWsStream::connect(&format!("ws://{addr}"))
            .await
            .unwrap();
        assert!(stream.recv().await.unwrap().is_none());
    }
}
