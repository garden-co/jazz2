#[cfg(feature = "transport-websocket")]
#[allow(clippy::module_inception)]
mod install_transport_tests {
    use super::super::*;
    use crate::transport_manager::{AuthConfig, StreamAdapter, TickNotifier};

    struct NopTick;
    impl TickNotifier for NopTick {
        fn notify(&self) {}
    }

    struct NopStreamAdapter;
    impl StreamAdapter for NopStreamAdapter {
        type Error = &'static str;
        async fn connect(_url: &str) -> Result<Self, Self::Error> {
            futures::future::pending::<()>().await;
            unreachable!()
        }
        async fn send(&mut self, _data: &[u8]) -> Result<(), Self::Error> {
            Ok(())
        }
        async fn recv(&mut self) -> Result<Option<Vec<u8>>, Self::Error> {
            Ok(None)
        }
        async fn close(&mut self) {}
    }

    #[test]
    #[should_panic(expected = "install_transport called while a transport is already installed")]
    fn install_transport_panics_if_transport_already_installed() {
        let mut core = create_test_runtime();
        // Install once.
        let _first = crate::runtime_core::install_transport::<_, _, NopStreamAdapter, _>(
            &mut core,
            "ws://example.test/ws".to_string(),
            AuthConfig::default(),
            NopTick,
        );
        // Install a second time — must panic via debug_assert.
        let _second = crate::runtime_core::install_transport::<_, _, NopStreamAdapter, _>(
            &mut core,
            "ws://example.test/ws".to_string(),
            AuthConfig::default(),
            NopTick,
        );
    }

    #[test]
    fn install_transport_seeds_catalogue_hash_and_declared_schema_hash() {
        let mut core = create_test_runtime();

        let _manager = crate::runtime_core::install_transport::<_, _, NopStreamAdapter, _>(
            &mut core,
            "ws://example.test/ws".to_string(),
            AuthConfig::default(),
            NopTick,
        );

        assert!(
            core.transport.is_some(),
            "transport handle should be installed"
        );
        let expected_hash = core.schema_manager().catalogue_state_hash();
        let handle_hash = core
            .transport
            .as_ref()
            .unwrap()
            .catalogue_state_hash_for_test();
        let expected_schema_hash = core.schema_manager().current_hash().to_string();
        let handle_schema_hash = core
            .transport
            .as_ref()
            .unwrap()
            .declared_schema_hash_for_test();
        assert_eq!(
            handle_hash.as_deref(),
            Some(expected_hash.as_str()),
            "install_transport must seed the handle's catalogue_state_hash",
        );
        assert_eq!(
            handle_schema_hash.as_deref(),
            Some(expected_schema_hash.as_str()),
            "install_transport must seed the handle's declared_schema_hash",
        );
    }

    #[test]
    fn install_transport_holds_initial_remote_query_frontier_while_connecting() {
        let mut core = create_test_runtime();

        let _manager = crate::runtime_core::install_transport::<_, _, NopStreamAdapter, _>(
            &mut core,
            "ws://example.test/ws".to_string(),
            AuthConfig::default(),
            NopTick,
        );

        let mut future = core.query_with_propagation(
            Query::new("users"),
            None,
            ReadDurabilityOptions {
                tier: Some(DurabilityTier::EdgeServer),
                local_updates: crate::query_manager::manager::LocalUpdates::Immediate,
            },
            crate::sync_manager::QueryPropagation::Full,
        );

        let waker = noop_waker();
        let mut cx = std::task::Context::from_waker(&waker);
        assert!(
            std::pin::Pin::new(&mut future).poll(&mut cx).is_pending(),
            "remote query should stay pending until the transport finishes connecting"
        );
    }

    /// Guards the fix for CI expo-e2e failing when the WS transport never
    /// completes: after the pending-server timeout elapses and any subsequent
    /// tick runs, a held initial subscription must actually deliver against
    /// local state — not just flip an internal flag.
    #[test]
    fn pending_server_frontier_releases_after_timeout() {
        use crate::sync_manager::PENDING_SERVER_TIMEOUT;

        let mut core = create_test_runtime();

        let (alice, _row_values) = core
            .insert("users", user_insert_values(ObjectId::new(), "Alice"), None)
            .unwrap()
            .0;

        let _manager = crate::runtime_core::install_transport::<_, _, NopStreamAdapter, _>(
            &mut core,
            "ws://example.test/ws".to_string(),
            AuthConfig::default(),
            NopTick,
        );

        let mut future = core.query_with_propagation(
            Query::new("users"),
            None,
            ReadDurabilityOptions {
                tier: Some(DurabilityTier::EdgeServer),
                local_updates: crate::query_manager::manager::LocalUpdates::Immediate,
            },
            crate::sync_manager::QueryPropagation::Full,
        );

        let waker = noop_waker();
        let mut cx = std::task::Context::from_waker(&waker);
        assert!(
            std::pin::Pin::new(&mut future).poll(&mut cx).is_pending(),
            "remote query must stay held while transport is pending"
        );

        std::thread::sleep(PENDING_SERVER_TIMEOUT + std::time::Duration::from_millis(100));

        // The timeout is a passive check; something must drive a settle after
        // the deadline. In production any ambient activity does this; here we
        // trigger an explicit tick.
        core.immediate_tick();

        match std::pin::Pin::new(&mut future).poll(&mut cx) {
            std::task::Poll::Ready(Ok(rows)) => {
                assert_eq!(rows.len(), 1, "held subscription must deliver Alice");
                assert_eq!(rows[0].0, alice);
            }
            other => panic!("expected Ready(Ok(_)) after timeout release, got {other:?}"),
        }
    }

    /// Shared body for terminal-transport-event release tests. Asserts that
    /// dispatching `event` unblocks a held initial subscription so it delivers
    /// the local row. `event_label` is used only for the panic message.
    fn assert_event_releases_held_subscription(
        event: crate::transport_manager::TransportInbound,
        event_label: &str,
    ) {
        let mut core = create_test_runtime();

        let (alice, _row_values) = core
            .insert("users", user_insert_values(ObjectId::new(), "Alice"), None)
            .unwrap()
            .0;

        let _manager = crate::runtime_core::install_transport::<_, _, NopStreamAdapter, _>(
            &mut core,
            "ws://example.test/ws".to_string(),
            AuthConfig::default(),
            NopTick,
        );

        let server_id = core.transport.as_ref().unwrap().server_id;

        let mut future = core.query_with_propagation(
            Query::new("users"),
            None,
            ReadDurabilityOptions {
                tier: Some(DurabilityTier::EdgeServer),
                local_updates: crate::query_manager::manager::LocalUpdates::Immediate,
            },
            crate::sync_manager::QueryPropagation::Full,
        );

        let waker = noop_waker();
        let mut cx = std::task::Context::from_waker(&waker);
        assert!(
            std::pin::Pin::new(&mut future).poll(&mut cx).is_pending(),
            "remote query must stay held while transport is pending"
        );

        core.handle_transport_inbound_for_test(server_id, event);

        match std::pin::Pin::new(&mut future).poll(&mut cx) {
            std::task::Poll::Ready(Ok(rows)) => {
                assert_eq!(rows.len(), 1, "held subscription must deliver Alice");
                assert_eq!(rows[0].0, alice);
            }
            other => panic!("expected Ready(Ok(_)) after {event_label} release, got {other:?}"),
        }
    }

    /// When the transport emits `ConnectFailed` (offline DNS/TCP/TLS error
    /// before the timeout), draining the event must release the held initial
    /// subscription *and* deliver its first batch against local state. Flipping
    /// the pending-server flag is not enough on its own — release also has to
    /// re-run `process()` so `settle()` observes the state change.
    #[test]
    fn connect_failed_event_releases_and_delivers_held_subscription() {
        assert_event_releases_held_subscription(
            crate::transport_manager::TransportInbound::ConnectFailed {
                reason: "dns lookup failed".into(),
            },
            "ConnectFailed",
        );
    }

    /// When the transport emits `AuthFailure` (server rejected the JWT —
    /// e.g. expired token, wrong audience), draining the event must both
    /// tear down the server registration *and* release any held initial
    /// subscriptions so local rows become visible. Without this, a signed-in
    /// user whose token is rejected at handshake time would see an empty
    /// UI instead of their local-first data.
    #[test]
    fn auth_failure_event_releases_and_delivers_held_subscription() {
        assert_event_releases_held_subscription(
            crate::transport_manager::TransportInbound::AuthFailure {
                reason: "jwt rejected".into(),
            },
            "AuthFailure",
        );
    }
}
