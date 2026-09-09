//! Shared test transports, fixtures, and assertion helpers.

use super::*;

/// Receive the next subscriber payload relevant to direct protocol assertions.
/// A subscriber begins by publishing its trusted catalogue prerequisite; tests
/// that do not model a receiving `Db` still need to consume that control-plane
/// message before asserting the requested registration/subscription response.
pub(super) fn try_recv_subscriber_payload(transport: &mut dyn Transport) -> Option<SyncMessage> {
    loop {
        match transport.try_recv()? {
            SyncMessage::CatalogueSnapshot(_) => continue,
            message => return Some(message),
        }
    }
}

/// Drive a raw subscriber connection across storage wakeups until it emits a
/// protocol payload. A connection tick may install a query whose hydration
/// suspends; the storage wake schedules the continuation rather than forcing
/// that potentially slow read into the registration tick.
pub(super) fn drive_subscriber_until_payload<S>(
    subscriber: &Rc<LocalMutex<PeerConnection<S>>>,
    transport: &mut dyn Transport,
) -> SyncMessage
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    drive_subscriber_until_payloads(subscriber, transport, 1)
        .pop()
        .expect("one requested subscriber payload")
}

pub(super) fn drive_subscriber_until_payloads<S>(
    subscriber: &Rc<LocalMutex<PeerConnection<S>>>,
    transport: &mut dyn Transport,
    count: usize,
) -> Vec<SyncMessage>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    let mut messages = Vec::with_capacity(count);
    for _ in 0..32 {
        while let Some(message) = try_recv_subscriber_payload(transport) {
            messages.push(message);
            if messages.len() == count {
                return messages;
            }
        }
        subscriber.borrow_mut().tick().unwrap();
    }
    panic!(
        "subscriber emitted only {} of {count} payloads after 32 scheduled progress turns",
        messages.len()
    )
}

pub(super) struct BackpressureOnceTransport {
    pub(super) outbound: Rc<RefCell<std::collections::VecDeque<SyncMessage>>>,
    pub(super) failed: bool,
}

impl Transport for BackpressureOnceTransport {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        if !self.failed {
            self.failed = true;
            return Err(TransportError::Backpressure);
        }
        self.outbound.borrow_mut().push_back(message);
        Ok(())
    }

    fn try_recv(&mut self) -> Option<SyncMessage> {
        None
    }
}

/// Byte transport pair: each side sends postcard-encoded frames to the
/// other's staged inbound queue.
pub(super) struct ByteDuplexTransport {
    pub(super) outbound: Rc<RefCell<std::collections::VecDeque<Vec<u8>>>>,
    pub(super) inbound: Rc<RefCell<std::collections::VecDeque<Vec<u8>>>>,
}

pub(super) struct OneShotBackpressureTransport {
    pub(super) outbound: Rc<RefCell<std::collections::VecDeque<Vec<u8>>>>,
    pub(super) calls: usize,
    pub(super) fail_on_call: usize,
    pub(super) failed: bool,
}

impl WireTransport for OneShotBackpressureTransport {
    fn send_frame(&mut self, frame: Vec<u8>) -> Result<(), TransportError> {
        self.calls += 1;
        if self.calls == self.fail_on_call && !self.failed {
            self.failed = true;
            return Err(TransportError::Backpressure);
        }
        self.outbound.borrow_mut().push_back(frame);
        Ok(())
    }

    fn try_recv_frame(&mut self) -> Option<Vec<u8>> {
        None
    }
}

impl WireTransport for ByteDuplexTransport {
    fn send_frame(&mut self, frame: Vec<u8>) -> Result<(), TransportError> {
        self.outbound.borrow_mut().push_back(frame);
        Ok(())
    }

    fn try_recv_frame(&mut self) -> Option<Vec<u8>> {
        self.inbound.borrow_mut().pop_front()
    }
}

pub(super) fn byte_duplex_raw() -> (ByteDuplexTransport, ByteDuplexTransport) {
    use std::collections::VecDeque;
    let left = Rc::new(RefCell::new(VecDeque::new()));
    let right = Rc::new(RefCell::new(VecDeque::new()));
    (
        ByteDuplexTransport {
            outbound: Rc::clone(&left),
            inbound: Rc::clone(&right),
        },
        ByteDuplexTransport {
            outbound: right,
            inbound: left,
        },
    )
}

pub(super) fn byte_duplex() -> (Box<dyn Transport>, Box<dyn Transport>) {
    let (left, right) = byte_duplex_raw();
    (
        Box::new(WireTransportAdapter::current(left)),
        Box::new(WireTransportAdapter::current(right)),
    )
}

pub(super) fn byte_duplex_uncompressed() -> (Box<dyn Transport>, Box<dyn Transport>) {
    let (left, right) = byte_duplex_raw();
    let features =
        FEATURE_SYNC_MESSAGE_PAYLOAD | FEATURE_STRUCTURED_ERRORS | FEATURE_MESSAGE_FRAGMENTATION;
    (
        Box::new(WireTransportAdapter::new(
            left,
            WIRE_PROTOCOL_VERSION,
            features,
            None,
        )),
        Box::new(WireTransportAdapter::new(
            right,
            WIRE_PROTOCOL_VERSION,
            features,
            None,
        )),
    )
}

pub(super) fn rocks_storage(schema: &JazzSchema) -> RocksDbStorage {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.keep();
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    RocksDbStorage::open(&path, &refs).unwrap()
}

pub(super) fn open_db(node: u8, author: AuthorSubject, schema: &JazzSchema) -> Db<RocksDbStorage> {
    let storage = rocks_storage(schema);
    block_on(Db::open(DbConfig {
        schema: schema.clone(),
        storage,
        identity: DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author,
        },
        id_source: Some(Box::new(SeededRowIdSource::new(node as u64))),
    }))
    .unwrap()
}

/// Construct the explicit provider-side identity fixture used by DB tests.
///
/// Test `AuthorSubject`s identify Jazz provenance; application UUID columns
/// intentionally compare against the separately admitted provider `sub`.
/// This helper keeps that distinction visible without inventing a provider
/// `sub` from the logical author.
pub(super) fn test_provider_claims(author: AuthorSubject) -> BTreeMap<String, Value> {
    match author {
        AuthorSubject::System => BTreeMap::new(),
        AuthorSubject::SystemAt(_) => BTreeMap::new(),
        AuthorSubject::Authenticated(_) => BTreeMap::from([(
            crate::query::provider_claim_key("sub"),
            Value::Uuid(author.test_uuid()),
        )]),
    }
}

pub(super) fn row_ids(rows: &[CurrentRow]) -> Vec<RowUuid> {
    rows.iter().map(CurrentRow::row_uuid).collect()
}

/// In-memory transport pair: each side's outbound queue is the other's
/// inbound queue, so a `send` lands directly in the peer's `try_recv`.
pub(super) struct DuplexTransport {
    outbound: Rc<RefCell<std::collections::VecDeque<SyncMessage>>>,
    inbound: Rc<RefCell<std::collections::VecDeque<SyncMessage>>>,
    session_context: Option<ConnectionSessionContext>,
}

impl Transport for DuplexTransport {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        self.outbound.borrow_mut().push_back(message);
        Ok(())
    }

    fn try_recv(&mut self) -> Option<SyncMessage> {
        self.inbound.borrow_mut().pop_front()
    }

    fn connection_session_context(&self) -> Option<ConnectionSessionContext> {
        self.session_context
    }
}

pub(super) fn duplex() -> (Box<dyn Transport>, Box<dyn Transport>) {
    use std::collections::VecDeque;
    let left = Rc::new(RefCell::new(VecDeque::new()));
    let right = Rc::new(RefCell::new(VecDeque::new()));
    (
        Box::new(DuplexTransport {
            outbound: Rc::clone(&left),
            inbound: Rc::clone(&right),
            session_context: None,
        }),
        Box::new(DuplexTransport {
            outbound: right,
            inbound: left,
            session_context: None,
        }),
    )
}

/// A test-only authenticated scope-isolated relay link. Production admission
/// derives this capability from the handshake, never from SyncMessage input.
pub(super) fn relay_duplex() -> (Box<dyn Transport>, Box<dyn Transport>) {
    use std::collections::VecDeque;
    let left = Rc::new(RefCell::new(VecDeque::new()));
    let right = Rc::new(RefCell::new(VecDeque::new()));
    (
        Box::new(RelayDuplexTransport {
            outbound: Rc::clone(&left),
            inbound: Rc::clone(&right),
            session_context: None,
        }),
        Box::new(RelayDuplexTransport {
            outbound: right,
            inbound: left,
            session_context: None,
        }),
    )
}

struct RelayDuplexTransport {
    outbound: Rc<RefCell<std::collections::VecDeque<SyncMessage>>>,
    inbound: Rc<RefCell<std::collections::VecDeque<SyncMessage>>>,
    session_context: Option<ConnectionSessionContext>,
}

impl Transport for RelayDuplexTransport {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        self.outbound.borrow_mut().push_back(message);
        Ok(())
    }

    fn try_recv(&mut self) -> Option<SyncMessage> {
        self.inbound.borrow_mut().pop_front()
    }

    fn connection_session_context(&self) -> Option<ConnectionSessionContext> {
        self.session_context
    }

    fn permits_delegated_sessions(&self) -> bool {
        true
    }
}

pub(super) fn duplex_with_taps() -> (
    Box<dyn Transport>,
    Box<dyn Transport>,
    Rc<RefCell<std::collections::VecDeque<SyncMessage>>>,
    Rc<RefCell<std::collections::VecDeque<SyncMessage>>>,
) {
    use std::collections::VecDeque;
    let client_to_server = Rc::new(RefCell::new(VecDeque::new()));
    let server_to_client = Rc::new(RefCell::new(VecDeque::new()));
    (
        Box::new(DuplexTransport {
            outbound: Rc::clone(&client_to_server),
            inbound: Rc::clone(&server_to_client),
            session_context: None,
        }),
        Box::new(DuplexTransport {
            outbound: Rc::clone(&server_to_client),
            inbound: Rc::clone(&client_to_server),
            session_context: None,
        }),
        client_to_server,
        server_to_client,
    )
}

/// In-memory transport pair with a read-only tap on server-to-client frames.
/// The tap lets a Core-serving test inspect the canonical `ViewUpdate` before
/// the receiving edge applies it.
pub(super) fn duplex_with_server_outbound_tap() -> (
    Box<dyn Transport>,
    Box<dyn Transport>,
    Rc<RefCell<std::collections::VecDeque<SyncMessage>>>,
) {
    use std::collections::VecDeque;
    let client_to_server = Rc::new(RefCell::new(VecDeque::new()));
    let server_to_client = Rc::new(RefCell::new(VecDeque::new()));
    (
        Box::new(DuplexTransport {
            outbound: Rc::clone(&client_to_server),
            inbound: Rc::clone(&server_to_client),
            session_context: None,
        }),
        Box::new(DuplexTransport {
            outbound: Rc::clone(&server_to_client),
            inbound: client_to_server,
            session_context: None,
        }),
        server_to_client,
    )
}

/// In-memory transport pair with a read-only tap on client-to-server frames.
/// The tap lets an Edge test inspect an upstream upload before Core applies it.
pub(super) fn duplex_with_client_outbound_tap() -> (
    Box<dyn Transport>,
    Box<dyn Transport>,
    Rc<RefCell<std::collections::VecDeque<SyncMessage>>>,
) {
    use std::collections::VecDeque;
    let client_to_server = Rc::new(RefCell::new(VecDeque::new()));
    let server_to_client = Rc::new(RefCell::new(VecDeque::new()));
    (
        Box::new(DuplexTransport {
            outbound: Rc::clone(&client_to_server),
            inbound: Rc::clone(&server_to_client),
            session_context: None,
        }),
        Box::new(DuplexTransport {
            outbound: server_to_client,
            inbound: Rc::clone(&client_to_server),
            session_context: None,
        }),
        client_to_server,
    )
}

/// In-memory handshake pairing needs an internal test because it verifies the
/// transport/admission boundary before any user-visible sync payload exists.
pub(super) fn duplex_with_admitted_session_context(
    identity: AuthorSubject,
    client_node: NodeUuid,
    client_epoch: u64,
    server_node: NodeUuid,
    server_epoch: u64,
) -> (Box<dyn Transport>, Box<dyn Transport>) {
    use std::collections::VecDeque;
    let left = Rc::new(RefCell::new(VecDeque::new()));
    let right = Rc::new(RefCell::new(VecDeque::new()));
    let features = crate::wire::FEATURE_AUTHORIZATION_SCOPE_VIEWS;
    let client = ConnectionSessionContext {
        local: crate::wire::WireAuthorityEndpoint {
            node: client_node,
            epoch: client_epoch,
        },
        remote: Some(crate::wire::WireAuthorityEndpoint {
            node: server_node,
            epoch: server_epoch,
        }),
        link_identity: identity,
        negotiated_features: features,
    };
    let server = ConnectionSessionContext {
        local: client.remote.unwrap(),
        remote: Some(client.local),
        link_identity: identity,
        negotiated_features: features,
    };
    (
        Box::new(DuplexTransport {
            outbound: Rc::clone(&left),
            inbound: Rc::clone(&right),
            session_context: Some(client),
        }),
        Box::new(DuplexTransport {
            outbound: right,
            inbound: left,
            session_context: Some(server),
        }),
    )
}

/// Authenticated in-memory transport pair with a read-only client-to-server
/// tap. This keeps reconnect tests at the real session-context boundary while
/// exposing only the wire frames they need to assert.
pub(super) fn duplex_with_admitted_session_context_and_client_outbound_tap(
    identity: AuthorSubject,
    client_node: NodeUuid,
    client_epoch: u64,
    server_node: NodeUuid,
    server_epoch: u64,
) -> (
    Box<dyn Transport>,
    Box<dyn Transport>,
    Rc<RefCell<std::collections::VecDeque<SyncMessage>>>,
) {
    use std::collections::VecDeque;
    let client_to_server = Rc::new(RefCell::new(VecDeque::new()));
    let server_to_client = Rc::new(RefCell::new(VecDeque::new()));
    let features = crate::wire::FEATURE_AUTHORIZATION_SCOPE_VIEWS;
    let client = ConnectionSessionContext {
        local: crate::wire::WireAuthorityEndpoint {
            node: client_node,
            epoch: client_epoch,
        },
        remote: Some(crate::wire::WireAuthorityEndpoint {
            node: server_node,
            epoch: server_epoch,
        }),
        link_identity: identity,
        negotiated_features: features,
    };
    let server = ConnectionSessionContext {
        local: client.remote.unwrap(),
        remote: Some(client.local),
        link_identity: identity,
        negotiated_features: features,
    };
    (
        Box::new(DuplexTransport {
            outbound: Rc::clone(&client_to_server),
            inbound: Rc::clone(&server_to_client),
            session_context: Some(client),
        }),
        Box::new(DuplexTransport {
            outbound: server_to_client,
            inbound: Rc::clone(&client_to_server),
            session_context: Some(server),
        }),
        client_to_server,
    )
}

pub(super) fn block_on<F: Future>(future: F) -> F::Output {
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    let mut future = pin!(future);
    loop {
        match future.as_mut().poll(&mut cx) {
            Poll::Ready(value) => return value,
            Poll::Pending => std::thread::yield_now(),
        }
    }
}

pub(super) fn apply_subscription_event(snapshot: &mut RelationSnapshot, event: SubscriptionEvent) {
    match event {
        SubscriptionEvent::Delta {
            reset,
            added,
            updated,
            removed,
            ..
        } => {
            if reset {
                snapshot.rows.clear();
                snapshot.edges.clear();
                snapshot.root_count = 0;
            }

            for removed in removed {
                if let Some(position) =
                    snapshot
                        .rows
                        .iter()
                        .take(snapshot.root_count)
                        .position(|row| {
                            row.table() == removed.table && row.row_uuid() == removed.row_uuid
                        })
                {
                    snapshot.rows.remove(position);
                    snapshot.root_count -= 1;
                }
            }

            for row in updated {
                let row = row.row;
                if let Some(position) = snapshot.rows.iter().position(|current| {
                    current.table() == row.table() && current.row_uuid() == row.row_uuid()
                }) {
                    snapshot.rows[position] = row;
                }
            }

            for row in added {
                let row = row.row;
                if let Some(position) =
                    snapshot
                        .rows
                        .iter()
                        .take(snapshot.root_count)
                        .position(|current| {
                            current.table() == row.table() && current.row_uuid() == row.row_uuid()
                        })
                {
                    snapshot.rows[position] = row;
                } else {
                    snapshot.rows.insert(snapshot.root_count, row);
                    snapshot.root_count += 1;
                }
            }
        }
        SubscriptionEvent::Rejected { reason } => {
            panic!("unexpected subscription rejection while applying delta: {reason:?}")
        }
        SubscriptionEvent::Closed => {}
    }
}

/// Check the first public delivery after the fixture has driven synchronization.
/// Polling instead of blocking makes missing progress fail here, not hang the test.
pub(super) fn next_settled_opening(stream: &mut SubscriptionStream) -> SubscriptionEvent {
    let event = stream
        .try_next_event()
        .expect("settled subscription opening after synchronization");
    assert!(
        matches!(
            &event,
            SubscriptionEvent::Delta {
                reset: true,
                publishable: true,
                settled: true,
                ..
            }
        ),
        "expected a complete settled opening, got {event:?}"
    );
    event
}

pub(super) fn opened_rows(event: SubscriptionEvent) -> Vec<CurrentRow> {
    let mut snapshot = RelationSnapshot::default();
    apply_subscription_event(&mut snapshot, event);
    snapshot.rows
}

pub(super) fn pending_upstream_subscribe_count<S>(db: &Db<S>) -> usize
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    db.node
        .upstream_subscriptions
        .borrow()
        .iter()
        .filter(|command| matches!(command, PendingUpstreamCommand::Subscribe(_)))
        .count()
}

pub(super) fn pending_upstream_unsubscribe_count<S>(db: &Db<S>) -> usize
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    db.node
        .upstream_subscriptions
        .borrow()
        .iter()
        .filter(|command| matches!(command, PendingUpstreamCommand::Unsubscribe(_)))
        .count()
}

pub(super) fn decode_wire_message_payload(
    decoder: &mut WireStreamDecoder,
    envelope: &crate::wire::WireEnvelope,
) -> SyncMessage {
    let payload = decoder
        .decode_message(&envelope.payload, envelope.features)
        .unwrap();
    decode_sync_message(&payload).unwrap()
}

pub(super) fn delta_rows(
    event: SubscriptionEvent,
) -> (Vec<CurrentRow>, Vec<CurrentRow>, Vec<RemovedRow>) {
    match event {
        SubscriptionEvent::Delta {
            added,
            updated,
            removed,
            ..
        } => (
            added.into_iter().map(|output| output.row).collect(),
            updated.into_iter().map(|output| output.row).collect(),
            removed,
        ),
        other => panic!("expected subscription delta event, got {other:?}"),
    }
}

pub(super) fn snapshot_from_event(event: SubscriptionEvent) -> RelationSnapshot {
    let mut snapshot = RelationSnapshot::default();
    apply_subscription_event(&mut snapshot, event);
    snapshot
}

/// A maintained union emits a suffix removal addressed to its typed union arm.
/// Alice's one source row appears through `left` and `right` union arms; only
/// the left occurrence leaves the ordered result.
pub(super) fn terminal_nested_text_values(
    snapshot: &RelationSnapshot,
    root: RowUuid,
    relation: &str,
    column: &str,
) -> Vec<String> {
    let row = snapshot
        .rows
        .iter()
        .take(snapshot.root_count)
        .find(|row| row.row_uuid() == root)
        .expect("terminal root row");
    let (descriptor, raw) = row.encoded_record();
    let record = groove::records::BorrowedRecord::new(raw, descriptor);
    let Value::Array(children) = record.get(relation).expect("nested terminal field") else {
        panic!("nested terminal field must be an array")
    };
    children
        .into_iter()
        .map(|child| {
            let Value::Record(child) = child else {
                panic!("nested terminal array must contain records")
            };
            let Value::String(value) = child.get(column).expect("nested text field") else {
                panic!("nested terminal field must be text")
            };
            value
        })
        .collect()
}

pub(super) fn terminal_nested_values(
    snapshot: &RelationSnapshot,
    root: RowUuid,
    relation: &str,
    column: &str,
) -> Vec<Value> {
    let row = snapshot
        .rows
        .iter()
        .take(snapshot.root_count)
        .find(|row| row.row_uuid() == root)
        .expect("terminal root row");
    let (descriptor, raw) = row.encoded_record();
    let record = groove::records::BorrowedRecord::new(raw, descriptor);
    let Value::Array(children) = record.get(relation).expect("nested terminal field") else {
        panic!("nested terminal field must be an array")
    };
    children
        .into_iter()
        .map(|child| {
            let Value::Record(child) = child else {
                panic!("nested terminal array must contain records")
            };
            child.get(column).unwrap_or_else(|error| {
                panic!(
                    "nested field {column:?} missing from {:?}: {error}",
                    child.descriptor()
                )
            })
        })
        .collect()
}

pub(super) fn oversized_row_version_refs(len: usize) -> Vec<RowVersionRef> {
    (0..len)
        .map(|idx| {
            RowVersionRef::new(
                "todos",
                RowUuid(uuid::Uuid::from_u128(idx as u128 + 1)),
                TxId::new(
                    crate::time::TxTime(idx as u64 + 1),
                    NodeUuid::from_bytes([0x44; 16]),
                ),
            )
        })
        .collect()
}

pub(super) fn event_settled(event: &SubscriptionEvent) -> bool {
    match event {
        SubscriptionEvent::Delta { settled, .. } => *settled,
        SubscriptionEvent::Rejected { .. } => false,
        SubscriptionEvent::Closed => false,
    }
}

pub(super) fn global_subscribe_opts() -> ReadOpts {
    ReadOpts {
        tier: DurabilityTier::Global,
        local_updates: LocalUpdates::Deferred,
        propagation: Propagation::Full,
        include_deleted: false,
        ..ReadOpts::default()
    }
}

pub(super) fn edge_subscribe_opts() -> ReadOpts {
    ReadOpts {
        tier: DurabilityTier::Edge,
        local_updates: LocalUpdates::Deferred,
        propagation: Propagation::Full,
        include_deleted: false,
        ..ReadOpts::default()
    }
}

pub(super) fn assert_unsupported_subscription_include_deleted(error: Error) {
    assert_eq!(error.code, ErrorCode::Query);
    assert!(
        error.message.contains("include_deleted"),
        "unexpected error message: {}",
        error.message
    );
}

pub(super) fn assert_subscribe_rejected_unsupported_shape_capability_detail(
    message: SyncMessage,
    expected_subscription: SubscriptionKey,
    expected_detail: &str,
) {
    match message {
        SyncMessage::SubscribeRejected {
            subscription,
            reason: SubscribeRejectReason::UnsupportedShapeCapability { detail },
        } => {
            assert_eq!(subscription, expected_subscription);
            assert!(
                detail.contains(expected_detail),
                "unexpected rejection detail: {detail}"
            );
        }
        other => panic!("expected SubscribeRejected, got {other:?}"),
    }
}

pub(super) fn assert_view_update_for_subscription(
    message: SyncMessage,
    expected_subscription: SubscriptionKey,
) {
    match message {
        SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload { subscription, .. }) => {
            assert_eq!(subscription, expected_subscription);
        }
        other => panic!("expected ViewUpdate, got {other:?}"),
    }
}

pub(super) fn expect_error<T>(result: Result<T, Error>) -> Error {
    match result {
        Ok(_) => panic!("expected operation to fail"),
        Err(error) => error,
    }
}

pub(super) fn prepared<S>(db: &Db<S>, query: &Query) -> PreparedQuery
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    db.prepare_query(query).unwrap()
}

pub(super) fn prepared_read<S>(db: &Db<S>, query: &Query) -> Vec<CurrentRow>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    let prepared = prepared(db, query);
    db.read(&prepared).unwrap()
}

pub(super) fn prepared_one<S>(db: &Db<S>, query: &Query) -> Option<CurrentRow>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    let prepared = prepared(db, query);
    db.one(&prepared).unwrap()
}

pub(super) fn prepared_all<S>(db: &Db<S>, query: &Query, opts: ReadOpts) -> Vec<CurrentRow>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    let prepared = prepared(db, query);
    block_on(db.all(&prepared, opts)).unwrap()
}

pub(super) fn prepared_subscribe<S>(
    db: &Db<S>,
    query: &Query,
    opts: ReadOpts,
) -> Result<SubscriptionStream, Error>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    let prepared = prepared(db, query);
    block_on(db.subscribe(&prepared, opts))
}

#[derive(Default)]
pub(super) struct RecordingScheduler {
    calls: RefCell<Vec<TickUrgency>>,
    delayed_calls_ms: RefCell<Vec<u64>>,
}

impl TickScheduler for RecordingScheduler {
    fn schedule_tick(&self, urgency: TickUrgency) {
        self.calls.borrow_mut().push(urgency);
    }

    fn schedule_tick_after(&self, delay_ms: u64) {
        self.delayed_calls_ms.borrow_mut().push(delay_ms);
    }
}

impl RecordingScheduler {
    pub(super) fn take(&self) -> Vec<TickUrgency> {
        std::mem::take(&mut self.calls.borrow_mut())
    }

    pub(super) fn take_delays(&self) -> Vec<u64> {
        std::mem::take(&mut self.delayed_calls_ms.borrow_mut())
    }
}

pub(super) fn compile_public_db_test_schema(source: &PublicSchema) -> JazzSchema {
    crate::schema::JazzSchema::new(source).expect("db-test public schema compiles")
}

pub(super) fn build_public_db_test_schema(builder: PublicSchemaBuilder) -> JazzSchema {
    compile_public_db_test_schema(&builder.build())
}

pub(super) fn public_session_eq(column: &str, path: &[&str]) -> PublicPolicyExpr {
    let path = path.iter().map(|segment| (*segment).to_owned()).collect();
    PublicPolicyExpr::eq_session(column, path)
}

pub(super) fn public_outer_eq(column: &str, outer_column: &str) -> PublicPolicyExpr {
    public_session_eq(column, &["__jazz_outer_row", outer_column])
}

pub(super) fn public_literal_eq(column: &str, value: PublicValue) -> PublicPolicyExpr {
    PublicPolicyExpr::Cmp {
        column: column.to_owned(),
        op: PublicCmpOp::Eq,
        value: PublicPolicyValue::Literal(value),
    }
}

pub(super) fn public_exists(
    table: &str,
    conditions: impl IntoIterator<Item = PublicPolicyExpr>,
) -> PublicPolicyExpr {
    PublicPolicyExpr::Exists {
        table: table.to_owned(),
        condition: Box::new(PublicPolicyExpr::and(conditions.into_iter().collect())),
    }
}

fn public_rel_column(scope: &str, column: &str) -> PublicRelColumnRef {
    PublicRelColumnRef {
        scope: Some(scope.to_owned()),
        column: column.to_owned(),
    }
}

fn public_rel_eq(scope: &str, column: &str, value: PublicRelValueRef) -> PublicRelPredicateExpr {
    PublicRelPredicateExpr::Cmp {
        left: public_rel_column(scope, column),
        op: PublicRelPredicateCmpOp::Eq,
        right: value,
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn public_recursive_access_policy(
    access_table: &str,
    access_row_column: &str,
    access_team_column: &str,
    access_filters: &[(&str, PublicValue)],
    access_in_filters: &[(&str, Vec<PublicValue>)],
    team_table: &str,
    edge_table: &str,
    edge_member_column: &str,
    edge_parent_column: &str,
    edge_filters: &[(&str, PublicValue)],
    seed_table: &str,
    seed_user_column: &str,
    seed_claim_path: &[&str],
    seed_team_column: &str,
) -> PublicPolicyExpr {
    let seed_alias = "seed";
    let edge_alias = "recursive_edge";
    let target_alias = "recursive_target";
    let access_alias = "access";
    let seed = PublicRelExpr::Project {
        input: Box::new(PublicRelExpr::Filter {
            input: Box::new(PublicRelExpr::TableScan {
                table: seed_table.into(),
                alias: Some(seed_alias.to_owned()),
            }),
            predicate: public_rel_eq(
                seed_alias,
                seed_user_column,
                PublicRelValueRef::SessionRef(
                    seed_claim_path
                        .iter()
                        .map(|segment| (*segment).to_owned())
                        .collect(),
                ),
            ),
        }),
        columns: vec![PublicRelProjectColumn {
            alias: "id".to_owned(),
            expr: PublicRelProjectExpr::Column(public_rel_column(seed_alias, seed_team_column)),
        }],
    };
    let mut edge_predicates = vec![public_rel_eq(
        edge_alias,
        edge_member_column,
        PublicRelValueRef::RowId(PublicRelRowIdRef::Frontier),
    )];
    edge_predicates.extend(edge_filters.iter().map(|(column, value)| {
        public_rel_eq(
            edge_alias,
            column,
            PublicRelValueRef::Literal(value.clone()),
        )
    }));
    let step = PublicRelExpr::Project {
        input: Box::new(PublicRelExpr::Join {
            left: Box::new(PublicRelExpr::Filter {
                input: Box::new(PublicRelExpr::TableScan {
                    table: edge_table.into(),
                    alias: Some(edge_alias.to_owned()),
                }),
                predicate: PublicRelPredicateExpr::And(edge_predicates),
            }),
            right: Box::new(PublicRelExpr::TableScan {
                table: team_table.into(),
                alias: Some(target_alias.to_owned()),
            }),
            on: vec![PublicRelJoinCondition {
                left: public_rel_column(edge_alias, edge_parent_column),
                right: public_rel_column(target_alias, "id"),
            }],
            join_kind: PublicRelJoinKind::Inner,
        }),
        columns: vec![PublicRelProjectColumn {
            alias: "id".to_owned(),
            expr: PublicRelProjectExpr::Column(public_rel_column(target_alias, "id")),
        }],
    };
    let reachable = PublicRelExpr::Gather {
        seed: Box::new(seed),
        step: Box::new(step),
        frontier_key: PublicRelKeyRef::RowId(PublicRelRowIdRef::Current),
        bound: PublicRelRecursionBound::MaxDepth(8),
        dedupe_key: vec![PublicRelKeyRef::RowId(PublicRelRowIdRef::Current)],
    };
    let mut access_predicates = vec![public_rel_eq(
        access_alias,
        access_row_column,
        PublicRelValueRef::RowId(PublicRelRowIdRef::Outer),
    )];
    access_predicates.extend(access_filters.iter().map(|(column, value)| {
        public_rel_eq(
            access_alias,
            column,
            PublicRelValueRef::Literal(value.clone()),
        )
    }));
    access_predicates.extend(access_in_filters.iter().map(|(column, values)| {
        PublicRelPredicateExpr::In {
            left: public_rel_column(access_alias, column),
            values: values
                .iter()
                .cloned()
                .map(PublicRelValueRef::Literal)
                .collect(),
        }
    }));
    PublicPolicyExpr::ExistsRel {
        rel: PublicRelExpr::Filter {
            input: Box::new(PublicRelExpr::Join {
                left: Box::new(reachable),
                right: Box::new(PublicRelExpr::TableScan {
                    table: access_table.into(),
                    alias: Some(access_alias.to_owned()),
                }),
                on: vec![PublicRelJoinCondition {
                    left: PublicRelColumnRef {
                        scope: None,
                        column: "id".to_owned(),
                    },
                    right: public_rel_column(access_alias, access_team_column),
                }],
                join_kind: PublicRelJoinKind::Inner,
            }),
            predicate: PublicRelPredicateExpr::And(access_predicates),
        },
    }
}

pub(super) fn public_legacy_write_policy(expr: PublicPolicyExpr) -> PublicTablePolicies {
    PublicTablePolicies::new()
        .with_insert(expr.clone())
        .with_update(Some(expr.clone()), expr.clone())
        .with_delete(expr)
}

pub(super) fn schema() -> JazzSchema {
    build_public_db_test_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("todos")
                .column("title", PublicColumnType::Text)
                .column("done", PublicColumnType::Boolean)
                .column("owner", PublicColumnType::Uuid),
        ),
    )
}

pub(super) fn payload_enum_query_schema() -> JazzSchema {
    build_public_db_test_schema(PublicSchemaBuilder::new().table(
        PublicTableSchemaBuilder::new("events").column(
            "event",
            PublicColumnType::EnumPayload {
                cases: vec![
                    PublicEnumCaseDescriptor {
                        name: "message".to_owned(),
                        fields: vec![PublicColumnDescriptor::new(
                            "level",
                            PublicColumnType::Integer,
                        )],
                    },
                    PublicEnumCaseDescriptor {
                        name: "closed".to_owned(),
                        fields: vec![PublicColumnDescriptor::new(
                            "code",
                            PublicColumnType::Integer,
                        )],
                    },
                ],
            },
        ),
    ))
}

pub(super) fn payload_message(level: i32) -> Value {
    Value::Enum(
        EnumValue::create(
            0,
            RecordDescriptor::new([("level", ValueType::I32)]),
            &[Value::I32(level)],
        )
        .unwrap(),
    )
}

pub(super) fn payload_closed(code: i32) -> Value {
    Value::Enum(
        EnumValue::create(
            1,
            RecordDescriptor::new([("code", ValueType::I32)]),
            &[Value::I32(code)],
        )
        .unwrap(),
    )
}

pub(super) fn empty_payload_case(discriminant: u32) -> Value {
    Value::Enum(
        EnumValue::create(
            discriminant,
            RecordDescriptor::new(Vec::<(String, ValueType)>::new()),
            &[],
        )
        .unwrap(),
    )
}

pub(super) fn owner_read_schema() -> JazzSchema {
    build_public_db_test_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("todos")
                .column("title", PublicColumnType::Text)
                .column("done", PublicColumnType::Boolean)
                .column("owner", PublicColumnType::Uuid)
                .policies(
                    PublicTablePolicies::new()
                        .with_select(public_session_eq("owner", &["claims", "sub"])),
                ),
        ),
    )
}

pub(super) fn created_by_read_schema() -> JazzSchema {
    created_by_read_schema_for_claim("user")
}

pub(super) fn created_by_read_schema_for_claim(claim_name: &str) -> JazzSchema {
    let session_path = if claim_name == "user" {
        vec!["user"]
    } else {
        vec!["claims", claim_name]
    };
    build_public_db_test_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("todos")
                .column("title", PublicColumnType::Text)
                .column("done", PublicColumnType::Boolean)
                .policies(PublicTablePolicies::new().with_select(public_session_eq(
                    if claim_name == "user" {
                        "$createdBy"
                    } else {
                        "$createdBy.identity.subject"
                    },
                    &session_path,
                ))),
        ),
    )
}

pub(super) fn owner_write_schema() -> JazzSchema {
    build_public_db_test_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("todos")
                .column("title", PublicColumnType::Text)
                .column("done", PublicColumnType::Boolean)
                .column("owner", PublicColumnType::Uuid)
                .policies(public_legacy_write_policy(public_session_eq(
                    "owner",
                    &["claims", "sub"],
                ))),
        ),
    )
}

pub(super) fn editor_claim_write_schema() -> JazzSchema {
    let editor = PublicPolicyExpr::SessionCmp {
        path: vec!["claims".to_owned(), "role".to_owned()],
        op: PublicCmpOp::Eq,
        value: PublicValue::Text("editor".to_owned()),
    };
    build_public_db_test_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("todos")
                .column("title", PublicColumnType::Text)
                .column("done", PublicColumnType::Boolean)
                .column("owner", PublicColumnType::Uuid)
                .policies(public_legacy_write_policy(editor)),
        ),
    )
}

pub(super) fn owner_id_read_schema() -> JazzSchema {
    build_public_db_test_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("messages")
                .column("body", PublicColumnType::Text)
                .column("owner_id", PublicColumnType::Text)
                .policies(
                    PublicTablePolicies::new()
                        .with_select(public_session_eq("owner_id", &["claims", "sub"]))
                        .with_insert(public_session_eq("owner_id", &["claims", "sub"])),
                ),
        ),
    )
}

pub(super) fn owner_id_public_schema() -> JazzSchema {
    build_public_db_test_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("messages")
                .column("body", PublicColumnType::Text)
                .column("owner_id", PublicColumnType::Text),
        ),
    )
}

pub(super) fn owner_id_session_write_schema() -> JazzSchema {
    build_public_db_test_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("messages")
                .column("body", PublicColumnType::Text)
                .column("owner_id", PublicColumnType::Text)
                .policies(public_legacy_write_policy(public_session_eq(
                    "owner_id",
                    &["claims", "sub"],
                ))),
        ),
    )
}

pub(super) fn owner_uuid_session_write_schema() -> JazzSchema {
    build_public_db_test_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("messages")
                .column("body", PublicColumnType::Text)
                .column("owner_id", PublicColumnType::Uuid)
                .policies(public_legacy_write_policy(public_session_eq(
                    "owner_id",
                    &["claims", "sub"],
                ))),
        ),
    )
}

pub(super) fn benchmark_shaped_recursive_reachable_read_schema() -> JazzSchema {
    let resource_policy = public_recursive_access_policy(
        "res_a_access_edges",
        "resource",
        "team",
        &[("administrator", PublicValue::Boolean(false))],
        &[],
        "group",
        "group_entry",
        "member_id",
        "target_id",
        &[("administrator", PublicValue::Boolean(false))],
        "group_access_edges",
        "user_id",
        &["claims", "sub"],
        "group_id",
    );
    build_public_db_test_schema(
        PublicSchemaBuilder::new()
            .table(public_resource_table_builder(
                "res_a",
                resource_policy,
                false,
            ))
            .table(PublicTableSchemaBuilder::new("group").column("name", PublicColumnType::Text))
            .table(
                PublicTableSchemaBuilder::new("group_access_edges")
                    .fk_column("group_id", "group")
                    .column("user_id", PublicColumnType::Uuid)
                    .column("role", PublicColumnType::Text),
            )
            .table(public_resource_access_table_builder(
                "res_a_access_edges",
                "res_a",
            ))
            .table(public_group_entry_table_builder()),
    )
}

fn public_resource_table_builder(
    name: &str,
    read_policy: PublicPolicyExpr,
    references_org: bool,
) -> PublicTableSchemaBuilder {
    let table = PublicTableSchemaBuilder::new(name);
    let table = if references_org {
        table.fk_column("org_id", "org")
    } else {
        table.column("org_id", PublicColumnType::Uuid)
    };
    table
        .fk_column("created_by", "group")
        .fk_column("updated_by", "group")
        .column("archived", PublicColumnType::Boolean)
        .column("label", PublicColumnType::Text)
        .column("date_created", PublicColumnType::Timestamp)
        .column("date_updated", PublicColumnType::Timestamp)
        .nullable_column("col_text_a", PublicColumnType::Text)
        .nullable_column("col_text_b", PublicColumnType::Text)
        .nullable_column("col_float", PublicColumnType::Double)
        .nullable_column("col_int", PublicColumnType::Timestamp)
        .nullable_column("col_json", PublicColumnType::Text)
        .nullable_column("col_tags", PublicColumnType::Text)
        .policies(PublicTablePolicies::new().with_select(read_policy))
}

fn public_resource_access_table_builder(
    name: &str,
    resource_table: &str,
) -> PublicTableSchemaBuilder {
    PublicTableSchemaBuilder::new(name)
        .fk_column("resource", resource_table)
        .fk_column("team", "group")
        .column("grant_role", PublicColumnType::Text)
        .column("administrator", PublicColumnType::Boolean)
}

fn public_group_entry_table_builder() -> PublicTableSchemaBuilder {
    PublicTableSchemaBuilder::new("group_entry")
        .fk_column("member_id", "group")
        .fk_column("target_id", "group")
        .column("administrator", PublicColumnType::Boolean)
        .column("date_added", PublicColumnType::Timestamp)
}

pub(super) fn customer_resource_policy_minimal_schema() -> JazzSchema {
    let resource_policy = public_recursive_access_policy(
        "res_i_access_edges",
        "resource",
        "team",
        &[("administrator", PublicValue::Boolean(false))],
        &[],
        "group",
        "group_entry",
        "member_id",
        "target_id",
        &[("administrator", PublicValue::Boolean(false))],
        "group_access_edges",
        "user_id",
        &["claims", "sub"],
        "group_id",
    );
    build_public_db_test_schema(
        PublicSchemaBuilder::new()
            .table(PublicTableSchemaBuilder::new("org").column("label", PublicColumnType::Text))
            .table(PublicTableSchemaBuilder::new("group").column("name", PublicColumnType::Text))
            .table(
                PublicTableSchemaBuilder::new("group_access_edges")
                    .fk_column("group_id", "group")
                    .column("user_id", PublicColumnType::Uuid)
                    .column("role", PublicColumnType::Text),
            )
            .table(public_group_entry_table_builder())
            .table(public_resource_table_builder(
                "res_i",
                resource_policy,
                true,
            ))
            .table(public_resource_access_table_builder(
                "res_i_access_edges",
                "res_i",
            )),
    )
}

pub(super) fn customer_two_resource_policy_minimal_schema() -> JazzSchema {
    let res_i_policy = public_recursive_access_policy(
        "res_i_access_edges",
        "resource",
        "team",
        &[("administrator", PublicValue::Boolean(false))],
        &[],
        "group",
        "group_entry",
        "member_id",
        "target_id",
        &[("administrator", PublicValue::Boolean(false))],
        "group_access_edges",
        "user_id",
        &["claims", "sub"],
        "group_id",
    );
    let res_j_policy = public_recursive_access_policy(
        "res_j_access_edges",
        "resource",
        "team",
        &[("administrator", PublicValue::Boolean(false))],
        &[],
        "group",
        "group_entry",
        "member_id",
        "target_id",
        &[("administrator", PublicValue::Boolean(false))],
        "group_access_edges",
        "user_id",
        &["claims", "sub"],
        "group_id",
    );
    build_public_db_test_schema(
        PublicSchemaBuilder::new()
            .table(PublicTableSchemaBuilder::new("org").column("label", PublicColumnType::Text))
            .table(PublicTableSchemaBuilder::new("group").column("name", PublicColumnType::Text))
            .table(
                PublicTableSchemaBuilder::new("group_access_edges")
                    .fk_column("group_id", "group")
                    .column("user_id", PublicColumnType::Uuid)
                    .column("role", PublicColumnType::Text),
            )
            .table(public_group_entry_table_builder())
            .table(public_resource_table_builder("res_i", res_i_policy, true))
            .table(public_resource_access_table_builder(
                "res_i_access_edges",
                "res_i",
            ))
            .table(public_resource_table_builder("res_j", res_j_policy, true))
            .table(public_resource_access_table_builder(
                "res_j_access_edges",
                "res_j",
            )),
    )
}

pub(super) fn same_table_seeded_resource_policy_schema() -> JazzSchema {
    let resource_policy = public_recursive_access_policy(
        "resource_access",
        "resource",
        "team",
        &[("administrator", PublicValue::Boolean(false))],
        &[],
        "teams",
        "team_entries",
        "member_id",
        "target_id",
        &[("administrator", PublicValue::Boolean(false))],
        "teams",
        "identity_key",
        &["claims", "sub"],
        "id",
    );
    same_table_seeded_public_schema(PublicColumnType::Uuid, resource_policy)
}

pub(super) fn same_table_string_seeded_resource_policy_schema() -> JazzSchema {
    let resource_policy = public_recursive_access_policy(
        "resource_access",
        "resource",
        "team",
        &[("administrator", PublicValue::Boolean(false))],
        &[],
        "teams",
        "team_entries",
        "member_id",
        "target_id",
        &[("administrator", PublicValue::Boolean(false))],
        "teams",
        "identity_key",
        &["claims", "sub"],
        "id",
    );
    same_table_seeded_public_schema(PublicColumnType::Text, resource_policy)
}

fn same_table_seeded_public_schema(
    identity_type: PublicColumnType,
    resource_policy: PublicPolicyExpr,
) -> JazzSchema {
    build_public_db_test_schema(
        PublicSchemaBuilder::new()
            .table(
                PublicTableSchemaBuilder::new("teams")
                    .column("name", PublicColumnType::Text)
                    .column("identity_key", identity_type),
            )
            .table(
                PublicTableSchemaBuilder::new("team_entries")
                    .fk_column("member_id", "teams")
                    .fk_column("target_id", "teams")
                    .column("administrator", PublicColumnType::Boolean),
            )
            .table(
                PublicTableSchemaBuilder::new("resources")
                    .column("label", PublicColumnType::Text)
                    .policies(PublicTablePolicies::new().with_select(resource_policy)),
            )
            .table(
                PublicTableSchemaBuilder::new("resource_access")
                    .fk_column("resource", "resources")
                    .fk_column("team", "teams")
                    .column("administrator", PublicColumnType::Boolean),
            ),
    )
}

pub(super) fn customer_inherited_child_policy_schema() -> JazzSchema {
    let resource_policy = public_recursive_access_policy(
        "res_i_access_edges",
        "resource",
        "team",
        &[("administrator", PublicValue::Boolean(false))],
        &[],
        "group",
        "group_entry",
        "member_id",
        "target_id",
        &[("administrator", PublicValue::Boolean(false))],
        "group_access_edges",
        "user_id",
        &["claims", "sub"],
        "group_id",
    );
    let child_read = PublicPolicyExpr::and(vec![
        PublicPolicyExpr::Inherits {
            operation: PublicOperation::Select,
            via_column: "resource".to_owned(),
            max_depth: None,
        },
        public_literal_eq("status", PublicValue::Text("open".to_owned())),
    ]);
    build_public_db_test_schema(
        PublicSchemaBuilder::new()
            .table(PublicTableSchemaBuilder::new("org").column("label", PublicColumnType::Text))
            .table(PublicTableSchemaBuilder::new("group").column("name", PublicColumnType::Text))
            .table(
                PublicTableSchemaBuilder::new("group_access_edges")
                    .fk_column("group_id", "group")
                    .column("user_id", PublicColumnType::Uuid)
                    .column("role", PublicColumnType::Text),
            )
            .table(public_group_entry_table_builder())
            .table(public_resource_table_builder(
                "res_i",
                resource_policy,
                true,
            ))
            .table(public_resource_access_table_builder(
                "res_i_access_edges",
                "res_i",
            ))
            .table(
                PublicTableSchemaBuilder::new("res_i_child")
                    .fk_column("resource", "res_i")
                    .column("status", PublicColumnType::Text)
                    .column("label", PublicColumnType::Text)
                    .policies(PublicTablePolicies::new().with_select(child_read)),
            )
            .table(
                PublicTableSchemaBuilder::new("res_i_grandchild")
                    .fk_column("child", "res_i_child")
                    .column("label", PublicColumnType::Text)
                    .policies(
                        PublicTablePolicies::new().with_select(PublicPolicyExpr::Inherits {
                            operation: PublicOperation::Select,
                            via_column: "child".to_owned(),
                            max_depth: None,
                        }),
                    ),
            ),
    )
}

pub(super) fn inherited_insert_policy_schema() -> JazzSchema {
    build_public_db_test_schema(
        PublicSchemaBuilder::new()
            .table(
                PublicTableSchemaBuilder::new("parents")
                    .column("owner", PublicColumnType::Uuid)
                    .column("locked", PublicColumnType::Boolean)
                    .policies(
                        PublicTablePolicies::new()
                            .with_select(PublicPolicyExpr::True)
                            .with_update(
                                Some(public_session_eq("owner", &["claims", "sub"])),
                                public_literal_eq("locked", PublicValue::Boolean(false)),
                            ),
                    ),
            )
            .table(
                PublicTableSchemaBuilder::new("children")
                    .fk_column("parent_id", "parents")
                    .column("label", PublicColumnType::Text)
                    .policies(
                        PublicTablePolicies::new().with_insert(PublicPolicyExpr::Inherits {
                            operation: PublicOperation::Select,
                            via_column: "parent_id".to_owned(),
                            max_depth: None,
                        }),
                    ),
            ),
    )
}

pub(super) fn relation_schema() -> JazzSchema {
    build_public_db_test_schema(
        PublicSchemaBuilder::new()
            .table(PublicTableSchemaBuilder::new("users").column("name", PublicColumnType::Text))
            .table(
                PublicTableSchemaBuilder::new("todos")
                    .column("title", PublicColumnType::Text)
                    .fk_column("owner_id", "users"),
            )
            .table(
                PublicTableSchemaBuilder::new("comments")
                    .column("body", PublicColumnType::Text)
                    .fk_column("todo_id", "todos"),
            ),
    )
}

pub(super) fn membership_scoped_relation_schema() -> JazzSchema {
    let chats_read = PublicPolicyExpr::or(vec![
        public_literal_eq("is_public", PublicValue::Boolean(true)),
        public_session_eq("join_code", &["claims", "join_code"]),
        public_exists(
            "chat_members",
            [
                public_outer_eq("chat_id", "id"),
                public_session_eq("user_id", &["claims", "sub"]),
            ],
        ),
    ]);
    let members_read = PublicPolicyExpr::or(vec![
        public_session_eq("user_id", &["claims", "sub"]),
        public_exists(
            "chat_members",
            [
                public_outer_eq("chat_id", "chat_id"),
                public_session_eq("user_id", &["claims", "sub"]),
            ],
        ),
    ]);
    let messages_read = PublicPolicyExpr::or(vec![
        public_exists(
            "chats",
            [
                public_outer_eq("id", "chat_id"),
                public_literal_eq("is_public", PublicValue::Boolean(true)),
            ],
        ),
        public_exists(
            "chat_members",
            [
                public_outer_eq("chat_id", "chat_id"),
                public_session_eq("user_id", &["claims", "sub"]),
            ],
        ),
    ]);
    build_public_db_test_schema(
        PublicSchemaBuilder::new()
            .table(
                PublicTableSchemaBuilder::new("chats")
                    .column("name", PublicColumnType::Text)
                    .column("is_public", PublicColumnType::Boolean)
                    .column("created_by", PublicColumnType::Text)
                    .nullable_column("join_code", PublicColumnType::Text)
                    .policies(PublicTablePolicies::new().with_select(chats_read)),
            )
            .table(
                PublicTableSchemaBuilder::new("chat_members")
                    .fk_column("chat_id", "chats")
                    .column("user_id", PublicColumnType::Text)
                    .nullable_column("join_code", PublicColumnType::Text)
                    .policies(
                        PublicTablePolicies::new()
                            .with_select(members_read)
                            .with_insert(public_session_eq("user_id", &["claims", "sub"])),
                    ),
            )
            .table(
                PublicTableSchemaBuilder::new("profiles")
                    .column("user_id", PublicColumnType::Text)
                    .column("name", PublicColumnType::Text)
                    .nullable_column("avatar", PublicColumnType::Text),
            )
            .table(
                PublicTableSchemaBuilder::new("messages")
                    .fk_column("chat_id", "chats")
                    .fk_column("sender_id", "profiles")
                    .column("text", PublicColumnType::Text)
                    .column("created_at", PublicColumnType::Timestamp)
                    .policies(PublicTablePolicies::new().with_select(messages_read)),
            ),
    )
}

pub(super) fn relation_hop_schema() -> JazzSchema {
    build_public_db_test_schema(
        PublicSchemaBuilder::new()
            .table(PublicTableSchemaBuilder::new("orgs").column("name", PublicColumnType::Text))
            .table(
                PublicTableSchemaBuilder::new("teams")
                    .column("name", PublicColumnType::Text)
                    .nullable_fk_column("org_id", "orgs")
                    .nullable_fk_column("parent_id", "teams"),
            )
            .table(
                PublicTableSchemaBuilder::new("users")
                    .column("name", PublicColumnType::Text)
                    .nullable_fk_column("team_id", "teams"),
            ),
    )
}

pub(super) fn access_edge_include_schema() -> JazzSchema {
    build_public_db_test_schema(
        PublicSchemaBuilder::new()
            .table(PublicTableSchemaBuilder::new("teams").column("name", PublicColumnType::Text))
            .table(
                PublicTableSchemaBuilder::new("team_access_edges")
                    .fk_column("resource_id", "teams")
                    .fk_column("team_id", "teams"),
            ),
    )
}

pub(super) fn policy_relation_schema() -> JazzSchema {
    build_public_db_test_schema(
        PublicSchemaBuilder::new()
            .table(PublicTableSchemaBuilder::new("todos").column("title", PublicColumnType::Text))
            .table(
                PublicTableSchemaBuilder::new("comments")
                    .column("body", PublicColumnType::Text)
                    .column("todo_id", PublicColumnType::Uuid)
                    .column("owner", PublicColumnType::Uuid)
                    .policies(
                        PublicTablePolicies::new()
                            .with_select(public_session_eq("owner", &["claims", "sub"])),
                    ),
            ),
    )
}

pub(super) fn evolved_owner_write_schema() -> JazzSchema {
    build_public_db_test_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("todos")
                .column("title", PublicColumnType::Text)
                .column("done", PublicColumnType::Boolean)
                .column("owner", PublicColumnType::Uuid)
                .column("body", PublicColumnType::Text)
                .policies(public_legacy_write_policy(public_session_eq(
                    "owner",
                    &["claims", "sub"],
                ))),
        ),
    )
}

pub(super) fn row(byte: u8) -> RowUuid {
    RowUuid::from_bytes([byte; 16])
}

pub(super) fn cells(title: &str, done: bool, owner: AuthorSubject) -> RowCells {
    BTreeMap::from([
        ("title".to_owned(), Value::String(title.to_owned())),
        ("done".to_owned(), Value::Bool(done)),
        ("owner".to_owned(), Value::Uuid(owner.test_uuid())),
    ])
}

pub(super) fn issue_schema() -> JazzSchema {
    build_public_db_test_schema(
        PublicSchemaBuilder::new()
            .table(PublicTableSchemaBuilder::new("projects").column("name", PublicColumnType::Text))
            .table(
                PublicTableSchemaBuilder::new("issues")
                    .column("title", PublicColumnType::Text)
                    .column("state", PublicColumnType::Text)
                    .column("assignee", PublicColumnType::Uuid)
                    .fk_column("project", "projects")
                    .column("priority", PublicColumnType::Timestamp)
                    .column(
                        "labels",
                        PublicColumnType::Array {
                            element: Box::new(PublicColumnType::Text),
                        },
                    )
                    .nullable_column("snoozed_until", PublicColumnType::Timestamp),
            )
            .table(
                PublicTableSchemaBuilder::new("issue_tags")
                    .fk_column("issue", "issues")
                    .column("tag", PublicColumnType::Text),
            ),
    )
}

pub(super) fn issue_cells(
    title: &str,
    state: &str,
    assignee: AuthorSubject,
    project: RowUuid,
    priority: u64,
    labels: &[&str],
    snoozed_until: Option<u64>,
) -> RowCells {
    BTreeMap::from([
        ("title".to_owned(), Value::String(title.to_owned())),
        ("state".to_owned(), Value::String(state.to_owned())),
        ("assignee".to_owned(), Value::Uuid(assignee.test_uuid())),
        ("project".to_owned(), Value::Uuid(project.0)),
        ("priority".to_owned(), Value::U64(priority)),
        (
            "labels".to_owned(),
            Value::Array(
                labels
                    .iter()
                    .map(|label| Value::String((*label).to_owned()))
                    .collect(),
            ),
        ),
        (
            "snoozed_until".to_owned(),
            Value::Nullable(snoozed_until.map(|value| Box::new(Value::U64(value)))),
        ),
    ])
}

pub(super) struct CoreDb {
    pub(super) server: Node<RocksDbStorage>,
    schema: JazzSchema,
    author: AuthorSubject,
    pub(super) next_now_ms: Cell<u64>,
    id_source: RefCell<SeededRowIdSource>,
}

pub(super) fn open_core(node_byte: u8, author: AuthorSubject, schema: &JazzSchema) -> CoreDb {
    open_core_with_claims(node_byte, author, schema, test_provider_claims(author))
}

pub(super) fn open_core_with_claims(
    node_byte: u8,
    author: AuthorSubject,
    schema: &JazzSchema,
    claims: BTreeMap<String, Value>,
) -> CoreDb {
    let storage = rocks_storage(schema);
    let mut node = NodeState::new_history_complete(
        NodeUuid::from_bytes([node_byte; 16]),
        schema.clone(),
        storage,
    )
    .unwrap();
    node.set_test_provider_claims(author, claims);
    CoreDb {
        server: Node::new(node),
        schema: schema.clone(),
        author,
        next_now_ms: Cell::new(1),
        id_source: RefCell::new(SeededRowIdSource::new(node_byte as u64)),
    }
}

impl CoreDb {
    /// Test authority counterpart to `Db::author_schema_lineage_publication`.
    /// Descendant fixtures must obtain the active source manifest here rather
    /// than minting an unrelated identity descriptor.
    pub(super) fn author_schema_lineage_publication(
        &self,
        schema: SchemaVersion,
        lens: MigrationLens,
        new_tables: impl IntoIterator<Item = impl Into<String>>,
        dropped_tables: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<SchemaLineagePublication, Error> {
        Ok(self
            .server
            .node()
            .borrow()
            .author_schema_lineage_publication(schema, lens, new_tables, dropped_tables)?)
    }

    pub(super) fn node(&self) -> SharedNodeState<RocksDbStorage> {
        self.server.node()
    }

    pub(super) fn next_now_ms(&self) -> u64 {
        let next = self.next_now_ms.get();
        self.next_now_ms.set(next + 1);
        next
    }

    pub(super) fn table(&self, table: impl Into<String>) -> Query {
        Query::from(table)
    }

    pub(super) fn read(&self, query: &Query) -> Result<Vec<CurrentRow>, Error> {
        let shape = query.validate(&self.schema)?;
        let binding = shape.bind(BTreeMap::new())?;
        block_on(self.server.node().borrow_mut().query_rows(
            &shape,
            &binding,
            DurabilityTier::Local,
        ))
        .map_err(Into::into)
    }

    pub(super) fn one(&self, query: &Query) -> Result<Option<CurrentRow>, Error> {
        Ok(self.read(query)?.into_iter().next())
    }

    pub(super) fn at(&self, position: GlobalTime, query: &Query) -> Result<Vec<CurrentRow>, Error> {
        let shape = query.validate(&self.schema)?;
        let binding = shape.bind(BTreeMap::new())?;
        block_on(
            self.server
                .node()
                .borrow_mut()
                .at(position)
                .read(&shape, &binding),
        )
        .map_err(Into::into)
    }

    pub(super) fn insert(
        &self,
        table: &str,
        cells: RowCells,
    ) -> Result<WriteHandle<RocksDbStorage>, Error> {
        let row = self.id_source.borrow_mut().next_row_id();
        self.insert_with_id(table, row, cells)
    }

    pub(super) fn insert_with_id(
        &self,
        table: &str,
        row: RowUuid,
        cells: RowCells,
    ) -> Result<WriteHandle<RocksDbStorage>, Error> {
        let node = self.server.node();
        let published = block_on(
            node.borrow_mut().commit_mergeable(
                MergeableCommit::new(table, row, self.next_now_ms())
                    .made_by(self.author)
                    .cells(cells),
            ),
        )?;
        let tx_id = block_on(node.borrow_mut().persist_and_settle_transaction(published))?;
        let outcome = block_on(node.borrow_mut().finalize_local_mergeable_commit(tx_id))?;
        block_on(node.borrow_mut().persist_and_settle_outcome(outcome))?;
        self.server.mark_subscriber_connections_dirty();
        Ok(WriteHandle {
            node: Rc::downgrade(&node),
            row_uuid: row,
            tx_id,
            local_tier: DurabilityTier::Global,
            queued_status: None,
            queued_alias: None,
        })
    }

    pub(super) fn insert_with_id_in_branch(
        &self,
        table: &str,
        branch: BranchSelector,
        row: RowUuid,
        cells: RowCells,
    ) -> Result<WriteHandle<RocksDbStorage>, Error> {
        let node = self.server.node();
        let published = block_on(
            node.borrow_mut().commit_mergeable(
                MergeableCommit::new(table, row, self.next_now_ms())
                    .made_by(self.author)
                    .branch(branch)
                    .cells(cells),
            ),
        )?;
        let tx_id = block_on(node.borrow_mut().persist_and_settle_transaction(published))?;
        let outcome = block_on(node.borrow_mut().finalize_local_mergeable_commit(tx_id))?;
        block_on(node.borrow_mut().persist_and_settle_outcome(outcome))?;
        self.server.mark_subscriber_connections_dirty();
        Ok(WriteHandle {
            node: Rc::downgrade(&node),
            row_uuid: row,
            tx_id,
            local_tier: DurabilityTier::Global,
            queued_status: None,
            queued_alias: None,
        })
    }

    pub(super) fn insert_same_row_in_branches(
        &self,
        table: &str,
        row: RowUuid,
        entries: impl IntoIterator<Item = (BranchSelector, RowCells)>,
    ) -> Result<TxId, Error> {
        let now_ms = self.next_now_ms();
        let commits = entries
            .into_iter()
            .map(|(branch, cells)| {
                MergeableCommit::new(table, row, now_ms)
                    .made_by(self.author)
                    .branch(branch)
                    .cells(cells)
            })
            .collect();
        let node = self.server.node();
        let published = block_on(node.borrow_mut().commit_mergeable_many(commits))?;
        let tx_id = block_on(node.borrow_mut().persist_and_settle_transaction(published))?;
        let outcome = block_on(node.borrow_mut().finalize_local_mergeable_commit(tx_id))?;
        block_on(node.borrow_mut().persist_and_settle_outcome(outcome))?;
        self.server.mark_subscriber_connections_dirty();
        Ok(tx_id)
    }

    pub(super) fn insert_attributed(
        &self,
        made_by: AuthorSubject,
        table: &str,
        cells: RowCells,
    ) -> Result<WriteHandle<RocksDbStorage>, Error> {
        let row = self.id_source.borrow_mut().next_row_id();
        let node = self.server.node();
        let published = block_on(
            node.borrow_mut().commit_mergeable(
                MergeableCommit::new(table, row, self.next_now_ms())
                    .made_by(made_by)
                    .permission_subject(self.author)
                    .cells(cells),
            ),
        )?;
        let tx_id = block_on(node.borrow_mut().persist_and_settle_transaction(published))?;
        let outcome = block_on(node.borrow_mut().finalize_local_mergeable_commit(tx_id))?;
        block_on(node.borrow_mut().persist_and_settle_outcome(outcome))?;
        self.server.mark_subscriber_connections_dirty();
        Ok(WriteHandle {
            node: Rc::downgrade(&node),
            row_uuid: row,
            tx_id,
            local_tier: DurabilityTier::Global,
            queued_status: None,
            queued_alias: None,
        })
    }

    pub(super) fn update(
        &self,
        table: &str,
        row: RowUuid,
        patch: RowCells,
    ) -> Result<WriteHandle<RocksDbStorage>, Error> {
        self.update_attributed(self.author, table, row, patch)
    }

    pub(super) fn update_attributed(
        &self,
        made_by: AuthorSubject,
        table: &str,
        row: RowUuid,
        patch: RowCells,
    ) -> Result<WriteHandle<RocksDbStorage>, Error> {
        let table_schema = self
            .schema
            .tables
            .iter()
            .find(|candidate| candidate.name == table)
            .cloned()
            .ok_or_else(|| Error::new(ErrorCode::Schema, format!("unknown table {table}")))?;
        let mut cells = BTreeMap::new();
        let mut parent = None;
        if let Some(existing) = self
            .read(&Query::from(table))?
            .into_iter()
            .find(|candidate| candidate.row_uuid() == row)
        {
            for column in &table_schema.columns {
                if let Some(value) = existing.cell(&table_schema, &column.name) {
                    cells.insert(column.name.clone(), value);
                }
            }
            parent = block_on(self.server.node().borrow_mut().current_row_tx_id(&existing));
        }
        cells.extend(patch);
        let node = self.server.node();
        let mut commit = MergeableCommit::new(table, row, self.next_now_ms())
            .made_by(made_by)
            .permission_subject(self.author)
            .cells(cells);
        if let Some(parent) = parent {
            commit = commit.parents(vec![parent]);
        }
        let published = block_on(node.borrow_mut().commit_mergeable(commit))?;
        let tx_id = block_on(node.borrow_mut().persist_and_settle_transaction(published))?;
        let outcome = block_on(node.borrow_mut().finalize_local_mergeable_commit(tx_id))?;
        block_on(node.borrow_mut().persist_and_settle_outcome(outcome))?;
        self.server.mark_subscriber_connections_dirty();
        Ok(WriteHandle {
            node: Rc::downgrade(&node),
            row_uuid: row,
            tx_id,
            local_tier: DurabilityTier::Global,
            queued_status: None,
            queued_alias: None,
        })
    }

    pub(super) fn accept_subscriber(
        &self,
        transport: Box<dyn Transport>,
        identity: AuthorSubject,
    ) -> Rc<LocalMutex<PeerConnection<RocksDbStorage>>> {
        self.server.accept_subscriber_with_claims(
            transport,
            identity,
            test_provider_claims(identity),
        )
    }

    pub(super) fn accept_subscriber_with_trust(
        &self,
        transport: Box<dyn Transport>,
        identity: AuthorSubject,
        trust: CommitUnitTrust,
    ) -> Rc<LocalMutex<PeerConnection<RocksDbStorage>>> {
        self.server.accept_subscriber_with_claims_and_trust(
            transport,
            identity,
            test_provider_claims(identity),
            trust,
        )
    }

    pub(super) fn accept_scope_isolated_relay_subscriber(
        &self,
        transport: Box<dyn Transport>,
        identity: AuthorSubject,
        claims: BTreeMap<String, Value>,
        admission_epoch: u64,
    ) -> Rc<LocalMutex<PeerConnection<RocksDbStorage>>> {
        self.server.accept_scope_isolated_relay_subscriber(
            transport,
            identity,
            claims,
            admission_epoch,
        )
    }

    pub(super) fn accept_subscriber_with_claims(
        &self,
        transport: Box<dyn Transport>,
        identity: AuthorSubject,
        claims: BTreeMap<String, Value>,
    ) -> Rc<LocalMutex<PeerConnection<RocksDbStorage>>> {
        self.server
            .accept_test_subscriber_with_claims(transport, identity, claims)
    }

    pub(super) fn accept_subscriber_with_resume(
        &self,
        transport: Box<dyn Transport>,
        identity: AuthorSubject,
        cursor: ResumeCursor,
    ) -> Rc<LocalMutex<PeerConnection<RocksDbStorage>>> {
        self.server
            .accept_subscriber_with_resume(transport, identity, cursor)
    }

    pub(super) fn tick(&self) -> Result<(), Error> {
        block_on(self.server.tick()).map(|_| ())
    }

    pub(super) fn exclusive_tx(&self) -> Result<CoreExclusiveTx<'_>, Error> {
        let tx_id = OpenTransactionId::new();
        block_on(
            self.server
                .node()
                .borrow_mut()
                .open_exclusive_for_identity(tx_id, self.author),
        )?;
        Ok(CoreExclusiveTx {
            core: self,
            tx_id,
            has_reads: Cell::new(false),
        })
    }

    pub(super) fn publish_schema(&self, schema: SchemaVersion) -> Result<Vec<SyncMessage>, Error> {
        let node = self.server.node();
        let outcome = block_on(node.borrow_mut().apply_trusted_catalogue_message(
            SyncMessage::PublishSchema {
                author: self.author,
                schema: Box::new(schema),
            },
        ))?;
        block_on(node.borrow_mut().persist_and_settle_outcome(outcome)).map_err(Into::into)
    }

    pub(super) fn publish_schema_with_lens(
        &self,
        catalogue_seq: u64,
        publication: SchemaLineagePublication,
    ) -> Result<Vec<SyncMessage>, Error> {
        let node = self.server.node();
        let outcome = block_on(node.borrow_mut().apply_trusted_catalogue_message(
            SyncMessage::PublishSchemaWithLens {
                author: self.author,
                catalogue_seq,
                publication: Box::new(publication),
            },
        ))?;
        block_on(node.borrow_mut().persist_and_settle_outcome(outcome)).map_err(Into::into)
    }

    pub(super) fn set_current_write_schema(
        &self,
        pointer: CurrentWriteSchema,
    ) -> Result<Vec<SyncMessage>, Error> {
        let node = self.server.node();
        let outcome = block_on(node.borrow_mut().apply_trusted_catalogue_message(
            SyncMessage::SetCurrentWriteSchema {
                author: self.author,
                pointer,
            },
        ))?;
        block_on(node.borrow_mut().persist_and_settle_outcome(outcome)).map_err(Into::into)
    }
}

pub(super) struct CoreExclusiveTx<'a> {
    core: &'a CoreDb,
    tx_id: OpenTransactionId,
    has_reads: Cell<bool>,
}

impl CoreExclusiveTx<'_> {
    pub(super) fn read(&self, table: &str, row: RowUuid) -> Result<Option<RowCells>, Error> {
        self.has_reads.set(true);
        block_on(
            self.core
                .server
                .node()
                .borrow_mut()
                .tx_read(self.tx_id, table, row),
        )
        .map_err(Into::into)
    }

    pub(super) fn insert_with_id(
        &self,
        table: &str,
        row: RowUuid,
        cells: RowCells,
    ) -> Result<(), Error> {
        block_on(
            self.core
                .server
                .node()
                .borrow_mut()
                .tx_write(self.tx_id, table, row, cells, None),
        )
        .map_err(Into::into)
    }

    pub(super) fn update(&self, table: &str, row: RowUuid, patch: RowCells) -> Result<(), Error> {
        let mut cells = self.read(table, row)?.unwrap_or_default();
        cells.extend(patch);
        self.insert_with_id(table, row, cells)
    }

    pub(super) fn commit(self) -> Result<TxId, Error> {
        let node = self.core.server.node();
        if self.has_reads.get() && node.borrow().open_exclusive_snapshot_moved(self.tx_id)? {
            node.borrow_mut().abandon_tx(self.tx_id)?;
            return Err(write_rejected(
                self.tx_id,
                RejectionReason::ExclusiveConflict,
            ));
        }
        let (published, unit) = block_on(node.borrow_mut().commit_exclusive(
            self.tx_id,
            self.core.author,
            self.core.next_now_ms(),
        ))?;
        let tx_id = block_on(node.borrow_mut().persist_and_settle_transaction(published))?;
        let SyncMessage::CommitUnit { tx, versions } = unit else {
            return Err(Error::new(
                ErrorCode::Protocol,
                "commit_exclusive must yield a CommitUnit",
            ));
        };
        let outcome = block_on(
            node.borrow_mut()
                .finalize_local_exclusive_commit(tx, versions),
        )?;
        let fate = block_on(node.borrow_mut().persist_and_settle_outcome(outcome))?;
        if let Fate::Rejected(reason) = fate {
            return Err(write_rejected(tx_id, reason));
        }
        self.core.server.mark_subscriber_connections_dirty();
        Ok(tx_id)
    }
}

/// Commit a row on an authority node and confirm it reached Global, so the
/// serving path ships it.
pub(super) fn seed(db: &CoreDb, table: &str, cells: RowCells) -> RowUuid {
    let write = db.insert(table, cells).unwrap();
    block_on(write.wait(DurabilityTier::Global)).unwrap();
    write.row_uuid()
}
