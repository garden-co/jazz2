use super::*;

impl<S: Storage, Sch: Scheduler> RuntimeCore<S, Sch> {
    fn retained_batch_terminal_tier(&self) -> DurabilityTier {
        let sync_manager = self.schema_manager.query_manager().sync_manager();

        if sync_manager.has_connected_servers() {
            DurabilityTier::GlobalServer
        } else {
            sync_manager
                .max_local_durability_tier()
                .unwrap_or(DurabilityTier::Local)
        }
    }
    fn pending_batch_ids_needing_reconciliation(&self) -> Vec<crate::row_histories::BatchId> {
        let Ok(records) = self.storage.scan_local_batch_records() else {
            return Vec::new();
        };
        let terminal_tier = self.retained_batch_terminal_tier();

        records
            .into_iter()
            .filter(|record| {
                record.mode != crate::batch_fate::BatchMode::Transactional || record.sealed
            })
            .filter(|record| match record.latest_settlement.as_ref() {
                None => true,
                Some(crate::batch_fate::BatchSettlement::Missing { .. }) => true,
                Some(crate::batch_fate::BatchSettlement::Rejected { .. }) => false,
                Some(crate::batch_fate::BatchSettlement::DurableDirect {
                    confirmed_tier, ..
                })
                | Some(crate::batch_fate::BatchSettlement::AcceptedTransaction {
                    confirmed_tier,
                    ..
                }) => confirmed_tier < &terminal_tier,
            })
            .map(|record| record.batch_id)
            .collect()
    }

    pub fn request_batch_settlements(&mut self, batch_ids: Vec<crate::row_histories::BatchId>) {
        if batch_ids.is_empty() {
            return;
        }

        let server_ids = self
            .schema_manager
            .query_manager()
            .sync_manager()
            .connected_server_ids();
        if server_ids.is_empty() {
            return;
        }

        let mut unique_batch_ids = Vec::with_capacity(batch_ids.len());
        let mut seen = std::collections::HashSet::new();
        for batch_id in batch_ids {
            if seen.insert(batch_id) {
                unique_batch_ids.push(batch_id);
            }
        }

        let sync_manager = self.schema_manager.query_manager_mut().sync_manager_mut();
        for server_id in server_ids {
            sync_manager.request_batch_settlements_from_server(server_id, unique_batch_ids.clone());
        }
        self.immediate_tick();
    }

    // =========================================================================
    // Sync Operations
    // =========================================================================

    /// Push a sync message to the inbox (from network).
    pub fn push_sync_inbox(&mut self, entry: InboxEntry) {
        if entry.payload.writes_storage() {
            self.mark_storage_write_pending_flush();
        }
        self.schema_manager
            .query_manager_mut()
            .sync_manager_mut()
            .push_inbox(entry);
    }

    /// Add a server connection.
    pub fn add_server(&mut self, server_id: ServerId) {
        self.add_server_with_catalogue_state_hash(server_id, None);
    }

    /// Add a server connection, optionally comparing the upstream catalogue
    /// digest first so unchanged catalogue objects are not replayed.
    pub fn add_server_with_catalogue_state_hash(
        &mut self,
        server_id: ServerId,
        remote_catalogue_state_hash: Option<&str>,
    ) {
        info!(%server_id, "adding server");
        let local_catalogue_state_hash = self.schema_manager.catalogue_state_hash();
        let skip_catalogue_sync = remote_catalogue_state_hash
            .is_some_and(|remote_hash| remote_hash == local_catalogue_state_hash);
        self.schema_manager
            .query_manager_mut()
            .add_server_with_storage(&self.storage, server_id, skip_catalogue_sync);
        let pending_batch_ids = self.pending_batch_ids_needing_reconciliation();
        self.schema_manager
            .query_manager_mut()
            .sync_manager_mut()
            .request_batch_settlements_from_server(server_id, pending_batch_ids);
        self.immediate_tick();
    }

    /// Remove a server connection.
    pub fn remove_server(&mut self, server_id: ServerId) {
        self.schema_manager
            .query_manager_mut()
            .sync_manager_mut()
            .remove_server(server_id);
        self.parked_sync_messages_by_server_seq.remove(&server_id);
        self.next_expected_server_seq.remove(&server_id);
        self.last_applied_server_seq.remove(&server_id);
    }

    /// Add a client connection.
    pub fn add_client(&mut self, client_id: ClientId, session: Option<Session>) {
        info!(%client_id, has_session = session.is_some(), "adding client");
        let sm = self.schema_manager.query_manager_mut().sync_manager_mut();
        sm.add_client_with_storage(&self.storage, client_id);
        if let Some(s) = session {
            sm.set_client_session(client_id, s);
        }
        self.immediate_tick();
    }

    /// Ensure a client exists with the given session.
    ///
    /// If the client already exists, updates the session. This is idempotent —
    /// calling with the same session is a no-op. Calling with a new session
    /// updates it in place without resetting the client's role or other state.
    ///
    /// A session is always required — callers must authenticate before
    /// registering a client.
    pub fn ensure_client_with_session(&mut self, client_id: ClientId, session: Session) {
        let sm = self.schema_manager.query_manager_mut().sync_manager_mut();
        if sm.get_client(client_id).is_some() {
            sm.set_client_session(client_id, session);
        } else {
            sm.add_client_with_storage(&self.storage, client_id);
            sm.set_client_session(client_id, session);
            self.immediate_tick();
        }
    }

    /// Remove a client connection.
    ///
    /// Returns `false` if the client has unprocessed messages — either
    /// parked in RuntimeCore (pre-inbox, from `push_sync_inbox`) or
    /// already in SyncManager's inbox. The caller should retry later.
    pub fn remove_client(&mut self, client_id: ClientId) -> bool {
        use crate::sync_manager::Source;

        let has_parked = self
            .parked_sync_messages
            .iter()
            .any(|e| e.source == Source::Client(client_id));
        if has_parked {
            tracing::warn!(
                %client_id,
                "skipping reap: client has parked sync messages"
            );
            return false;
        }

        self.schema_manager
            .query_manager_mut()
            .remove_client(client_id)
    }

    /// Promote a client to Admin role (full access, no ReBAC).
    pub fn set_client_admin(&mut self, client_id: ClientId) {
        use crate::sync_manager::ClientRole;
        self.schema_manager
            .query_manager_mut()
            .sync_manager_mut()
            .set_client_role(client_id, ClientRole::Admin);
    }

    /// Ensure a client exists and is marked as Admin without resetting state.
    pub fn ensure_client_as_admin(&mut self, client_id: ClientId) {
        use crate::sync_manager::ClientRole;
        let sm = self.schema_manager.query_manager_mut().sync_manager_mut();
        if sm.get_client(client_id).is_some() {
            sm.set_client_role(client_id, ClientRole::Admin);
        } else {
            sm.add_client_with_storage(&self.storage, client_id);
            sm.set_client_role(client_id, ClientRole::Admin);
            self.immediate_tick();
        }
    }

    /// Promote a client to Backend role (row access, no catalogue writes).
    pub fn set_client_backend(&mut self, client_id: ClientId) {
        use crate::sync_manager::ClientRole;
        self.schema_manager
            .query_manager_mut()
            .sync_manager_mut()
            .set_client_role(client_id, ClientRole::Backend);
    }

    /// Ensure a client exists and is marked as Backend without resetting state.
    pub fn ensure_client_as_backend(&mut self, client_id: ClientId) {
        use crate::sync_manager::ClientRole;
        let sm = self.schema_manager.query_manager_mut().sync_manager_mut();
        if sm.get_client(client_id).is_some() {
            sm.set_client_role(client_id, ClientRole::Backend);
        } else {
            sm.add_client_with_storage(&self.storage, client_id);
            sm.set_client_role(client_id, ClientRole::Backend);
            self.immediate_tick();
        }
    }

    /// Set a client's role.
    pub fn set_client_role_by_name(
        &mut self,
        client_id: ClientId,
        role: crate::sync_manager::ClientRole,
    ) {
        self.schema_manager
            .query_manager_mut()
            .sync_manager_mut()
            .set_client_role(client_id, role);
    }
}
