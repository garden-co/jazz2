use ahash::{AHashMap, AHashSet};
use smallvec::SmallVec;

use crate::object::ObjectId;
use crate::query_manager::types::{LoadedRow, RowDelta, TableName, Tuple, TupleDelta};
use crate::storage::Storage;

use super::super::graph_nodes::{NodeId, RowNode, SourceContext, SourceNode, TransformNode};
use super::{GraphNode, QueryGraph};

impl QueryGraph {
    /// Mark a node as dirty using the bitmap.
    pub fn mark_dirty(&mut self, id: NodeId) {
        let idx = id.0 as usize;
        if idx >= self.dirty_bitmap.len() {
            self.dirty_bitmap.resize(idx + 1, false);
        }
        self.dirty_bitmap.set(idx, true);
    }

    /// Check if a node is dirty.
    pub(super) fn is_dirty(&self, id: NodeId) -> bool {
        let idx = id.0 as usize;
        idx < self.dirty_bitmap.len() && self.dirty_bitmap[idx]
    }

    /// Check if any nodes are dirty.
    pub fn has_dirty_nodes(&self) -> bool {
        self.dirty_bitmap.any()
    }

    /// Clear all dirty flags.
    pub fn clear_dirty(&mut self) {
        self.dirty_bitmap.fill(false);
    }

    /// Force a full graph recompute.
    pub fn mark_all_dirty(&mut self) {
        self.dirty_bitmap.fill(true);
    }

    /// Mark index scan nodes dirty for a given table/column.
    /// Also propagates dirty marks to downstream nodes.
    pub fn mark_dirty_for_column(&mut self, table: &str, column: &str) {
        let affected: Vec<NodeId> = self
            .index_scan_nodes
            .iter()
            .filter(|(_, t, c)| {
                t.as_str() == table && (c.as_str() == column || c.as_str() == "_id")
            })
            .map(|(node_id, _, _)| *node_id)
            .collect();
        for node_id in affected {
            self.mark_dirty(node_id);
            self.mark_downstream_dirty(node_id);
        }
    }

    /// Mark all index scan nodes for a table dirty.
    /// Also marks array/recursive subquery nodes dirty if the table is their inner table.
    /// Also marks PolicyFilter nodes dirty if the table is INHERITS-referenced.
    pub fn mark_dirty_for_table(&mut self, table: &str) {
        // Mark index scan nodes and propagate downstream
        let affected_index_scans: Vec<NodeId> = self
            .index_scan_nodes
            .iter()
            .filter_map(|(node_id, t, _)| {
                if t.as_str() == table {
                    Some(*node_id)
                } else {
                    None
                }
            })
            .collect();

        for node_id in affected_index_scans {
            self.mark_dirty(node_id);
            self.mark_downstream_dirty(node_id);
        }
        // Mark array subquery nodes whose inner table changed
        // Collect node_ids first to avoid borrow conflict
        let affected_array_subqueries: Vec<NodeId> = self
            .array_subquery_tables
            .iter()
            .filter_map(|(node_id, inner_table)| {
                if inner_table.as_str() == table {
                    Some(*node_id)
                } else {
                    None
                }
            })
            .collect();

        for node_id in affected_array_subqueries {
            self.mark_dirty(node_id);
            // Mark the node as needing inner re-evaluation
            if let Some(GraphNode::ArraySubquery(node)) = self.get_node_mut(node_id) {
                node.mark_inner_dirty();
            }
            // Propagate dirty marks to downstream nodes (Output, etc.)
            self.mark_downstream_dirty(node_id);
        }

        // Mark PolicyFilter nodes whose policy dependency tables changed
        let affected_policy_filters: Vec<NodeId> = self
            .policy_filter_tables
            .iter()
            .filter_map(|(node_id, inherits_table)| {
                if inherits_table.as_str() == table {
                    Some(*node_id)
                } else {
                    None
                }
            })
            .collect();

        for node_id in affected_policy_filters {
            self.mark_dirty(node_id);
            // Mark the node as needing policy re-evaluation
            if let Some(GraphNode::PolicyFilter(node)) = self.get_node_mut(node_id) {
                node.mark_inherits_dirty();
            }
            // Propagate dirty marks to downstream nodes
            self.mark_downstream_dirty(node_id);
        }

        let affected_magic_columns: Vec<NodeId> = self
            .magic_column_tables
            .iter()
            .filter_map(|(node_id, dependency_table)| {
                if dependency_table.as_str() == table {
                    Some(*node_id)
                } else {
                    None
                }
            })
            .collect();

        for node_id in affected_magic_columns {
            self.mark_dirty(node_id);
            if let Some(GraphNode::MagicColumns(node)) = self.get_node_mut(node_id) {
                node.mark_dependency_dirty();
            }
            self.mark_downstream_dirty(node_id);
        }

        // Mark RecursiveRelation nodes whose step table changed
        let affected_recursive_relations: Vec<NodeId> = self
            .recursive_relation_tables
            .iter()
            .filter_map(|(node_id, step_table)| {
                if step_table.as_str() == table {
                    Some(*node_id)
                } else {
                    None
                }
            })
            .collect();

        for node_id in affected_recursive_relations {
            self.mark_dirty(node_id);
            if let Some(GraphNode::RecursiveRelation(node)) = self.get_node_mut(node_id) {
                node.mark_inner_dirty();
            }
            self.mark_downstream_dirty(node_id);
        }
    }

    /// Check if this graph involves a table (as index scan, array subquery inner table, or INHERITS reference).
    pub fn involves_table(&self, table: &str) -> bool {
        self.index_scan_nodes
            .iter()
            .any(|(_, t, _)| t.as_str() == table)
            || self
                .array_subquery_tables
                .iter()
                .any(|(_, t)| t.as_str() == table)
            || self
                .policy_filter_tables
                .iter()
                .any(|(_, t)| t.as_str() == table)
            || self
                .magic_column_tables
                .iter()
                .any(|(_, t)| t.as_str() == table)
            || self
                .recursive_relation_tables
                .iter()
                .any(|(_, t)| t.as_str() == table)
    }

    /// Check if this graph uses a specific index (table + column combination).
    pub fn uses_index(&self, table: &str, column: &str) -> bool {
        self.index_scan_nodes
            .iter()
            .any(|(_, t, c)| t.as_str() == table && c.as_str() == column)
    }

    /// Mark a row ID as updated for content checking.
    /// This tells MaterializeNodes to check if the row's content has changed.
    pub fn mark_row_updated(&mut self, id: ObjectId) {
        // First pass: mark the ID as updated in each MaterializeNode and collect node IDs
        let materialize_node_ids: Vec<NodeId> = self
            .nodes
            .iter_mut()
            .enumerate()
            .filter_map(|(idx, compact)| {
                if let GraphNode::Materialize(mat_node) = &mut compact.node {
                    mat_node.mark_updated(id);
                    Some(NodeId(idx as u64))
                } else {
                    None
                }
            })
            .collect();

        // Second pass: mark dirty and propagate downstream
        for node_id in materialize_node_ids {
            self.mark_dirty(node_id);
            self.mark_downstream_dirty(node_id);
        }
    }

    /// Mark a row ID as deleted for removal delta emission.
    /// This tells MaterializeNodes to emit a removal delta for this row.
    pub fn mark_row_deleted(&mut self, id: ObjectId) {
        // First pass: mark the ID as deleted in each MaterializeNode and collect node IDs
        let materialize_node_ids: Vec<NodeId> = self
            .nodes
            .iter_mut()
            .enumerate()
            .filter_map(|(idx, compact)| {
                if let GraphNode::Materialize(mat_node) = &mut compact.node {
                    mat_node.mark_deleted(id);
                    Some(NodeId(idx as u64))
                } else {
                    None
                }
            })
            .collect();

        // Second pass: mark dirty and propagate downstream
        for node_id in materialize_node_ids {
            self.mark_dirty(node_id);
            self.mark_downstream_dirty(node_id);
        }
    }

    /// Mark all nodes that depend on the given node as dirty (propagate forward).
    fn mark_downstream_dirty(&mut self, node_id: NodeId) {
        if let Some(outputs) = self.get_outputs(node_id) {
            let parents: SmallVec<[NodeId; 2]> = outputs.iter().copied().collect();
            for parent in parents {
                // Only recurse if not already dirty (avoid infinite loops)
                if !self.is_dirty(parent) {
                    self.mark_dirty(parent);
                    // Recursively mark parents of parent
                    self.mark_downstream_dirty(parent);
                }
            }
        }
    }

    /// Topological sort of dirty nodes (dependencies first).
    fn topo_sort_dirty(&self) -> Vec<NodeId> {
        let mut result = Vec::new();
        let mut visited = AHashSet::new();

        fn visit(
            node: NodeId,
            graph: &QueryGraph,
            visited: &mut AHashSet<NodeId>,
            result: &mut Vec<NodeId>,
        ) {
            if visited.contains(&node) {
                return;
            }
            visited.insert(node);

            // Visit dependencies first (inputs)
            if let Some(compact) = graph.nodes.get(node.0 as usize) {
                for dep in &compact.inputs {
                    visit(*dep, graph, visited, result);
                }
            }

            result.push(node);
        }

        // Iterate over dirty nodes using BitVec's iter_ones()
        for idx in self.dirty_bitmap.iter_ones() {
            visit(NodeId(idx as u64), self, &mut visited, &mut result);
        }

        result
    }

    fn process_limit_offset_with_ordered_sort_input(
        &mut self,
        node_id: NodeId,
        input_node: NodeId,
    ) -> Option<TupleDelta> {
        let node_idx = node_id.0 as usize;
        let input_idx = input_node.0 as usize;

        if node_idx == input_idx || node_idx >= self.nodes.len() || input_idx >= self.nodes.len() {
            return None;
        }

        if input_idx < node_idx {
            let (before_node, from_node) = self.nodes.split_at_mut(node_idx);
            let input = &before_node[input_idx].node;
            let node = &mut from_node[0].node;
            return match (input, node) {
                (GraphNode::Sort(sort_node), GraphNode::LimitOffset(limit_offset_node)) => {
                    Some(limit_offset_node.process_with_ordered_input(sort_node.sorted_tuples()))
                }
                _ => None,
            };
        }

        let (before_input, from_input) = self.nodes.split_at_mut(input_idx);
        let node = &mut before_input[node_idx].node;
        let input = &from_input[0].node;
        match (input, node) {
            (GraphNode::Sort(sort_node), GraphNode::LimitOffset(limit_offset_node)) => {
                Some(limit_offset_node.process_with_ordered_input(sort_node.sorted_tuples()))
            }
            _ => None,
        }
    }

    /// Settle the graph - process all dirty nodes in topological order.
    /// Uses tuple-based processing internally, converts to RowDelta for output.
    pub fn settle<F>(&mut self, storage: &dyn Storage, mut row_loader: F) -> RowDelta
    where
        F: FnMut(ObjectId, Option<TableName>) -> Option<LoadedRow>,
    {
        let order = self.topo_sort_dirty();
        if !order.is_empty() {
            tracing::trace!(dirty_nodes = order.len(), table = %self.table, "settling query graph");
        }
        let mut tuple_deltas: AHashMap<NodeId, TupleDelta> = AHashMap::new();

        let ctx = SourceContext { storage };

        for node_id in order {
            let node_type = match self.get_node(node_id) {
                Some(GraphNode::IndexScan(_)) => "IndexScan",
                Some(GraphNode::Union(_)) => "Union",
                Some(GraphNode::Alias(_)) => "Alias",
                Some(GraphNode::Join(_)) => "Join",
                Some(GraphNode::MagicColumns(_)) => "MagicColumns",
                Some(GraphNode::Project(_)) => "Project",
                Some(GraphNode::SelectElement(_)) => "SelectElement",
                Some(GraphNode::RecursiveRelation(_)) => "RecursiveRelation",
                Some(GraphNode::Materialize(_)) => "Materialize",
                Some(GraphNode::Filter(_)) => "Filter",
                Some(GraphNode::PolicyFilter(_)) => "PolicyFilter",
                Some(GraphNode::Sort(_)) => "Sort",
                Some(GraphNode::LimitOffset(_)) => "LimitOffset",
                Some(GraphNode::ArraySubquery(_)) => "ArraySubquery",
                Some(GraphNode::Output(_)) => "Output",
                Some(GraphNode::ExistsOutput(_)) => "ExistsOutput",
                None => "Unknown",
            };

            match self.get_node(node_id) {
                Some(GraphNode::IndexScan(_)) => {
                    if let Some(GraphNode::IndexScan(scan_node)) = self.get_node_mut(node_id) {
                        let delta = SourceNode::scan(scan_node, &ctx);
                        tracing::debug!(
                            node_id = node_id.0,
                            node_type,
                            added = delta.added.len(),
                            removed = delta.removed.len(),
                            "graph node evaluated"
                        );
                        tuple_deltas.insert(node_id, delta);
                    }
                }
                Some(GraphNode::Union(_)) => {
                    let inputs = self.collect_tuple_inputs(node_id);
                    if let Some(GraphNode::Union(union_node)) = self.get_node_mut(node_id) {
                        let input_refs: Vec<_> = inputs.iter().collect();
                        let delta = TransformNode::process(union_node, &input_refs);
                        tracing::debug!(
                            node_id = node_id.0,
                            node_type,
                            added = delta.added.len(),
                            removed = delta.removed.len(),
                            "graph node evaluated"
                        );
                        tuple_deltas.insert(node_id, delta);
                    }
                }
                Some(GraphNode::Alias(_)) => {
                    let input_delta = self
                        .get_inputs(node_id)
                        .first()
                        .and_then(|dep| tuple_deltas.get(dep).cloned())
                        .unwrap_or_default();

                    if let Some(GraphNode::Alias(alias_node)) = self.get_node_mut(node_id) {
                        let delta = RowNode::process(alias_node, input_delta);
                        tracing::debug!(
                            node_id = node_id.0,
                            node_type,
                            added = delta.added.len(),
                            removed = delta.removed.len(),
                            "graph node evaluated"
                        );
                        tuple_deltas.insert(node_id, delta);
                    }
                }
                Some(GraphNode::Join(_)) => {
                    // JoinNode has two inputs: left (index 0) and right (index 1)
                    let inputs = self.get_inputs(node_id);
                    let left_delta = inputs
                        .first()
                        .and_then(|dep| tuple_deltas.get(dep).cloned())
                        .unwrap_or_default();
                    let right_delta = inputs
                        .get(1)
                        .and_then(|dep| tuple_deltas.get(dep).cloned())
                        .unwrap_or_default();

                    if let Some(GraphNode::Join(join_node)) = self.get_node_mut(node_id) {
                        // Process left side first, then right side
                        let left_result = join_node.process_left(left_delta);
                        let right_result = join_node.process_right(right_delta);

                        // Merge results
                        let mut merged = TupleDelta::new();
                        merged.added.extend(left_result.added);
                        merged.added.extend(right_result.added);
                        merged.removed.extend(left_result.removed);
                        merged.removed.extend(right_result.removed);

                        tracing::debug!(
                            node_id = node_id.0,
                            node_type,
                            added = merged.added.len(),
                            removed = merged.removed.len(),
                            "graph node evaluated"
                        );
                        tuple_deltas.insert(node_id, merged);
                    }
                }
                Some(GraphNode::Project(_)) => {
                    let input_delta = self
                        .get_inputs(node_id)
                        .first()
                        .and_then(|dep| tuple_deltas.get(dep).cloned())
                        .unwrap_or_default();

                    if let Some(GraphNode::Project(project_node)) = self.get_node_mut(node_id) {
                        let delta = RowNode::process(project_node, input_delta);
                        tracing::debug!(
                            node_id = node_id.0,
                            node_type,
                            added = delta.added.len(),
                            removed = delta.removed.len(),
                            "graph node evaluated"
                        );
                        tuple_deltas.insert(node_id, delta);
                    }
                }
                Some(GraphNode::SelectElement(_)) => {
                    let input_delta = self
                        .get_inputs(node_id)
                        .first()
                        .and_then(|dep| tuple_deltas.get(dep).cloned())
                        .unwrap_or_default();

                    if let Some(GraphNode::SelectElement(select_node)) = self.get_node_mut(node_id)
                    {
                        let delta = RowNode::process(select_node, input_delta);
                        tracing::debug!(
                            node_id = node_id.0,
                            node_type,
                            added = delta.added.len(),
                            removed = delta.removed.len(),
                            "graph node evaluated"
                        );
                        tuple_deltas.insert(node_id, delta);
                    }
                }
                Some(GraphNode::RecursiveRelation(_)) => {
                    let input_delta = self
                        .get_inputs(node_id)
                        .first()
                        .and_then(|dep| tuple_deltas.get(dep).cloned())
                        .unwrap_or_default();

                    if let Some(GraphNode::RecursiveRelation(recursive_node)) =
                        self.get_node_mut(node_id)
                    {
                        let delta = recursive_node.process_with_context(
                            input_delta,
                            storage,
                            &mut |id, hint| row_loader(id, hint),
                        );
                        tracing::debug!(
                            node_id = node_id.0,
                            node_type,
                            added = delta.added.len(),
                            removed = delta.removed.len(),
                            "graph node evaluated"
                        );
                        tuple_deltas.insert(node_id, delta);
                    }
                }
                Some(GraphNode::Materialize(_)) => {
                    let input_delta = self
                        .get_inputs(node_id)
                        .first()
                        .and_then(|dep| tuple_deltas.get(dep).cloned())
                        .unwrap_or_default();

                    if let Some(GraphNode::Materialize(mat_node)) = self.get_node_mut(node_id) {
                        let deleted_delta = mat_node.check_deleted_tuples();
                        let new_delta = mat_node.materialize_tuples(input_delta, &mut row_loader);
                        let update_delta = mat_node.check_updated_tuples(&mut row_loader);

                        let mut merged = TupleDelta::new();
                        merged.added.extend(new_delta.added);
                        merged.added.extend(update_delta.added);
                        merged.removed.extend(deleted_delta.removed);
                        merged.removed.extend(new_delta.removed);
                        merged.removed.extend(update_delta.removed);
                        merged.updated.extend(new_delta.updated);
                        merged.updated.extend(update_delta.updated);

                        tracing::debug!(
                            node_id = node_id.0,
                            node_type,
                            added = merged.added.len(),
                            removed = merged.removed.len(),
                            "graph node evaluated"
                        );
                        tuple_deltas.insert(node_id, merged);
                    }
                }
                Some(GraphNode::MagicColumns(_)) => {
                    let input_delta = self
                        .get_inputs(node_id)
                        .first()
                        .and_then(|dep| tuple_deltas.get(dep).cloned())
                        .unwrap_or_default();

                    if let Some(GraphNode::MagicColumns(magic_node)) = self.get_node_mut(node_id) {
                        let delta = magic_node.process_with_context(
                            input_delta,
                            storage,
                            &mut |id, hint| row_loader(id, hint),
                        );
                        tracing::debug!(
                            node_id = node_id.0,
                            node_type,
                            added = delta.added.len(),
                            removed = delta.removed.len(),
                            "graph node evaluated"
                        );
                        tuple_deltas.insert(node_id, delta);
                    }
                }
                Some(GraphNode::Filter(_)) => {
                    let input_delta = self
                        .get_inputs(node_id)
                        .first()
                        .and_then(|dep| tuple_deltas.get(dep).cloned())
                        .unwrap_or_default();

                    if let Some(GraphNode::Filter(filter_node)) = self.get_node_mut(node_id) {
                        let delta = RowNode::process(filter_node, input_delta);
                        tracing::debug!(
                            node_id = node_id.0,
                            node_type,
                            added = delta.added.len(),
                            removed = delta.removed.len(),
                            "graph node evaluated"
                        );
                        tuple_deltas.insert(node_id, delta);
                    }
                }
                Some(GraphNode::PolicyFilter(_)) => {
                    let input_delta = self
                        .get_inputs(node_id)
                        .first()
                        .and_then(|dep| tuple_deltas.get(dep).cloned())
                        .unwrap_or_default();

                    if let Some(GraphNode::PolicyFilter(policy_node)) = self.get_node_mut(node_id) {
                        // Use process_with_context if the policy has INHERITS clauses
                        let delta = if policy_node.has_inherits() {
                            policy_node.process_with_context(
                                input_delta,
                                storage,
                                &mut |id, hint| row_loader(id, hint),
                            )
                        } else {
                            RowNode::process(policy_node, input_delta)
                        };
                        tracing::debug!(
                            node_id = node_id.0,
                            node_type,
                            added = delta.added.len(),
                            removed = delta.removed.len(),
                            "graph node evaluated"
                        );
                        tuple_deltas.insert(node_id, delta);
                    }
                }
                Some(GraphNode::Sort(_)) => {
                    let input_delta = self
                        .get_inputs(node_id)
                        .first()
                        .and_then(|dep| tuple_deltas.get(dep).cloned())
                        .unwrap_or_default();

                    if let Some(GraphNode::Sort(sort_node)) = self.get_node_mut(node_id) {
                        let delta = RowNode::process(sort_node, input_delta);
                        tracing::debug!(
                            node_id = node_id.0,
                            node_type,
                            added = delta.added.len(),
                            removed = delta.removed.len(),
                            "graph node evaluated"
                        );
                        tuple_deltas.insert(node_id, delta);
                    }
                }
                Some(GraphNode::LimitOffset(_)) => {
                    let input_node = self.get_inputs(node_id).first().copied();
                    let delta = input_node
                        .and_then(|dep| {
                            self.process_limit_offset_with_ordered_sort_input(node_id, dep)
                        })
                        .or_else(|| {
                            let Some(GraphNode::LimitOffset(lo_node)) = self.get_node_mut(node_id)
                            else {
                                return None;
                            };
                            let input_delta = input_node
                                .and_then(|dep| tuple_deltas.get(&dep).cloned())
                                .unwrap_or_default();
                            Some(RowNode::process(lo_node, input_delta))
                        });

                    if let Some(delta) = delta {
                        tracing::debug!(
                            node_id = node_id.0,
                            node_type,
                            added = delta.added.len(),
                            removed = delta.removed.len(),
                            "graph node evaluated"
                        );
                        tuple_deltas.insert(node_id, delta);
                    }
                }
                Some(GraphNode::ArraySubquery(_)) => {
                    let input_delta = self
                        .get_inputs(node_id)
                        .first()
                        .and_then(|dep| tuple_deltas.get(dep).cloned())
                        .unwrap_or_default();

                    if let Some(GraphNode::ArraySubquery(subquery_node)) =
                        self.get_node_mut(node_id)
                    {
                        // Check if inner table changed - need to reevaluate all existing instances
                        let mut delta = if subquery_node.is_inner_dirty() {
                            subquery_node
                                .reevaluate_all(storage, &mut |id, hint| row_loader(id, hint))
                        } else {
                            TupleDelta::new()
                        };

                        // Process outer input changes
                        let outer_delta = subquery_node.process_with_context(
                            input_delta,
                            storage,
                            &mut |id, hint| row_loader(id, hint),
                        );

                        // Merge outer delta into combined delta
                        delta.merge(outer_delta);
                        tracing::debug!(
                            node_id = node_id.0,
                            node_type,
                            added = delta.added.len(),
                            removed = delta.removed.len(),
                            "graph node evaluated"
                        );
                        tuple_deltas.insert(node_id, delta);
                    }
                }
                Some(GraphNode::Output(_)) => {
                    let input_node = self.get_inputs(node_id).first().copied();
                    let ordered_input = input_node.and_then(|dep| match self.get_node(dep) {
                        Some(GraphNode::LimitOffset(lo_node)) => {
                            Some(lo_node.windowed_tuples().to_vec())
                        }
                        Some(GraphNode::Sort(sort_node)) => {
                            Some(sort_node.sorted_tuples().to_vec())
                        }
                        _ => None,
                    });

                    if let Some(GraphNode::Output(output_node)) = self.get_node_mut(node_id) {
                        let delta = if let Some(ordered) = ordered_input {
                            output_node.process_with_ordered_input(&ordered)
                        } else {
                            let input_delta = input_node
                                .and_then(|dep| tuple_deltas.get(&dep).cloned())
                                .unwrap_or_default();
                            RowNode::process(output_node, input_delta)
                        };
                        tracing::debug!(
                            node_id = node_id.0,
                            node_type,
                            added = delta.added.len(),
                            removed = delta.removed.len(),
                            "graph node evaluated"
                        );
                        tuple_deltas.insert(node_id, delta);
                    }
                }
                Some(GraphNode::ExistsOutput(_)) => {
                    let input_delta = self
                        .get_inputs(node_id)
                        .first()
                        .and_then(|dep| tuple_deltas.get(dep).cloned())
                        .unwrap_or_default();

                    if let Some(GraphNode::ExistsOutput(exists_node)) = self.get_node_mut(node_id) {
                        let delta = RowNode::process(exists_node, input_delta);
                        tracing::debug!(
                            node_id = node_id.0,
                            node_type,
                            added = delta.added.len(),
                            removed = delta.removed.len(),
                            "graph node evaluated"
                        );
                        tuple_deltas.insert(node_id, delta);
                    }
                }
                None => {}
            }
        }

        self.dirty_bitmap.fill(false);

        // Convert TupleDelta to RowDelta for output
        // For single-table queries: use simple conversion
        // For join queries: flatten multi-element tuples using table descriptors
        tuple_deltas
            .remove(&self.output_node)
            .and_then(|td| {
                if self.table_descriptors.len() == 1 {
                    // Single-table query - direct conversion
                    td.to_row_delta()
                } else {
                    // Join query - flatten multi-element tuples
                    td.flatten_to_row_delta(&self.table_descriptors, &self.combined_descriptor)
                }
            })
            .unwrap_or_default()
    }

    /// Collect tuple sets from input nodes for a transform node.
    fn collect_tuple_inputs(&self, node_id: NodeId) -> Vec<AHashSet<Tuple>> {
        self.get_inputs(node_id)
            .iter()
            .filter_map(|dep| match &self.nodes[dep.0 as usize].node {
                GraphNode::IndexScan(n) => Some(n.current_tuples().clone()),
                GraphNode::Union(n) => Some(n.current_tuples().clone()),
                _ => None,
            })
            .collect()
    }
}
