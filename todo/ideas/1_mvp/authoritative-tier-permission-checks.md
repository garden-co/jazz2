# Authoritative Tier Permission Checks

## What

Explore a separate authoritative permission preflight that can ask a chosen durability tier, such as edge or global, to decide whether a write would be allowed when local-only `canInsert` or `canUpdate` is not enough.

## Notes

- Keep the current `canInsert` and `canUpdate` API local-only for fast UI gating.
- Consider a distinct API for authoritative checks instead of adding tier back to local preflight.
- The check should make clear which authority answered: local runtime, edge server, or global server.
- Useful for product flows where disabling or enabling UI must reflect server-known permissions, not just local cached policy state.
- Open questions:
  - should this be a one-shot RPC, a subscription-backed policy decision, or a query settlement mode?
  - how should `"unknown"` behave when the requested tier is unreachable?
  - should the server return denial reasons, and under what privacy constraints?
  - how should this interact with optimistic writes and eventual server rejection?
