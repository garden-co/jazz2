---
"jazz-tools": patch
---

Fix `$createdAt` and `$updatedAt` provenance magic columns in `jazz-tools` so they round-trip as real JavaScript dates instead of far-future timestamps.

Queries now convert JS millisecond `Date` and numeric filter values to the internal provenance timestamp format consistently, so selecting and filtering on those magic columns uses the same time scale.
