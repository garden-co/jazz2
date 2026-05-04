---
"jazz-tools": patch
---

Keep dev plugins running when the initial schema auto-push cannot reach a configured remote Jazz server. The plugins now warn and continue with the configured app and server URL, while still failing on schema, auth, or server rejections.
