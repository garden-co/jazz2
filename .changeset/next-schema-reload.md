---
"jazz-tools": patch
---

Auto-reload the browser when the schema changes in a Next.js app.
`withJazz` writes the live schema hash into a generated module that the
React provider depends on, so Turbopack and Webpack reload the page
whenever the schema is pushed — no consumer-side wiring required.
