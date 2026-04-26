---
"chat-react": patch
---

Fix race condition where the message composer could enable before the auto-join
chatMembers insert was acknowledged by the server, allowing a message send to
fail with a permissions error. The composer now stays disabled until the
membership insert has been confirmed at edge durability.
