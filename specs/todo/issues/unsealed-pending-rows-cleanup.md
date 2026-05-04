# Unsealed Pending Rows Cleanup

## What

Clean up hidden pending rows that were staged for a transaction but never sealed, for example because the user rolled back, disconnected before sending the seal, threw during application code, or hit a client bug.

## Priority

unknown

## Notes

The server can retain hidden pending rows indefinitely when a transaction is staged but no seal ever arrives.
