# Inactive session manifest retirement

## Problem

The durable Stream database can retain an `ELIGIBLE` latest-live manifest after
its Main processing job has already stopped. On restart, requesting a relay
credential for that manifest returns HTTP 409. Treating this response as a
transient transport failure increased reconnect backoff and delayed a newly
active session beyond the 500 ms freshness budget.

## Implementation

- Main credential HTTP 409 is translated into the explicit
  `RelayCredentialSessionInactive` lifecycle signal.
- The exact, still-eligible manifest is moved to `SESSION_INACTIVE` with the
  reason `RELAY_CREDENTIAL_SESSION_INACTIVE`.
- The relay loop returns to its 50 ms idle poll without incrementing connection
  backoff, allowing a newer active-session manifest to be selected immediately.
- Other HTTP failures remain transient failures and keep the existing bounded
  exponential backoff behavior.

## Reliability boundary

Retirement is keyed by the full frame-set identity and only applies while the
projection is still `ELIGIBLE`. It never modifies an offered/in-flight frame or
the durable watermark, so a concurrent state transition cannot be overwritten.

## Verification

Unit coverage verifies HTTP 409 translation, safe store retirement, and the
client's no-backoff behavior for inactive jobs. The Docker happy-path E2E is the
integration check for quick recovery from stale manifests across restarts.
