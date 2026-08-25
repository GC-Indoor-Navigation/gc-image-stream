# Authorized ingest session handoff

## Problem

The multi-camera ingest gate stopped collection when any expected camera
disconnected after collection began. That protected a synchronized capture from
silently continuing with missing cameras, but the stopped flag belonged to the
Stream process rather than to an authenticated capture session. Every later
session was rejected until the container restarted.

## Implementation

Stream now records the capture-session ID that owns the ingest collection and
counts authenticated gRPC streams before their first frame arrives.

- Streams for the same capture session may connect concurrently and camera
  credential rotation remains an overlapping same-session connection.
- A different capture session is rejected while any authenticated stream from
  the current session is still connected.
- After every current-session stream has closed, the first stream from a new
  authenticated session resets the gate, device, timestamp, and collection
  counters before it starts ingesting.
- A disconnected session cannot reopen its own stopped gate merely by
  reconnecting; Main must establish a new capture session for a new collection.

The authenticated stream count is acquired immediately after token validation,
before packet iteration. This prevents two different sessions from both taking
ownership during the interval before either sends its first frame.

## Reliability boundary

The handoff resets only transient ingest-gate state. Durable frames, manifests,
and relay work remain intact. The synchronization matcher already fences frame
sets by capture-session authorization scope, so prior durable history is not
discarded during handoff.

## Verification

Tests cover active-session exclusion, stopped-gate reset, new owner visibility,
and authenticated stream-count release. Docker validation runs two complete
three-camera sessions consecutively without restarting Stream or Processing.
