# Square Transport Foundation

The initial Square implementation is transport-only. It uses Apache Thrift
Compact Protocol and the existing `pkg/line.Client` HTTP/session lifecycle.

`CallRead` accepts only a `SquareReadOperation`; it does not accept a URL,
method, or arbitrary callback. The private operation registry is intentionally
empty until endpoint, exact path, method, and argument schema are verified.
Consequently, authenticated Square network calls are denied by default.
`Manager.DoSquareReadContext` uses the existing session credential and refresh
lifecycle while accepting only the registered operation identifier.

## Authentication handling

HTTP 401/403 responses remain compatible with the existing `ResponseError`,
`IsAuthError`, refresh, and invalidation behavior. Standard Thrift application
exceptions are decoded as typed errors but are not classified as authentication
or logout responses. Other result fields, including any possible service
exception struct, remain opaque until their schema is verified.

The repository does not contain verified Thrift service exception field IDs or
service codes for auth mappings. No field-ID interpretation or auth mapping is
made.

## Rust compatibility

Compared against `softnetics/line-rs` commit
`addf69c2e886a2364a396ce360093a62119dcb32`:

- `src/request/mod.rs` selects `/SQ1` and `/SQLV1` for Square, uses
  `SquareService_<method>_args`/`_result` when present, and requests Compact
  Protocol through its caller.
- `src/service/square.rs` constructs `getJoinedSquares` arguments as a
  `request` containing `limit` and `continuationToken`. The bundled
  `resources/line.thrift` defines request field 1, continuation token field 2,
  limit field 3; the response contains a Square list and several maps.
- `src/thrift/wire.rs` fixes Compact message sequence ID to 0 and emits one
  additional STOP after a non-empty args struct. The Go network path now uses
  sequence ID 0 and matches that framing.
- `tests/schema_compat.rs::square_send_message_args` contains a Square request
  golden whose comment says the bytes were produced by linejs + Node thrift.
  `TestRustLineRSSquareSendMessageGoldenBytes` matches those bytes exactly.
  This verifies request encoding against a fixture maintained by line-rs, but
  is not a Rust-generated response fixture or proof of live server behavior.

The Go decoder now retains list, set, and map values and caps nesting and
collection sizes. The request encoder still supports scalar and nested-struct
values only. There is still no Rust golden response fixture for
`getJoinedSquares`; response field mapping and Square service-exception
interpretation remain unverified. The operation registry remains empty.

## Protocol uncertainties

- Although line-rs documents `/SQ1` and the `getJoinedSquares` IDL, this
  repository has not established that these are the exact Chrome extension
  endpoint/header contract. The allowlist therefore remains empty.
- The decoded result is generic; no typed `Square` result model is registered.
- Square service-exception auth/logout mappings and the exact response golden
  remain unknown.
- Authentication headers beyond existing Chrome headers are not inferred.
- Request-side list/map/set encoding, schema-aware binary decoding, and typed
  response models remain future work.
