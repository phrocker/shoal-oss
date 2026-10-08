# Executor attestation

> **Status:** this is the verifier half of #446 slice 3. `internal/executorattest`
> verifies statements and stores the latest verified result. Fleet enforcement
> (claims that require a current attestation, the ATPL `attestation` field, the
> `POST /api/v1/fleet/executors/attestation` route and the SDK) is pending and
> not wired. Nothing in the fleet consults this package yet.

An executor attests that it runs a pinned image by presenting a statement signed
by an operator verifier. The server checks it against a trust file and records
the latest verified result per principal and executor ref.

## Statement

The evidence is a JSON envelope:

```json
{"verifier_id": "operator-key:1", "statement": { ... }, "signature": "<base64url Ed25519>"}
```

The signature is Ed25519 over `"shoal-executor-statement-v1\x00" || statement`.
That signing context differs from collector statements
(`shoal-collector-statement-v1\x00`), so a collector statement is never accepted
as an executor statement, and the reverse.

The statement is canonical JSON with exactly these fields, in this order. A
non-canonical encoding (whitespace, reordered or unknown fields) is refused:

| Field | Meaning |
|---|---|
| `authorization_domain` | base64url (no padding) of the caller's authorization domain |
| `subject` | base64url of the caller's subject |
| `client_id` | base64url of the caller's client |
| `executor_ref` | the fleet executor ref |
| `image_digest` | `sha256:<64 lowercase hex>` |
| `provenance_digest` | optional, same form |
| `issued_at`, `expires_at` | RFC 3339 times |
| `nonce` | `executorattest.Nonce(principal, ref, key)` |

The nonce is the hex SHA-256 of a domain tag, domain, subject, client, ref and
the presentation's idempotency key, each length-prefixed. A statement minted for
one presentation cannot be replayed under another key or by another principal.
The principal always comes from the authentication decision, never from the
request.

## Trust file

```json
{"executors": {
  "executor:deploy": {
    "verifiers": [{"id": "operator-key:1", "public_key": "<std base64, 32 bytes>",
                   "max_validity": "1h", "clock_skew": "1m"}],
    "image_digests": ["sha256:..."],
    "provenance_digests": ["sha256:..."]
  }}}
```

Parsing is strict: unknown fields and trailing data are refused. Each ref needs
at least one verifier and one image digest. `max_validity` must be in (0, 1h],
and `clock_skew` must be in [0, 1m]. Verifier IDs and keys are unique within a
ref. Across refs, an ID is always bound to the same key, so the `VerifierID`
names one signer. Duplicate or malformed digests are refused.
`provenance_digests` is optional. When it is set, a statement must carry a
pinned provenance digest.

## Verification

`Verify(trust, Expectation{Principal, ExecutorRef, Key, Now}, evidence)` refuses
on any of these:

- an unknown ref or verifier;
- a bad signature, including a wrong signing context;
- malformed or non-canonical bytes;
- a mismatch on any bound field (domain, subject, client, ref, nonce);
- an unpinned image digest, or an unpinned provenance digest;
- validity over `max_validity`;
- `issued_at` later than now plus skew;
- expiry, judged strictly: `now >= expires_at` is expired.

Skew applies only to `issued_at`. It never extends expiry.

Each refusal carries a typed `Reason` for audit (`ReasonOf`). Presenters only
ever see the one opaque `ErrRefused` (`Public`).

## Store and lapse rule

`Store` keeps one row per (domain, subject, client, ref) on a
`decisionstore.CAS` backend, table `_shoal_executor_attestations`. The row holds
the latest verified result: a content-addressed `AttestationID`, `VerifierID`,
verifier key digest, image and provenance digests, `IssuedAt` and `ExpiresAt`.

- Re-presenting the recorded statement is idempotent.
- A different statement must have a strictly later `IssuedAt`. Otherwise it is
  refused as a rollback.

`Current(ctx, principal, ref, now, needUntil)` is ok only if all of these hold:

- a row exists;
- its verifier is still configured with the same key;
- its digests are still pinned;
- its validity still fits the verifier's current `max_validity`;
- `now < ExpiresAt`;
- `ExpiresAt >= needUntil`.

Trust is read on every check, so removing trust takes effect for the next
check. A store failure is returned as an error, which callers map to
unavailable. It is never "not attested".

**A claim never outlives its attestation.** When fleet enforcement lands, a claim
or extension will be granted only if `Current` holds with `needUntil` equal to
the new lease end. A lapse then coincides with a lease lapse, and a worker
re-attests before it extends. Removing trust lets live claims run to their lease
end, but refuses new claims and extensions.

## Residual trust

As with collector statements, a verified statement proves only that the holder
of an operator key vouched for these bindings. It does not prove what code
runs. The operator's signing process and key custody are trusted. Hardware and
code-signing verifiers are deferred.
