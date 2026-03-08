# Security Policy

## Reporting a Vulnerability

If you discover a security vulnerability in `jobviz-agent`, please report it responsibly:

1. **Do not** open a public GitHub issue.
2. Email **security@jobviz.dev** with a description of the vulnerability, steps to reproduce, and any relevant logs or screenshots.
3. You will receive an acknowledgement within 48 hours and a detailed response within 5 business days.

## Scope

This policy covers the `jobviz-agent` npm package — the client-side SDK that runs inside your Node.js application. It does **not** cover the Jobviz hosted platform (app.jobviz.dev); for platform-level issues, contact us at the same email above.

## What we consider in scope

- Credential leakage (API keys, Redis URLs, or job data exposed via logs, HTTP headers, or error messages)
- Prototype pollution or injection via job payloads
- Denial of service through unbounded memory or CPU usage
- Data exfiltration (agent sending data to endpoints other than the configured one)
- Bypass of redaction (`redactKeys`) allowing sensitive fields to be transmitted

## Security design decisions

| Area | Design |
|------|--------|
| **API key handling** | Never logged, never included in error messages, never sent in debug health endpoint responses |
| **Data redaction** | Recursive key redaction with prototype-pollution guards (`__proto__`, `constructor`, `prototype` are skipped). Input objects larger than 1 MB are replaced entirely to prevent CPU stalls. |
| **Debug endpoint** | Binds to `127.0.0.1` only (not `0.0.0.0`). No authentication — intended for local diagnostics only. |
| **Transport** | HTTPS by default. Bearer token auth. 10-second request timeout. No retry on 4xx (prevents amplification). |
| **Memory safety** | Event buffer capped at `maxBufferSize` (default 10 000). Job metadata cache capped at 50 000 entries with FIFO eviction and 30-min TTL. |

## Supported Versions

| Version | Supported |
|---------|-----------|
| 0.1.x   | Yes       |

We will backport critical security fixes to the latest minor release.
