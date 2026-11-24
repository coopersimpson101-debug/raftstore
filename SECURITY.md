# Security policy

## Supported versions

Only the latest commit on `main` is supported.

## Reporting a vulnerability

Please **do not open a public issue** for security problems. Use GitHub's
[private vulnerability reporting](https://github.com/coopersimpson101-debug/raftstore/security/advisories/new)
("Security" tab → "Report a vulnerability"). Include the affected version or commit, a description, and a
reproducer if possible. You should get an acknowledgement within 7 days. Please allow up to 90 days
for a fix before public disclosure.

## Scope and threat model

raftstore is designed for a **trusted home-lab network**. These reports are in scope:

- ways to read or write data without a valid client certificate when mutual TLS is enabled;
- ways for a network peer to make a node acknowledge data that is not durable, or to lose
  acknowledged data, within the documented [fault model](docs/fault-model.md);
- path traversal or arbitrary file access via the HTTP API or chunk names.

These are documented non-goals and out of scope: Byzantine (malicious) cluster members, deployments without TLS on
untrusted networks, drives that lie about `fsync`, and denial of service by an authenticated client.
To enable mutual TLS, see [deploy/tls/README.md](deploy/tls/README.md).
