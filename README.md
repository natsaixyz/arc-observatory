# Natsai Arc Observatory

Operator-side consensus and finality telemetry for Arc Mainnet, operated by Natsai.

Live dashboard: https://arc.natsai.xyz

Public RPC: https://rpc.arc-mainnet.natsai.xyz

## What it does

Arc Observatory watches Arc Mainnet from Natsai-operated infrastructure and records consensus/finality telemetry over time.

For observed finalized blocks it:

- records the Arc finality certificate
- snapshots the validator set and voting power context
- calculates signed voting power and BFT quorum margin
- records certificate signer inclusion
- verifies the certificate block hash against local execution
- compares the local executed block with Arc's public RPC
- records consensus round information
- stores historical observations in SQLite
- exposes the resulting data through a read-only HTTP API and dashboard

## Why

Raw RPC health only tells an operator whether a node is responding.

Arc Observatory is intended to make Arc consensus and finality behavior easier to inspect over time from an infrastructure operator's perspective.

It provides historical answers to questions such as:

- How much voting power is represented in finalized certificates?
- How far above the BFT quorum are certificates finalizing?
- Are certificates finalizing at round 0 or requiring additional rounds?
- Does the certificate block hash match local execution?
- Does the Natsai node agree with Arc's public RPC at the same height?
- How often is each validator signature included in observed certificates?

Certificate inclusion is not a validator uptime or performance score.

## Architecture

Arc consensus status at 127.0.0.1:31000/status feeds validator-set and consensus context into the collector.

The local Arc execution RPC at 127.0.0.1:8545 provides execution blocks and Arc certificates.

Arc's public RPC is queried as an external consistency reference.

The collector stores observations in SQLite.

The read-only API serves the stored telemetry to the public dashboard.

## Components

- collector.py — consensus/finality collector
- api.py — read-only JSON API
- web/index.html — public dashboard
- deploy/systemd — example production services
- deploy/nginx — example reverse-proxy configuration

## Live consensus telemetry

The public dashboard includes several Arc consensus views built from Natsai-operated infrastructure:

- **Live Quorum** — shows which validators signed the latest observed finality certificate, signed voting power, BFT quorum threshold and margin above quorum
- **Consensus Pulse** — tracks the current observed Round-0 certificate streak and recent consensus activity
- **Proposal Arrival** — displays proposal arrival observations from Natsai infrastructure, including p50 and p95 latency
- **Consensus Events** — surfaces round escalation, certificate/execution mismatches, RPC inconsistencies, unknown signers, misbehavior evidence and invalid payloads
- **Integrity checks** — compares Arc finality certificates with local execution and the official Arc public RPC

Proposal arrival is an observation from Natsai infrastructure and should not be interpreted as validator latency or a validator performance score.

## API

The live API is available under:

https://arc.natsai.xyz/api/

Current endpoints:

- /api/health
- /api/summary
- /api/latest
- /api/quorum
- /api/pulse
- /api/validators
- /api/rounds

## Network

Arc Mainnet

Chain ID: 5042

## Requirements

The current collector uses only the Python standard library and SQLite support included with Python.

The production Natsai deployment currently expects:

- Arc consensus API on 127.0.0.1:31000
- Arc execution JSON-RPC on 127.0.0.1:8545

## Interpretation

Validator certificate inclusion indicates that a validator signature appeared in an observed finalized certificate.

It should not be interpreted as validator uptime, reliability, or a performance ranking.

The Observatory publishes objective telemetry rather than assigning validator scores.

## About Natsai

Natsai operates validator, RPC and blockchain infrastructure across multiple networks.

https://natsai.xyz

This project is operated independently by Natsai and is not an official Circle or Arc product.
