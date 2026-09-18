#!/usr/bin/env python3

import argparse
import hashlib
import json
import signal
import sqlite3
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

CL_STATUS = "http://127.0.0.1:31000/status"
LOCAL_RPC = "http://127.0.0.1:8545"
OFFICIAL_RPC = "https://rpc.mainnet.arc.io"

USER_AGENT = "Natsai-Arc-Observatory/0.1"

STOP = threading.Event()


class RpcError(Exception):
    def __init__(self, code, message):
        super().__init__(f"RPC {code}: {message}")
        self.code = code
        self.message = message


def now():
    return datetime.now(timezone.utc).isoformat()


def http_json(url, timeout=5):
    req = urllib.request.Request(
        url,
        headers={
            "Accept": "application/json",
            "User-Agent": USER_AGENT,
        },
    )

    with urllib.request.urlopen(req, timeout=timeout) as response:
        return json.loads(response.read())


def rpc(url, method, params=None, timeout=8):
    payload = json.dumps({
        "jsonrpc": "2.0",
        "id": 1,
        "method": method,
        "params": params or [],
    }).encode()

    req = urllib.request.Request(
        url,
        data=payload,
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": USER_AGENT,
        },
        method="POST",
    )

    with urllib.request.urlopen(req, timeout=timeout) as response:
        body = json.loads(response.read())

    if "error" in body:
        error = body["error"]
        raise RpcError(
            error.get("code"),
            error.get("message", "Unknown RPC error"),
        )

    return body["result"]


def connect_db(path):
    conn = sqlite3.connect(path, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db(conn):
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS validator_sets (
        set_hash TEXT PRIMARY KEY,
        total_voting_power INTEGER NOT NULL,
        validator_count INTEGER NOT NULL,
        first_seen_height INTEGER NOT NULL,
        last_seen_height INTEGER NOT NULL,
        observed_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS validators (
        set_hash TEXT NOT NULL,
        address TEXT NOT NULL,
        voting_power INTEGER NOT NULL,
        public_key_hex TEXT,
        PRIMARY KEY (set_hash, address)
    );

    CREATE TABLE IF NOT EXISTS status_snapshots (
        height INTEGER PRIMARY KEY,
        round INTEGER,
        proposer TEXT,
        sync_state TEXT,
        height_start_time TEXT,
        validator_set_hash TEXT NOT NULL,
        observed_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS certificates (
        height INTEGER PRIMARY KEY,
        certificate_round INTEGER,
        block_hash TEXT,
        execution_hash TEXT,
        official_hash TEXT,
        signer_count INTEGER,
        total_voting_power INTEGER,
        signed_voting_power INTEGER,
        signed_voting_power_pct REAL,
        quorum_required INTEGER,
        voting_power_above_quorum INTEGER,
        unknown_signer_count INTEGER,
        cert_matches_execution INTEGER,
        rpc_hash_match INTEGER,
        validator_set_hash TEXT,
        proposer TEXT,
        observed_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS certificate_signers (
        height INTEGER NOT NULL,
        address TEXT NOT NULL,
        voting_power INTEGER,
        PRIMARY KEY (height, address)
    );

    CREATE INDEX IF NOT EXISTS idx_status_set
        ON status_snapshots(validator_set_hash);

    CREATE INDEX IF NOT EXISTS idx_cert_set
        ON certificates(validator_set_hash);
    """)

    conn.commit()


def validator_set_hash(validators):
    canonical = [
        {
            "address": v["address"].lower(),
            "voting_power": int(v["voting_power"]),
            "public_key_hex": v.get("public_key_hex"),
        }
        for v in validators
    ]

    canonical.sort(key=lambda x: x["address"])

    raw = json.dumps(
        canonical,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()

    return hashlib.sha256(raw).hexdigest()


def store_status(conn, status):
    height = int(status["height"])
    validator_set = status["validator_set"]
    validators = validator_set["validators"]

    set_hash = validator_set_hash(validators)

    conn.execute("""
        INSERT OR IGNORE INTO validator_sets (
            set_hash,
            total_voting_power,
            validator_count,
            first_seen_height,
            last_seen_height,
            observed_at
        )
        VALUES (?, ?, ?, ?, ?, ?)
    """, (
        set_hash,
        int(validator_set["total_voting_power"]),
        int(validator_set["count"]),
        height,
        height,
        now(),
    ))

    conn.execute("""
        UPDATE validator_sets
        SET last_seen_height =
            CASE
                WHEN last_seen_height < ? THEN ?
                ELSE last_seen_height
            END
        WHERE set_hash = ?
    """, (
        height,
        height,
        set_hash,
    ))

    for validator in validators:
        conn.execute("""
            INSERT OR IGNORE INTO validators (
                set_hash,
                address,
                voting_power,
                public_key_hex
            )
            VALUES (?, ?, ?, ?)
        """, (
            set_hash,
            validator["address"].lower(),
            int(validator["voting_power"]),
            validator.get("public_key_hex"),
        ))

    conn.execute("""
        INSERT INTO status_snapshots (
            height,
            round,
            proposer,
            sync_state,
            height_start_time,
            validator_set_hash,
            observed_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(height) DO UPDATE SET
            round = excluded.round,
            proposer = excluded.proposer,
            sync_state = excluded.sync_state,
            height_start_time = excluded.height_start_time,
            validator_set_hash = excluded.validator_set_hash,
            observed_at = excluded.observed_at
    """, (
        height,
        status.get("round"),
        status.get("proposer"),
        status.get("sync_state"),
        status.get("height_start_time"),
        set_hash,
        now(),
    ))

    conn.commit()

    return height, set_hash


def collect_certificate(conn, height, set_hash, proposer):
    height_hex = hex(height)

    try:
        cert = rpc(
            LOCAL_RPC,
            "arc_getCertificate",
            [height_hex],
        )
    except RpcError as exc:
        if exc.code == -32004:
            return False
        raise

    local_block = rpc(
        LOCAL_RPC,
        "eth_getBlockByNumber",
        [height_hex, False],
    )

    official_block = rpc(
        OFFICIAL_RPC,
        "eth_getBlockByNumber",
        [height_hex, False],
    )

    validators = dict(conn.execute("""
        SELECT address, voting_power
        FROM validators
        WHERE set_hash = ?
    """, (set_hash,)).fetchall())

    set_row = conn.execute("""
        SELECT total_voting_power
        FROM validator_sets
        WHERE set_hash = ?
    """, (set_hash,)).fetchone()

    if not set_row:
        return False

    total_voting_power = int(set_row[0])

    signatures = cert.get("signatures", [])

    signer_rows = []

    signed_voting_power = 0
    unknown_signers = 0

    for signature in signatures:
        address = signature["address"].lower()
        voting_power = validators.get(address)

        if voting_power is None:
            unknown_signers += 1
        else:
            signed_voting_power += int(voting_power)

        signer_rows.append((
            height,
            address,
            voting_power,
        ))

    quorum_required = (2 * total_voting_power) // 3 + 1

    execution_hash = local_block["hash"]
    official_hash = official_block["hash"]
    certificate_hash = cert["block_hash"]

    cert_matches_execution = (
        certificate_hash.lower() == execution_hash.lower()
    )

    rpc_hash_match = (
        execution_hash.lower() == official_hash.lower()
    )

    signed_pct = (
        signed_voting_power / total_voting_power * 100
        if total_voting_power
        else 0
    )

    conn.execute("""
        INSERT OR REPLACE INTO certificates (
            height,
            certificate_round,
            block_hash,
            execution_hash,
            official_hash,
            signer_count,
            total_voting_power,
            signed_voting_power,
            signed_voting_power_pct,
            quorum_required,
            voting_power_above_quorum,
            unknown_signer_count,
            cert_matches_execution,
            rpc_hash_match,
            validator_set_hash,
            proposer,
            observed_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        height,
        int(cert["round"]),
        certificate_hash,
        execution_hash,
        official_hash,
        len(signatures),
        total_voting_power,
        signed_voting_power,
        signed_pct,
        quorum_required,
        signed_voting_power - quorum_required,
        unknown_signers,
        int(cert_matches_execution),
        int(rpc_hash_match),
        set_hash,
        proposer,
        now(),
    ))

    for row in signer_rows:
        conn.execute("""
            INSERT OR REPLACE INTO certificate_signers (
                height,
                address,
                voting_power
            )
            VALUES (?, ?, ?)
        """, row)

    conn.commit()

    if (
        not cert_matches_execution
        or not rpc_hash_match
        or unknown_signers
        or signed_voting_power < quorum_required
        or int(cert["round"]) > 0
    ):
        print(
            f"NOTICE height={height} "
            f"round={cert['round']} "
            f"signers={len(signatures)} "
            f"power={signed_voting_power}/{total_voting_power} "
            f"pct={signed_pct:.2f} "
            f"unknown={unknown_signers} "
            f"cert_match={cert_matches_execution} "
            f"rpc_match={rpc_hash_match}",
            flush=True,
        )

    return True


def sampler(db_path, interval):
    conn = connect_db(db_path)

    previous_set = None
    previous_height = None

    while not STOP.is_set():
        try:
            status = http_json(CL_STATUS)

            height, set_hash = store_status(conn, status)

            if set_hash != previous_set:
                print(
                    f"validator-set height={height} "
                    f"count={status['validator_set']['count']} "
                    f"power={status['validator_set']['total_voting_power']} "
                    f"hash={set_hash[:12]}",
                    flush=True,
                )
                previous_set = set_hash

            if (
                previous_height is None
                or height >= previous_height + 100
            ):
                print(
                    f"status height={height} "
                    f"round={status.get('round')} "
                    f"sync={status.get('sync_state')}",
                    flush=True,
                )
                previous_height = height

        except Exception as exc:
            print(f"status error: {exc}", flush=True)

        STOP.wait(interval)

    conn.close()


def certificate_worker(db_path, interval):
    conn = connect_db(db_path)

    processed = 0

    while not STOP.is_set():
        try:
            row = conn.execute("""
                SELECT
                    s.height,
                    s.validator_set_hash,
                    s.proposer
                FROM status_snapshots s
                LEFT JOIN certificates c
                    ON c.height = s.height
                WHERE
                    c.height IS NULL
                    AND s.height < (
                        SELECT MAX(height)
                        FROM status_snapshots
                    )
                ORDER BY s.height
                LIMIT 1
            """).fetchone()

            if not row:
                STOP.wait(interval)
                continue

            height, set_hash, proposer = row

            if collect_certificate(
                conn,
                int(height),
                set_hash,
                proposer,
            ):
                processed += 1

                if processed <= 5 or processed % 100 == 0:
                    cert = conn.execute("""
                        SELECT
                            signed_voting_power_pct,
                            signer_count,
                            certificate_round
                        FROM certificates
                        WHERE height = ?
                    """, (height,)).fetchone()

                    print(
                        f"certificate height={height} "
                        f"round={cert[2]} "
                        f"signers={cert[1]} "
                        f"power={cert[0]:.2f}%",
                        flush=True,
                    )
            else:
                STOP.wait(interval)

        except Exception as exc:
            print(f"certificate error: {exc}", flush=True)
            STOP.wait(1)

    conn.close()


def stop_handler(signum, frame):
    STOP.set()


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--db",
        default="/var/lib/natsai-arc-observatory/observatory.db",
    )

    parser.add_argument(
        "--sample-interval",
        type=float,
        default=0.25,
    )

    parser.add_argument(
        "--worker-interval",
        type=float,
        default=0.5,
    )

    args = parser.parse_args()

    conn = connect_db(args.db)
    init_db(conn)
    conn.close()

    signal.signal(signal.SIGTERM, stop_handler)
    signal.signal(signal.SIGINT, stop_handler)

    print("Natsai Arc Observatory starting", flush=True)

    threads = [
        threading.Thread(
            target=sampler,
            args=(args.db, args.sample_interval),
            daemon=True,
        ),
        threading.Thread(
            target=certificate_worker,
            args=(args.db, args.worker_interval),
            daemon=True,
        ),
    ]

    for thread in threads:
        thread.start()

    while not STOP.is_set():
        time.sleep(0.5)

    for thread in threads:
        thread.join(timeout=5)

    print("Natsai Arc Observatory stopped", flush=True)


if __name__ == "__main__":
    main()
