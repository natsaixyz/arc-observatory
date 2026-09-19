#!/usr/bin/env python3

import json
import sqlite3
import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

DB = "/var/lib/natsai-arc-observatory/observatory.db"
HOST = "127.0.0.1"
PORT = 8790

CL_API = "http://127.0.0.1:31000"
FORENSIC_CACHE_TTL = 15

FORENSIC_CACHE = {
    "at": 0.0,
    "start": None,
    "count": None,
    "value": None,
}

FORENSIC_LOCK = threading.Lock()


def db():
    conn = sqlite3.connect(
        f"file:{DB}?mode=ro",
        uri=True,
        timeout=5,
    )
    conn.row_factory = sqlite3.Row
    return conn


def row_dict(row):
    return dict(row) if row else None


def cl_json(path, timeout=4):
    req = urllib.request.Request(
        CL_API + path,
        headers={
            "Accept": "application/vnd.arc.v1+json",
            "User-Agent": "Natsai-Arc-Observatory/0.2",
        },
    )

    with urllib.request.urlopen(req, timeout=timeout) as response:
        return json.loads(response.read())


def percentile(values, fraction):
    if not values:
        return None

    values = sorted(float(v) for v in values)

    index = round((len(values) - 1) * fraction)
    return round(values[index], 1)


def get_forensics(start, count):
    current = time.monotonic()

    with FORENSIC_LOCK:
        if (
            FORENSIC_CACHE["value"] is not None
            and FORENSIC_CACHE["start"] == start
            and FORENSIC_CACHE["count"] == count
            and current - FORENSIC_CACHE["at"] < FORENSIC_CACHE_TTL
        ):
            return FORENSIC_CACHE["value"]

    result = {}

    endpoints = {
        "proposal_monitor": "proposal-monitor",
        "misbehavior": "misbehavior-evidence",
        "invalid_payloads": "invalid-payloads",
    }

    for key, endpoint in endpoints.items():
        try:
            value = cl_json(
                f"/{endpoint}?height={start}&count={count}"
            )

            result[key] = value if isinstance(value, list) else []
            result[key + "_available"] = isinstance(value, list)

        except Exception:
            result[key] = []
            result[key + "_available"] = False

    with FORENSIC_LOCK:
        FORENSIC_CACHE.update({
            "at": current,
            "start": start,
            "count": count,
            "value": result,
        })

    return result


class Handler(BaseHTTPRequestHandler):

    def send_json(self, obj, status=200):
        body = json.dumps(
            obj,
            separators=(",", ":"),
        ).encode()

        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        path = urlparse(self.path).path

        try:
            if path == "/api/health":
                with db() as conn:
                    conn.execute("SELECT 1").fetchone()

                return self.send_json({
                    "status": "ok"
                })

            if path == "/api/latest":
                with db() as conn:
                    cert = conn.execute("""
                        SELECT
                            height,
                            certificate_round AS round,
                            block_hash,
                            signer_count,
                            total_voting_power,
                            signed_voting_power,
                            ROUND(signed_voting_power_pct, 2)
                                AS signed_voting_power_pct,
                            quorum_required,
                            voting_power_above_quorum,
                            unknown_signer_count,
                            cert_matches_execution,
                            rpc_hash_match,
                            proposer,
                            observed_at
                        FROM certificates
                        ORDER BY height DESC
                        LIMIT 1
                    """).fetchone()

                    status = conn.execute("""
                        SELECT
                            height,
                            round,
                            proposer,
                            sync_state,
                            validator_set_hash,
                            observed_at
                        FROM status_snapshots
                        ORDER BY height DESC
                        LIMIT 1
                    """).fetchone()

                return self.send_json({
                    "latest_certificate": row_dict(cert),
                    "latest_status": row_dict(status),
                })

            if path == "/api/summary":
                with db() as conn:
                    summary = conn.execute("""
                        SELECT
                            COUNT(*) AS certificates,
                            MIN(height) AS first_height,
                            MAX(height) AS latest_height,
                            ROUND(AVG(signed_voting_power_pct), 2)
                                AS avg_signed_voting_power_pct,
                            ROUND(MIN(signed_voting_power_pct), 2)
                                AS min_signed_voting_power_pct,
                            ROUND(MAX(signed_voting_power_pct), 2)
                                AS max_signed_voting_power_pct,
                            SUM(
                                CASE WHEN certificate_round > 0
                                THEN 1 ELSE 0 END
                            ) AS rounds_above_zero,
                            SUM(
                                CASE WHEN cert_matches_execution = 0
                                THEN 1 ELSE 0 END
                            ) AS certificate_mismatches,
                            SUM(
                                CASE WHEN rpc_hash_match = 0
                                THEN 1 ELSE 0 END
                            ) AS rpc_mismatches,
                            SUM(
                                CASE WHEN unknown_signer_count > 0
                                THEN 1 ELSE 0 END
                            ) AS unknown_signer_events
                        FROM certificates
                    """).fetchone()

                    latest_set = conn.execute("""
                        SELECT
                            vs.validator_count,
                            vs.total_voting_power,
                            vs.first_seen_height,
                            vs.last_seen_height,
                            vs.set_hash
                        FROM validator_sets vs
                        ORDER BY vs.last_seen_height DESC
                        LIMIT 1
                    """).fetchone()

                    status = conn.execute("""
                        SELECT
                            height,
                            round,
                            sync_state
                        FROM status_snapshots
                        ORDER BY height DESC
                        LIMIT 1
                    """).fetchone()

                return self.send_json({
                    "network": "Arc Mainnet",
                    "chain_id": 5042,
                    "status": row_dict(status),
                    "validator_set": row_dict(latest_set),
                    "observations": row_dict(summary),
                })

            if path == "/api/rounds":
                with db() as conn:
                    rows = conn.execute("""
                        SELECT
                            certificate_round AS round,
                            COUNT(*) AS certificates
                        FROM certificates
                        GROUP BY certificate_round
                        ORDER BY certificate_round
                    """).fetchall()

                    total = sum(
                        row["certificates"] for row in rows
                    )

                result = []

                for row in rows:
                    count = row["certificates"]
                    result.append({
                        "round": row["round"],
                        "certificates": count,
                        "pct": round(
                            count / total * 100, 2
                        ) if total else 0,
                    })

                return self.send_json({
                    "total": total,
                    "rounds": result,
                })

            if path == "/api/quorum":
                with db() as conn:
                    cert = conn.execute("""
                        SELECT
                            height,
                            certificate_round AS round,
                            block_hash,
                            signer_count,
                            total_voting_power,
                            signed_voting_power,
                            ROUND(
                                signed_voting_power_pct,
                                2
                            ) AS signed_voting_power_pct,
                            quorum_required,
                            voting_power_above_quorum,
                            validator_set_hash,
                            proposer
                        FROM certificates
                        ORDER BY height DESC
                        LIMIT 1
                    """).fetchone()

                    if not cert:
                        return self.send_json({
                            "certificate": None,
                            "validators": [],
                        })

                    validators = conn.execute("""
                        SELECT
                            v.address,
                            v.voting_power,
                            CASE
                                WHEN cs.address IS NULL THEN 0
                                ELSE 1
                            END AS signed
                        FROM validators v
                        LEFT JOIN certificate_signers cs
                            ON cs.height = ?
                            AND cs.address = v.address
                        WHERE v.set_hash = ?
                        ORDER BY
                            v.voting_power DESC,
                            v.address
                    """, (
                        cert["height"],
                        cert["validator_set_hash"],
                    )).fetchall()

                return self.send_json({
                    "certificate": row_dict(cert),
                    "validators": [
                        dict(row) for row in validators
                    ],
                    "note":
                        "Signer inclusion reflects the latest observed finalized certificate.",
                })

            if path == "/api/pulse":
                db_window = 10000
                forensic_window = 100

                with db() as conn:
                    latest_height = conn.execute("""
                        SELECT MAX(height)
                        FROM certificates
                    """).fetchone()[0]

                    if latest_height is None:
                        return self.send_json({
                            "status": "no_data"
                        })

                    last_round_escalation = conn.execute("""
                        SELECT MAX(height)
                        FROM certificates
                        WHERE certificate_round > 0
                    """).fetchone()[0]

                    if last_round_escalation is None:
                        round_zero_streak = conn.execute("""
                            SELECT COUNT(*)
                            FROM certificates
                        """).fetchone()[0]
                    else:
                        round_zero_streak = conn.execute("""
                            SELECT COUNT(*)
                            FROM certificates
                            WHERE height > ?
                        """, (
                            last_round_escalation,
                        )).fetchone()[0]

                    anomalies = conn.execute("""
                        WITH recent AS (
                            SELECT
                                height,
                                certificate_round,
                                cert_matches_execution,
                                rpc_hash_match,
                                unknown_signer_count
                            FROM certificates
                            ORDER BY height DESC
                            LIMIT ?
                        )
                        SELECT *
                        FROM recent
                        WHERE
                            certificate_round > 0
                            OR cert_matches_execution = 0
                            OR rpc_hash_match = 0
                            OR unknown_signer_count > 0
                        ORDER BY height DESC
                        LIMIT 20
                    """, (
                        db_window,
                    )).fetchall()

                start = max(
                    1,
                    int(latest_height) - forensic_window + 1,
                )

                forensic = get_forensics(
                    start,
                    forensic_window,
                )

                proposal_points = []

                for row in forensic["proposal_monitor"]:
                    delay = row.get("proposal_delay_ms")

                    if not isinstance(delay, (int, float)):
                        continue

                    proposal_points.append({
                        "height": row.get("height"),
                        "proposer": row.get("proposer"),
                        "delay_ms": delay,
                    })

                proposal_points.sort(
                    key=lambda x: x["height"] or 0
                )

                delays = [
                    point["delay_ms"]
                    for point in proposal_points
                ]

                misbehavior = []

                for row in forensic["misbehavior"]:
                    validators = row.get("validators") or []

                    if validators:
                        misbehavior.append({
                            "height": row.get("height"),
                            "validator_count": len(validators),
                        })

                invalid_payloads = []

                for row in forensic["invalid_payloads"]:
                    payloads = row.get("payloads") or []

                    if payloads:
                        invalid_payloads.append({
                            "height": row.get("height"),
                            "payload_count": len(payloads),
                        })

                db_anomalies = [
                    dict(row) for row in anomalies
                ]

                event_count = (
                    len(db_anomalies)
                    + len(misbehavior)
                    + len(invalid_payloads)
                )

                return self.send_json({
                    "latest_height": latest_height,
                    "round_zero_streak": round_zero_streak,

                    "proposal_arrival": {
                        "samples": len(delays),
                        "latest_ms":
                            proposal_points[-1]["delay_ms"]
                            if proposal_points else None,
                        "p50_ms": percentile(delays, 0.50),
                        "p95_ms": percentile(delays, 0.95),
                        "points": proposal_points[-60:],
                        "available":
                            forensic[
                                "proposal_monitor_available"
                            ],
                    },

                    "events": {
                        "count": event_count,
                        "database_anomalies": db_anomalies,
                        "misbehavior": misbehavior,
                        "invalid_payloads": invalid_payloads,
                        "misbehavior_available":
                            forensic[
                                "misbehavior_available"
                            ],
                        "invalid_payloads_available":
                            forensic[
                                "invalid_payloads_available"
                            ],
                    },

                    "windows": {
                        "database_certificates":
                            db_window,
                        "forensic_heights":
                            forensic_window,
                    },

                    "note":
                        "Proposal arrival is observed from Natsai infrastructure and should not be interpreted as validator latency.",
                })

            if path == "/api/validators":
                with db() as conn:
                    latest = conn.execute("""
                        SELECT validator_set_hash
                        FROM certificates
                        ORDER BY height DESC
                        LIMIT 1
                    """).fetchone()

                    if not latest:
                        return self.send_json({
                            "validators": []
                        })

                    set_hash = latest["validator_set_hash"]

                    window_size = 10000

                    total = conn.execute("""
                        SELECT COUNT(*) AS n
                        FROM (
                            SELECT height
                            FROM certificates
                            WHERE validator_set_hash = ?
                            ORDER BY height DESC
                            LIMIT ?
                        )
                    """, (set_hash, window_size)).fetchone()["n"]

                    rows = conn.execute("""
                        WITH recent AS (
                            SELECT height
                            FROM certificates
                            WHERE validator_set_hash = ?
                            ORDER BY height DESC
                            LIMIT ?
                        ),
                        signer_counts AS (
                            SELECT
                                cs.address,
                                COUNT(*) AS certificate_appearances
                            FROM certificate_signers cs
                            JOIN recent r
                                ON r.height = cs.height
                            GROUP BY cs.address
                        )
                        SELECT
                            v.address,
                            v.voting_power,
                            v.public_key_hex,
                            COALESCE(
                                sc.certificate_appearances,
                                0
                            ) AS certificate_appearances
                        FROM validators v
                        LEFT JOIN signer_counts sc
                            ON sc.address = v.address
                        WHERE v.set_hash = ?
                        ORDER BY
                            v.voting_power DESC,
                            v.address
                    """, (
                        set_hash,
                        window_size,
                        set_hash,
                    )).fetchall()

                validators = []

                for row in rows:
                    item = dict(row)
                    appearances = item["certificate_appearances"]

                    item["certificate_inclusion_pct"] = round(
                        appearances / total * 100,
                        2,
                    ) if total else 0

                    validators.append(item)

                return self.send_json({
                    "validator_set_hash": set_hash,
                    "certificates_observed": total,
                    "note":
                        "Certificate inclusion is calculated over the latest 10,000 observed finalized certificates and is not a validator uptime metric.",
                    "validators": validators,
                })

            if path == "/" or path == "/api":
                return self.send_json({
                    "name": "Natsai Arc Observatory",
                    "network": "Arc Mainnet",
                    "endpoints": [
                        "/api/health",
                        "/api/summary",
                        "/api/latest",
                        "/api/quorum",
                        "/api/pulse",
                        "/api/validators",
                        "/api/rounds",
                    ],
                })

            return self.send_json({
                "error": "not_found"
            }, 404)

        except Exception as exc:
            return self.send_json({
                "error": "internal_error",
                "message": str(exc),
            }, 500)

    def log_message(self, fmt, *args):
        print(
            f'{self.client_address[0]} '
            f'{self.command} {self.path} '
            f'{fmt % args}',
            flush=True,
        )


if __name__ == "__main__":
    server = ThreadingHTTPServer(
        (HOST, PORT),
        Handler,
    )

    print(
        f"Natsai Arc Observatory API listening "
        f"on http://{HOST}:{PORT}",
        flush=True,
    )

    server.serve_forever()
