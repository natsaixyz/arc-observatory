#!/usr/bin/env python3

import json
import sqlite3
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

DB = "/var/lib/natsai-arc-observatory/observatory.db"
HOST = "127.0.0.1"
PORT = 8790


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

                    total = conn.execute("""
                        SELECT COUNT(*) AS n
                        FROM certificates
                        WHERE validator_set_hash = ?
                    """, (set_hash,)).fetchone()["n"]

                    rows = conn.execute("""
                        SELECT
                            v.address,
                            v.voting_power,
                            v.public_key_hex,
                            COUNT(cs.height) AS certificate_appearances
                        FROM validators v
                        LEFT JOIN certificates c
                            ON c.validator_set_hash = v.set_hash
                        LEFT JOIN certificate_signers cs
                            ON cs.height = c.height
                            AND cs.address = v.address
                        WHERE v.set_hash = ?
                        GROUP BY
                            v.address,
                            v.voting_power,
                            v.public_key_hex
                        ORDER BY
                            v.voting_power DESC,
                            v.address
                    """, (set_hash,)).fetchall()

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
                        "Certificate inclusion reflects appearance in finalized certificates and is not a validator uptime metric.",
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
