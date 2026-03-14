#!/usr/bin/env python3
"""
Migrate MongoDB collections into separate NornicDB databases via Neo4j HTTP API.

Source collections:
  - nornic_translation
  - nornic_translation_text

Target databases (same names as source collections).

Rules:
  - Exclude openai_embedding and mongo-internal id fields.
  - Keep all other fields as node properties.
  - One node per Mongo document (label: MongoDocument).

Usage:
  python scripts/migrate_mongodb_to_nornic.py \
    --mongo-uri "mongodb+srv://..." \
    --nornic-login-url "https://.../nornic-db/login" \
    --nornic-user "<user>" \
    --nornic-password "<password>"
"""

from __future__ import annotations

import argparse
import datetime as dt
import decimal
import json
import re
import sys
import time
from typing import Any, Dict, Iterable, List
from urllib.parse import urlparse

import requests
from bson import ObjectId
from pymongo import MongoClient
from pymongo.errors import CursorNotFound


DEFAULT_COLLECTIONS = ["nornic_translation", "nornic_translation_text"]
EXCLUDED_FIELDS = {
    "_id",
    "openai_embedding",
    "mongo_id",
    "mongoid",
    "internal_mongo_id",
    "internalmongoid",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="MongoDB -> NornicDB migration")
    parser.add_argument("--mongo-uri", required=True, help="MongoDB connection URI")
    parser.add_argument(
        "--mongo-db",
        default="nornic-translation-prod",
        help="MongoDB database name (default: nornic-translation-prod)",
    )
    parser.add_argument(
        "--collections",
        nargs="+",
        default=DEFAULT_COLLECTIONS,
        help=f"Mongo collection names (default: {' '.join(DEFAULT_COLLECTIONS)})",
    )
    parser.add_argument(
        "--nornic-login-url",
        required=True,
        help="Nornic login URL (e.g. https://host/nornic-db/login)",
    )
    parser.add_argument("--nornic-user", required=True, help="Nornic username")
    parser.add_argument("--nornic-password", required=True, help="Nornic password")
    parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="Batch size for UNWIND inserts (default: 500)",
    )
    parser.add_argument(
        "--no-clear-target",
        action="store_true",
        help="Do not delete existing nodes before import (default behavior is to clear)",
    )
    parser.add_argument(
        "--mongo-cursor-retries",
        type=int,
        default=20,
        help="Max retries for cursor resume on CursorNotFound (default: 20)",
    )
    parser.add_argument(
        "--mongo-cursor-retry-sleep-seconds",
        type=float,
        default=1.0,
        help="Seconds to sleep between cursor resume attempts (default: 1.0)",
    )
    return parser.parse_args()


def to_service_base_url(login_url: str) -> str:
    parsed = urlparse(login_url)
    path = parsed.path.rstrip("/")
    if path.endswith("/login"):
        path = path[: -len("/login")]
    return parsed._replace(path=path, params="", query="", fragment="").geturl().rstrip("/")


def sanitize_key(key: str) -> str:
    # Normalize keys to parser-safe property names.
    clean = re.sub(r"[^A-Za-z0-9_]", "_", key.strip())
    if not clean:
        clean = "field"
    if clean[0].isdigit():
        clean = f"f_{clean}"
    return clean


def should_exclude_field(key: str) -> bool:
    normalized = key.lower().replace("-", "_")
    return normalized in EXCLUDED_FIELDS


def sanitize_value(value: Any) -> Any:
    if isinstance(value, ObjectId):
        return str(value)
    if isinstance(value, dt.datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=dt.timezone.utc)
        return value.isoformat()
    if isinstance(value, dt.date):
        return value.isoformat()
    if isinstance(value, decimal.Decimal):
        return float(value)
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if isinstance(value, dict):
        return sanitize_document(value)
    if isinstance(value, list):
        return [sanitize_value(v) for v in value]
    return value


def sanitize_document(doc: Dict[str, Any]) -> Dict[str, Any]:
    clean: Dict[str, Any] = {}
    for raw_key, raw_val in doc.items():
        original_key = str(raw_key)
        if should_exclude_field(original_key):
            continue
        key = sanitize_key(original_key)
        value = sanitize_value(raw_val)

        # Keep scalar values directly; stringify complex nested values to avoid
        # parser edge cases in SET n = row over HTTP parameter substitution.
        if isinstance(value, (dict, list)):
            value = json.dumps(value, ensure_ascii=False)

        # Avoid silent overwrite when two source keys sanitize to same key.
        if key in clean:
            suffix = 2
            alt = f"{key}_{suffix}"
            while alt in clean:
                suffix += 1
                alt = f"{key}_{suffix}"
            key = alt
        clean[key] = value
    return clean


class NornicNeo4jHTTPClient:
    def __init__(self, service_base_url: str, user: str, password: str) -> None:
        self.service_base_url = service_base_url.rstrip("/")
        self.auth = (user, password)
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})

    def close(self) -> None:
        self.session.close()

    def execute(
        self,
        database: str,
        statement: str,
        parameters: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        endpoint = f"{self.service_base_url}/db/{database}/tx/commit"
        payload = {"statements": [{"statement": statement, "parameters": parameters or {}}]}
        resp = self.session.post(endpoint, auth=self.auth, json=payload, timeout=120)
        resp.raise_for_status()
        body = resp.json()
        errors = body.get("errors", [])
        if errors:
            raise RuntimeError(json.dumps(errors, indent=2))
        results = body.get("results", [])
        if not results:
            return {"columns": [], "rows": [], "rowCount": 0}
        data_rows = results[0].get("data", [])
        rows = [entry.get("row", []) for entry in data_rows]
        return {"columns": results[0].get("columns", []), "rows": rows, "rowCount": len(rows)}


def chunks(items: List[Dict[str, Any]], size: int) -> Iterable[List[Dict[str, Any]]]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


def ensure_database(client: NornicNeo4jHTTPClient, db_name: str) -> None:
    stmt = f"CREATE DATABASE `{db_name}` IF NOT EXISTS"
    client.execute("system", stmt)


def clear_database(client: NornicNeo4jHTTPClient, db_name: str) -> None:
    stmt = "MATCH (n) DETACH DELETE n"
    client.execute(db_name, stmt)


def insert_batch(client: NornicNeo4jHTTPClient, db_name: str, docs: List[Dict[str, Any]]) -> int:
    # Use deterministic single-row inserts instead of UNWIND batching.
    # Some deployments exhibit parser/runtime edge cases with very large
    # parameter arrays; this path prioritizes correctness over throughput.
    inserted = 0
    single_stmt = "CREATE (n:MongoDocument) SET n = $row"
    for i, row in enumerate(docs):
        try:
            client.execute(db_name, single_stmt, {"row": row})
            inserted += 1
        except Exception as exc:
            print(f"[WARN] Skipping bad document in batch index {i}: {exc}")
    return inserted


def get_node_counts(client: NornicNeo4jHTTPClient, db_name: str) -> Dict[str, int]:
    result = client.execute(
        db_name,
        "MATCH (n) RETURN count(n) AS total, count(CASE WHEN n:MongoDocument THEN 1 END) AS mongo_docs",
    )
    rows = result.get("rows", [])
    if not rows:
        return {"total": 0, "mongo_docs": 0}
    row = rows[0]
    total = int(row[0]) if len(row) > 0 and row[0] is not None else 0
    mongo_docs = int(row[1]) if len(row) > 1 and row[1] is not None else 0
    return {"total": total, "mongo_docs": mongo_docs}


def is_multidb_unavailable_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    return "database manager not available" in msg or "requires multi-database support" in msg


def iter_collection_with_resume(
    coll: Any,
    batch_size: int,
    max_retries: int,
    retry_sleep_seconds: float,
    start_after_id: Any | None = None,
) -> Iterable[Dict[str, Any]]:
    """
    Iterate collection in stable _id order and resume after CursorNotFound.
    """
    last_id: Any | None = start_after_id
    retries = 0

    while True:
        query: Dict[str, Any] = {}
        if last_id is not None:
            query["_id"] = {"$gt": last_id}

        cursor = (
            coll.find(query, no_cursor_timeout=True)
            .sort("_id", 1)
            .batch_size(max(1, batch_size))
        )
        advanced = False
        try:
            for doc in cursor:
                advanced = True
                retries = 0
                doc_id = doc.get("_id")
                if doc_id is not None:
                    last_id = doc_id
                yield doc
        except CursorNotFound as exc:
            retries += 1
            if retries > max_retries:
                raise RuntimeError(
                    f"Mongo cursor lost after {max_retries} retries; last _id={last_id}"
                ) from exc
            print(
                f"[WARN] CursorNotFound (retry {retries}/{max_retries}); "
                f"resuming from _id>{last_id}"
            )
            time.sleep(max(0.0, retry_sleep_seconds))
            continue
        finally:
            try:
                cursor.close()
            except Exception:
                pass

        if not advanced:
            break
        break


def migrate_collection(
    mongo_db: Any,
    collection_name: str,
    client: NornicNeo4jHTTPClient,
    batch_size: int,
    clear_target: bool,
    mongo_cursor_retries: int,
    mongo_cursor_retry_sleep_seconds: float,
) -> None:
    db_name = collection_name
    coll = mongo_db[collection_name]
    if collection_name not in mongo_db.list_collection_names():
        print(f"[WARN] Mongo collection not found, skipping: {collection_name}")
        return

    print(f"[INFO] Ensuring Nornic database exists: {db_name}")
    try:
        ensure_database(client, db_name)
    except Exception as exc:
        if is_multidb_unavailable_error(exc):
            raise RuntimeError(
                f"Target server does not support CREATE DATABASE for '{db_name}'. "
                "Multi-database must be enabled on the NornicDB instance."
            ) from exc
        raise

    if clear_target:
        print(f"[INFO] Clearing target database: {db_name}")
        clear_database(client, db_name)

    total_source = coll.count_documents({})
    print(f"[INFO] Migrating {collection_name} -> {db_name}, source docs: {total_source}")

    already_migrated = 0
    start_after_id = None
    if not clear_target:
        existing = get_node_counts(client, db_name)
        already_migrated = existing.get("mongo_docs", 0)
        if already_migrated > 0:
            print(f"[INFO] {collection_name}: existing mongo_docs={already_migrated}, resuming forward")
        if already_migrated >= total_source:
            print(
                f"[DONE] {collection_name}: nothing to migrate "
                f"(already_migrated={already_migrated} >= source={total_source})"
            )
            return
        if already_migrated > 0:
            # Find the _id at the existing offset and resume from the next document.
            marker = (
                coll.find({}, {"_id": 1})
                .sort("_id", 1)
                .skip(max(0, already_migrated - 1))
                .limit(1)
            )
            marker_doc = next(marker, None)
            if marker_doc is not None and "_id" in marker_doc:
                start_after_id = marker_doc["_id"]
            else:
                # Defensive fallback when offset lookup fails: restart from beginning.
                already_migrated = 0
                start_after_id = None
                print(
                    f"[WARN] {collection_name}: could not resolve resume marker from source; "
                    "starting from beginning"
                )

    batch: List[Dict[str, Any]] = []
    inserted = already_migrated
    processed = already_migrated
    for doc in iter_collection_with_resume(
        coll=coll,
        batch_size=batch_size,
        max_retries=mongo_cursor_retries,
        retry_sleep_seconds=mongo_cursor_retry_sleep_seconds,
        start_after_id=start_after_id,
    ):
        batch.append(sanitize_document(doc))
        processed += 1
        if len(batch) >= batch_size:
            inserted += insert_batch(client, db_name, batch)
            batch = []
            counts = get_node_counts(client, db_name)
            print(
                f"[INFO] {collection_name}: processed={processed} inserted={inserted} "
                f"db_total={counts['total']} db_mongo_docs={counts['mongo_docs']}"
            )

    if batch:
        inserted += insert_batch(client, db_name, batch)

    final_counts = get_node_counts(client, db_name)
    print(
        f"[DONE] {collection_name}: processed={processed}, inserted={inserted}, "
        f"db_total={final_counts['total']}, db_mongo_docs={final_counts['mongo_docs']}"
    )


def main() -> int:
    args = parse_args()
    service_base_url = to_service_base_url(args.nornic_login_url)
    print(f"[INFO] Nornic service base URL: {service_base_url}")

    mongo_client = MongoClient(args.mongo_uri)
    nornic = NornicNeo4jHTTPClient(service_base_url, args.nornic_user, args.nornic_password)
    try:
        mongo_db = mongo_client[args.mongo_db]
        clear_target = not args.no_clear_target
        for coll in args.collections:
            migrate_collection(
                mongo_db=mongo_db,
                collection_name=coll,
                client=nornic,
                batch_size=args.batch_size,
                clear_target=clear_target,
                mongo_cursor_retries=args.mongo_cursor_retries,
                mongo_cursor_retry_sleep_seconds=args.mongo_cursor_retry_sleep_seconds,
            )
    finally:
        nornic.close()
        mongo_client.close()

    print("[SUCCESS] Migration complete.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
