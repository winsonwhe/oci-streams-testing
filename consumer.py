#!/usr/bin/env python3
"""Consume JSON messages produced from Excel rows in an OCI Stream."""

from __future__ import annotations

import argparse
import base64
import json
import os
import socket
import sys
import time
from pathlib import Path
from typing import Any, TextIO

import oci


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Consume OCI Streams messages and write enriched JSON Lines."
    )
    parser.add_argument("--stream-ocid", default=os.getenv("OCI_STREAM_OCID"))
    parser.add_argument("--endpoint", default=os.getenv("OCI_MESSAGE_ENDPOINT"))
    parser.add_argument(
        "--config-file",
        default=os.getenv("OCI_CONFIG_FILE", "~/.oci/config"),
        help="OCI SDK config file (default: ~/.oci/config)",
    )
    parser.add_argument(
        "--profile",
        default=os.getenv("OCI_CONFIG_PROFILE", "DEFAULT"),
        help="OCI SDK profile name (default: DEFAULT)",
    )
    parser.add_argument(
        "--auth",
        choices=("config", "instance-principal"),
        default=os.getenv("OCI_AUTH", "config"),
    )
    parser.add_argument("--group", default="excel-metrics-consumers")
    parser.add_argument(
        "--instance",
        default=f"{socket.gethostname()}-{os.getpid()}",
        help="Unique consumer-group instance name",
    )
    parser.add_argument(
        "--cursor-type",
        choices=("TRIM_HORIZON", "LATEST"),
        default="TRIM_HORIZON",
        help="Used only when the consumer group is first created",
    )
    parser.add_argument("--limit", type=int, default=50, help="Messages per GET")
    parser.add_argument("--timeout-ms", type=int, default=30000)
    parser.add_argument(
        "--commit-on-get",
        action="store_true",
        help="Let OCI commit the previous GET; default is commit after output succeeds",
    )
    parser.add_argument(
        "--follow",
        action="store_true",
        help="Keep polling when no messages are currently available",
    )
    parser.add_argument("--poll-seconds", type=float, default=1.0)
    parser.add_argument(
        "--output",
        type=Path,
        help="Append JSON Lines to this file; defaults to standard output",
    )
    return parser.parse_args()


def create_stream_client(args: argparse.Namespace):
    if args.auth == "instance-principal":
        signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
        return oci.streaming.StreamClient(
            {}, service_endpoint=args.endpoint, signer=signer
        )
    config = oci.config.from_file(
        file_location=str(Path(args.config_file).expanduser()),
        profile_name=args.profile,
    )
    return oci.streaming.StreamClient(config, service_endpoint=args.endpoint)


def create_group_cursor(client: Any, args: argparse.Namespace) -> str:
    details = oci.streaming.models.CreateGroupCursorDetails(
        group_name=args.group,
        instance_name=args.instance,
        type=args.cursor_type,
        timeout_in_ms=args.timeout_ms,
        commit_on_get=args.commit_on_get,
    )
    response = client.create_group_cursor(args.stream_ocid, details)
    return response.data.value


def decode_message(message: Any) -> dict[str, Any]:
    key = (
        base64.b64decode(message.key).decode("utf-8")
        if message.key is not None
        else None
    )
    value_text = base64.b64decode(message.value).decode("utf-8")
    payload = json.loads(value_text)
    timestamp = getattr(message, "timestamp", None)
    if hasattr(timestamp, "isoformat"):
        timestamp = timestamp.isoformat()
    return {
        "stream": {
            "key": key,
            "partition": getattr(message, "partition", None),
            "offset": getattr(message, "offset", None),
            "timestamp": timestamp,
        },
        "message": payload,
    }


def write_batch(output: TextIO, messages: list[Any]) -> None:
    for message in messages:
        record = decode_message(message)
        output.write(
            json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n"
        )
    output.flush()


def consume(client: Any, args: argparse.Namespace, output: TextIO) -> int:
    cursor = create_group_cursor(client, args)
    total = 0
    print(
        f"consumer started: group={args.group!r} instance={args.instance!r} "
        f"manual_commit={not args.commit_on_get}",
        file=sys.stderr,
    )

    while True:
        response = client.get_messages(
            args.stream_ocid,
            cursor,
            limit=args.limit,
            retry_strategy=oci.retry.DEFAULT_RETRY_STRATEGY,
        )
        next_cursor = response.headers["opc-next-cursor"]
        messages = response.data

        if messages:
            # Decode and durably flush output before a manual offset commit.
            write_batch(output, messages)
            total += len(messages)
            print(f"consumed batch={len(messages)} total={total}", file=sys.stderr)
            if not args.commit_on_get:
                commit_response = client.consumer_commit(
                    args.stream_ocid,
                    next_cursor,
                    retry_strategy=oci.retry.DEFAULT_RETRY_STRATEGY,
                )
                cursor = commit_response.data.value
            else:
                cursor = next_cursor
            continue

        cursor = next_cursor
        if not args.follow:
            break
        time.sleep(args.poll_seconds)

    print(f"consumer stopped: total={total}", file=sys.stderr)
    return total


def main() -> int:
    args = parse_args()
    if not args.stream_ocid or not args.endpoint:
        raise ValueError(
            "Set --stream-ocid/OCI_STREAM_OCID and "
            "--endpoint/OCI_MESSAGE_ENDPOINT"
        )
    if not 1 <= args.limit <= 10000:
        raise ValueError("--limit must be between 1 and 10000")
    if args.timeout_ms < 1:
        raise ValueError("--timeout-ms must be positive")
    if args.poll_seconds < 0:
        raise ValueError("--poll-seconds cannot be negative")

    client = create_stream_client(args)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        with args.output.open("a", encoding="utf-8") as output:
            consume(client, args, output)
    else:
        consume(client, args, sys.stdout)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("Interrupted", file=sys.stderr)
        raise SystemExit(130)
    except Exception as error:
        print(f"ERROR: {error}", file=sys.stderr)
        raise SystemExit(1)
