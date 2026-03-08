#!/usr/bin/env python3
"""Kafka CLI - A command-line tool for interacting with Apache Kafka."""

import json
import os
import re
import signal
import sys
from pathlib import Path

import click
from jsonpath_ng.ext import parse as jsonpath_parse
from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer, TopicPartition
from kafka.admin import NewTopic
from kafka.errors import KafkaError, NoBrokersAvailable, TopicAlreadyExistsError


def _fix_unquoted_filter_strings(expr: str) -> str:
    """Re-add double quotes around string values stripped by Windows shells.

    Windows/PowerShell strips the double quotes from arguments that contain both
    spaces and double quotes, turning e.g.:
        $[?(@.name=="message 4")]  →  $[?(@.name==message 4)]

    This function detects unquoted string values after comparison operators and
    wraps them in double quotes so jsonpath_ng can parse them correctly.
    """
    def requote(m):
        op, value, closing = m.group(1), m.group(2).strip(), m.group(3)
        if not value or value[0] in ('"', "'"):  # already quoted
            return m.group(0)
        try:
            float(value)                          # numeric literal — leave alone
            return m.group(0)
        except ValueError:
            pass
        if value in ("true", "false"):            # boolean literal — leave alone
            return m.group(0)
        if " " in value:                          # unquoted string with spaces
            return f'{op}"{value}"{closing}'
        return m.group(0)

    return re.sub(
        r'(==|!=|<=|>=|<|>|=~)\s*([^"\'@][^)\]&]*?)(\s*[)\]&])',
        requote,
        expr,
    )


def build_kafka_config(bootstrap_servers, username, password, security_protocol):
    """Build common Kafka connection config from CLI parameters."""
    config = {"bootstrap_servers": bootstrap_servers.split(",")}
    if username and password:
        config.update(
            {
                "security_protocol": security_protocol,
                "sasl_mechanism": "PLAIN",
                "sasl_plain_username": username,
                "sasl_plain_password": password,
            }
        )
    return config


@click.group()
@click.option(
    "--bootstrap-servers",
    "-b",
    required=True,
    envvar="KAFKA_BOOTSTRAP_SERVERS",
    help="Kafka bootstrap servers (host:port,...)",
)
@click.option(
    "--username",
    "-u",
    envvar="KAFKA_USERNAME",
    default=None,
    help="SASL username",
)
@click.option(
    "--password",
    "-p",
    envvar="KAFKA_PASSWORD",
    default=None,
    help="SASL password",
)
@click.option(
    "--security-protocol",
    envvar="KAFKA_SECURITY_PROTOCOL",
    default="PLAINTEXT",
    type=click.Choice(["PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"]),
    show_default=True,
    help="Security protocol",
)
@click.pass_context
def cli(ctx, bootstrap_servers, username, password, security_protocol):
    """Kafka CLI - create topics, subscribe, or publish messages."""
    ctx.ensure_object(dict)
    ctx.obj["bootstrap_servers"] = bootstrap_servers
    ctx.obj["username"] = username
    ctx.obj["password"] = password
    ctx.obj["security_protocol"] = security_protocol


# ---------------------------------------------------------------------------
# create-topic command
# ---------------------------------------------------------------------------


@cli.command("create-topic")
@click.option("--topic", "-t", required=True, help="Name of the topic to create.")
@click.option(
    "--partitions",
    "-p",
    default=1,
    show_default=True,
    type=int,
    help="Number of partitions.",
)
@click.option(
    "--replication-factor",
    "-r",
    default=1,
    show_default=True,
    type=int,
    help="Replication factor.",
)
@click.option(
    "--retention-ms",
    default=None,
    type=int,
    help="Message retention period in milliseconds (e.g. 86400000 for 24 h).",
)
@click.option(
    "--retention-bytes",
    default=None,
    type=int,
    help="Max bytes retained per partition before old segments are discarded.",
)
@click.option(
    "--cleanup-policy",
    default=None,
    type=click.Choice(["delete", "compact", "compact,delete"]),
    help="Log cleanup policy (default: delete).",
)
@click.option(
    "--segment-ms",
    default=None,
    type=int,
    help="Period after which a log segment is rolled even if not full (ms).",
)
@click.option(
    "--if-not-exists",
    is_flag=True,
    default=False,
    help="Succeed silently if the topic already exists.",
)
@click.pass_context
def create_topic(
    ctx,
    topic,
    partitions,
    replication_factor,
    retention_ms,
    retention_bytes,
    cleanup_policy,
    segment_ms,
    if_not_exists,
):
    """Create a Kafka topic."""
    config = ctx.obj

    # Build optional topic-level configs.
    topic_configs = {}
    if retention_ms is not None:
        topic_configs["retention.ms"] = str(retention_ms)
    if retention_bytes is not None:
        topic_configs["retention.bytes"] = str(retention_bytes)
    if cleanup_policy is not None:
        topic_configs["cleanup.policy"] = cleanup_policy
    if segment_ms is not None:
        topic_configs["segment.ms"] = str(segment_ms)

    new_topic = NewTopic(
        name=topic,
        num_partitions=partitions,
        replication_factor=replication_factor,
        topic_configs=topic_configs,
    )

    admin_config = build_kafka_config(
        config["bootstrap_servers"],
        config["username"],
        config["password"],
        config["security_protocol"],
    )

    try:
        admin = KafkaAdminClient(**admin_config)
    except NoBrokersAvailable:
        click.echo(
            f"Error: could not connect to brokers at {config['bootstrap_servers']}",
            err=True,
        )
        sys.exit(1)

    try:
        admin.create_topics([new_topic])
        click.echo(
            f"Created topic '{topic}' "
            f"(partitions={partitions}, replication-factor={replication_factor}"
            + (f", retention.ms={retention_ms}" if retention_ms is not None else "")
            + (f", retention.bytes={retention_bytes}" if retention_bytes is not None else "")
            + (f", cleanup.policy={cleanup_policy}" if cleanup_policy is not None else "")
            + (f", segment.ms={segment_ms}" if segment_ms is not None else "")
            + ")"
        )
    except TopicAlreadyExistsError:
        if if_not_exists:
            click.echo(f"Topic '{topic}' already exists — skipping.")
        else:
            click.echo(f"Error: topic '{topic}' already exists.", err=True)
            sys.exit(1)
    except KafkaError as exc:
        click.echo(f"Error: failed to create topic — {exc}", err=True)
        sys.exit(1)
    finally:
        admin.close()


# ---------------------------------------------------------------------------
# consume command
# ---------------------------------------------------------------------------


@cli.command()
@click.option("--topic", "-t", required=True, help="Topic to consume from.")
@click.option(
    "--group-id",
    "-g",
    default="kafka-cli",
    show_default=True,
    help="Consumer group ID.",
)
@click.option(
    "--filter",
    "-f",
    "json_filter",
    default=None,
    help=(
        "JSONPath expression to filter/extract from JSON messages. "
        "Only messages that match are printed. "
        "Examples: '$.status', '$.items[*].name', "
        "'$.events[?(@.type==\"click\")]'"
    ),
)
@click.option(
    "--from-beginning",
    is_flag=True,
    default=False,
    help="Start consuming from the beginning of the topic.",
)
@click.option(
    "--pretty",
    is_flag=True,
    default=False,
    help="Pretty-print JSON messages.",
)
@click.option(
    "--timeout",
    default=None,
    type=int,
    help="Stop consuming after N seconds of inactivity.",
)
@click.pass_context
def consume(ctx, topic, group_id, json_filter, from_beginning, pretty, timeout):
    """Subscribe to a topic and print messages as they arrive."""
    config = ctx.obj

    # Parse JSONPath expression up front so errors surface immediately.
    # Pre-process first to restore any double quotes stripped by Windows shells.
    jsonpath_expr = None
    if json_filter:
        json_filter = _fix_unquoted_filter_strings(json_filter)
        try:
            jsonpath_expr = jsonpath_parse(json_filter)
        except Exception as exc:
            click.echo(f"Error: invalid JSONPath expression — {exc}", err=True)
            sys.exit(1)

    consumer_config = build_kafka_config(
        config["bootstrap_servers"],
        config["username"],
        config["password"],
        config["security_protocol"],
    )
    consumer_config.update(
        {
            "group_id": group_id,
            "auto_offset_reset": "earliest" if from_beginning else "latest",
            "enable_auto_commit": True,
            "value_deserializer": lambda m: m.decode("utf-8", errors="replace"),
            "consumer_timeout_ms": timeout * 1000 if timeout else float("inf"),
        }
    )

    try:
        if from_beginning:
            # auto_offset_reset only applies when no committed offsets exist for the
            # consumer group. To unconditionally start at offset 0 we use manual
            # partition assignment (bypasses group coordination and saved offsets).
            consumer = KafkaConsumer(**consumer_config)
            partitions = consumer.partitions_for_topic(topic)
            if not partitions:
                # Metadata not cached yet — trigger a fetch then retry.
                consumer.poll(timeout_ms=2000)
                partitions = consumer.partitions_for_topic(topic)
            if not partitions:
                click.echo(f"Error: topic '{topic}' not found or has no partitions.", err=True)
                consumer.close()
                sys.exit(1)
            tp_list = [TopicPartition(topic, p) for p in sorted(partitions)]
            consumer.assign(tp_list)
            consumer.seek_to_beginning(*tp_list)
        else:
            consumer = KafkaConsumer(topic, **consumer_config)
    except NoBrokersAvailable:
        click.echo(
            f"Error: could not connect to brokers at {config['bootstrap_servers']}",
            err=True,
        )
        sys.exit(1)

    click.echo(f"Consuming from '{topic}' (Ctrl+C to stop)…", err=True)

    def _shutdown(sig, frame):
        click.echo("\nStopping…", err=True)
        consumer.close()
        sys.exit(0)

    signal.signal(signal.SIGINT, _shutdown)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _shutdown)

    for message in consumer:
        _print_message(message.value, jsonpath_expr, pretty)

    consumer.close()


def _print_message(raw: str, jsonpath_expr, pretty: bool) -> None:
    """Print a single message, applying an optional JSONPath filter."""
    if jsonpath_expr is not None:
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            click.echo(
                "Warning: non-JSON message skipped by filter.", err=True
            )
            return

        matches = jsonpath_expr.find(data)

        # $[?(@.field==value)] predicates only match against sequences, not plain
        # objects. If the root is a dict and nothing matched, retry with the object
        # wrapped in a list so root-level filter expressions work as expected.
        if not matches and isinstance(data, dict):
            matches = jsonpath_expr.find([data])

        if not matches:
            return  # No match — suppress the message.

        values = [m.value for m in matches]
        output = values[0] if len(values) == 1 else values
        _echo_value(output, pretty)
    else:
        if pretty:
            try:
                data = json.loads(raw)
                click.echo(json.dumps(data, indent=2))
                return
            except json.JSONDecodeError:
                pass
        click.echo(raw)


def _echo_value(value, pretty: bool) -> None:
    if isinstance(value, (dict, list)):
        click.echo(json.dumps(value, indent=2) if pretty else json.dumps(value))
    else:
        click.echo(str(value))


# ---------------------------------------------------------------------------
# produce command
# ---------------------------------------------------------------------------


def _natural_key(path: Path):
    """Sort key that orders embedded numbers numerically (m2 < m10)."""
    return [
        int(part) if part.isdigit() else part.lower()
        for segment in path.parts
        for part in re.split(r"(\d+)", segment)
    ]


def _collect_files(root: Path) -> list[Path]:
    """Recursively collect files under root, sorted at every directory level.

    Both directory names and filenames are sorted with natural (numeric-aware)
    ordering at each level, so m2.json always precedes m10.json.
    """
    files = []
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames.sort(key=lambda d: _natural_key(Path(d)))
        for name in sorted(filenames, key=lambda f: _natural_key(Path(f))):
            files.append(Path(dirpath) / name)
    return files


def _send_one(producer: KafkaProducer, topic: str, payload: str, key: str | None) -> bool:
    """Send a single message. Returns True on success, False on error."""
    try:
        future = producer.send(topic, value=payload, key=key)
        meta = future.get(timeout=10)
        click.echo(
            f"Sent to {meta.topic}[partition={meta.partition}, offset={meta.offset}]"
        )
        return True
    except KafkaError as exc:
        click.echo(f"Error: failed to send message — {exc}", err=True)
        return False


@cli.command()
@click.option("--topic", "-t", required=True, help="Topic to publish to.")
@click.option("--message", "-m", default=None, help="Message payload (string).")
@click.option("--key", "-k", default=None, help="Optional message key.")
@click.option(
    "--file",
    "-f",
    "path",
    default=None,
    type=click.Path(exists=True),
    help=(
        "File or directory to read message(s) from. "
        "A single file is published as one message. "
        "A directory is walked recursively; every file becomes a message, "
        "ordered by sorted file path (use '-' for stdin)."
    ),
)
@click.pass_context
def produce(ctx, topic, message, key, path):
    """Publish one or more messages to a topic."""
    config = ctx.obj

    producer_config = build_kafka_config(
        config["bootstrap_servers"],
        config["username"],
        config["password"],
        config["security_protocol"],
    )
    producer_config.update(
        {
            "value_serializer": lambda v: v.encode("utf-8"),
            "key_serializer": lambda k: k.encode("utf-8") if k else None,
        }
    )

    try:
        producer = KafkaProducer(**producer_config)
    except NoBrokersAvailable:
        click.echo(
            f"Error: could not connect to brokers at {config['bootstrap_servers']}",
            err=True,
        )
        sys.exit(1)

    try:
        if path is not None and os.path.isdir(path):
            # --- directory mode: publish every file found recursively ---
            files = _collect_files(Path(path))
            if not files:
                click.echo(f"Error: no files found under '{path}'.", err=True)
                sys.exit(1)
            click.echo(
                f"Publishing {len(files)} file(s) from '{path}'…", err=True
            )
            errors = 0
            for file_path in files:
                click.echo(f"  {file_path}", err=True)
                payload = file_path.read_text(encoding="utf-8")
                if not _send_one(producer, topic, payload, key):
                    errors += 1
            if errors:
                click.echo(f"{errors} message(s) failed.", err=True)
                sys.exit(1)
        else:
            # --- single-message mode: file, --message, or stdin ---
            if path is not None:
                payload = Path(path).read_text(encoding="utf-8")
            elif message is not None:
                payload = message
            elif not sys.stdin.isatty():
                payload = sys.stdin.read()
            else:
                click.echo(
                    "Error: provide --message, --file, a directory, or pipe via stdin.",
                    err=True,
                )
                sys.exit(1)
            if not _send_one(producer, topic, payload, key):
                sys.exit(1)
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    cli()
