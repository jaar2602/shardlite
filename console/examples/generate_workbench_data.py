#!/usr/bin/env python3
"""Generate reproducible synthetic INSERT and UPDATE SQL for the console test schema.

The generator uses Python's standard pseudo-random number generator and fixed word lists. It does
not use AI, make network requests, connect to MeshDB, or execute the generated SQL.
"""

from __future__ import annotations

import argparse
import json
import random
import re
import sys
from dataclasses import dataclass


FIRST_NAMES = (
    "Ada",
    "Amina",
    "Chen",
    "Diego",
    "Elena",
    "Farah",
    "Grace",
    "Hiro",
    "Iris",
    "Jamal",
    "Kai",
    "Lina",
    "Mateo",
    "Nora",
    "Omar",
    "Priya",
    "Quinn",
    "Ravi",
    "Sofia",
    "Tariq",
)
LAST_NAMES = (
    "Baker",
    "Costa",
    "Das",
    "Evans",
    "Fischer",
    "Garcia",
    "Hassan",
    "Ito",
    "Jones",
    "Khan",
    "Lee",
    "Martin",
    "Ng",
    "Ortiz",
    "Patel",
    "Reed",
    "Singh",
    "Tan",
    "Usman",
    "Vega",
)
CHANNELS = ("api", "batch", "console", "import", "mobile", "web")
PREFIX_RE = re.compile(r"^[A-Za-z0-9_-]+$")


@dataclass(frozen=True)
class Account:
    account_id: str
    email: str


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed < 1:
        raise argparse.ArgumentTypeError("must be at least 1")
    return parsed


def non_negative_int(value: str) -> int:
    parsed = int(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("must be zero or greater")
    return parsed


def prefix_value(value: str) -> str:
    if not PREFIX_RE.fullmatch(value):
        raise argparse.ArgumentTypeError("use only letters, numbers, underscores, and hyphens")
    return value


def sql_text(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def random_blob(rng: random.Random, size: int = 12) -> str:
    return "X'" + bytes(rng.getrandbits(8) for _ in range(size)).hex() + "'"


def emit(statement: str, data_key: str) -> None:
    print(f"-- Data key: {data_key}")
    print(statement)
    print()


def generate(args: argparse.Namespace) -> None:
    rng = random.Random(args.seed)
    accounts: list[Account] = []
    event_id = rng.randrange(1_000_000_000, 8_000_000_000)

    print("-- Synthetic MeshDB console data")
    print(f"-- seed={args.seed} accounts={args.accounts} events_per_account={args.events_per_account} updates={args.updates}")
    print("-- Generated locally from fixed word lists and Python's pseudo-random generator; no AI is used.")
    print("-- Run the schema section of workbench-test.sql first.")
    print("-- Run each statement in the unified workbench using the Data key printed above it.")
    print()

    for number in range(1, args.accounts + 1):
        account_id = f"{args.prefix}:{number:06d}"
        first = rng.choice(FIRST_NAMES)
        last = rng.choice(LAST_NAMES)
        email = f"{first}.{last}.{number}.{args.seed}@example.test".lower()
        balance = rng.randrange(0, 500_001)
        active = 1 if rng.random() < 0.85 else 0
        profile = random_blob(rng)
        accounts.append(Account(account_id, email))

        emit(
            "INSERT INTO console_test_accounts "
            "(account_id, email, balance_cents, active, profile) VALUES "
            f"({sql_text(account_id)}, {sql_text(email)}, {balance}, {active}, {profile});",
            account_id,
        )

        for event_number in range(1, args.events_per_account + 1):
            kind = rng.choices(("credit", "debit", "note"), weights=(5, 3, 2), k=1)[0]
            amount = "NULL" if kind == "note" else str(rng.randrange(100, 25_001))
            metadata = json.dumps(
                {
                    "channel": rng.choice(CHANNELS),
                    "generated": True,
                    "sequence": event_number,
                },
                separators=(",", ":"),
                sort_keys=True,
            )
            emit(
                "INSERT INTO console_test_events "
                "(event_id, account_id, kind, amount_cents, metadata) VALUES "
                f"({event_id}, {sql_text(account_id)}, {sql_text(kind)}, {amount}, {sql_text(metadata)});",
                account_id,
            )
            event_id += 1

    for update_number in range(1, args.updates + 1):
        account = rng.choice(accounts)
        active = rng.randrange(0, 2)
        balance_change = rng.randrange(-5_000, 10_001)
        updated_email = account.email.replace("@", f".update{update_number}@")
        emit(
            "UPDATE console_test_accounts SET "
            f"email = {sql_text(updated_email)}, "
            f"balance_cents = MAX(0, balance_cents + ({balance_change})), "
            f"active = {active}, profile = {random_blob(rng)} "
            f"WHERE account_id = {sql_text(account.account_id)};",
            account.account_id,
        )


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(
        description="Generate synthetic SQL for the console_test_accounts and console_test_events tables."
    )
    result.add_argument("--accounts", type=positive_int, default=25, help="account INSERT count (default: 25)")
    result.add_argument("--events-per-account", type=non_negative_int, default=1, help="event INSERTs per account (default: 1)")
    result.add_argument("--updates", type=non_negative_int, default=10, help="account UPDATE count (default: 10)")
    result.add_argument("--seed", type=int, default=20260722, help="repeatable random seed (default: 20260722)")
    result.add_argument("--prefix", type=prefix_value, default="generated", help="data-key prefix (default: generated)")
    return result


def main() -> int:
    try:
        generate(parser().parse_args())
    except BrokenPipeError:
        return 0
    return 0


if __name__ == "__main__":
    sys.exit(main())
