"""Unit tests for the synthetic data generator (no database needed)."""
import random
from datetime import date

import pandas as pd
import pytest

from data_generator.generate import generate_day

DAY = date(2026, 10, 1)
SEED = 42


@pytest.fixture(scope="module")
def accounts():
    rng = random.Random(1)
    return pd.DataFrame({
        "account_id": [f"A{i:05d}" for i in range(400)],
        "account_type": [rng.choice(["current", "savings", "business"]) for _ in range(400)],
    })


def test_same_day_is_reproducible(accounts):
    first, truth_first = generate_day(DAY, accounts, SEED)
    second, truth_second = generate_day(DAY, accounts, SEED)
    pd.testing.assert_frame_equal(first, second)
    pd.testing.assert_frame_equal(truth_first, truth_second)


def test_transaction_ids_are_unique(accounts):
    txns, _ = generate_day(DAY, accounts, SEED)
    assert txns["txn_id"].is_unique


def test_ground_truth_points_at_real_transactions(accounts):
    txns, truth = generate_day(DAY, accounts, SEED)
    assert len(truth) > 0
    assert set(truth["txn_id"]) <= set(txns["txn_id"])
    assert set(truth["pattern"]) <= {"structuring", "rapid_in_out"}


def test_decoys_are_not_labelled_as_suspicious(accounts):
    with_decoys, truth = generate_day(DAY, accounts, SEED, decoys=True)
    without, _ = generate_day(DAY, accounts, SEED, decoys=False)
    decoy_ids = set(with_decoys["txn_id"]) - set(without["txn_id"])
    assert decoy_ids, "expected the generator to add decoy transactions"
    assert decoy_ids.isdisjoint(set(truth["txn_id"]))


def test_decoys_do_not_change_the_other_rows(accounts):
    with_decoys, _ = generate_day(DAY, accounts, SEED, decoys=True)
    without, _ = generate_day(DAY, accounts, SEED, decoys=False)
    pd.testing.assert_frame_equal(
        with_decoys.iloc[: len(without)].reset_index(drop=True), without)
