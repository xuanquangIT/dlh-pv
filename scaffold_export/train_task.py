"""Copied train_task wrapper for scaffold export."""
import argparse
from types import SimpleNamespace

from src.model.train import run_training


def parse_args():
    parser = argparse.ArgumentParser(description="Scaffold train task wrapper")
    parser.add_argument("--dataset", default="data/model_ready/dataset.parquet")
    parser.add_argument("--target", default="energy_mwh")
    parser.add_argument("--estimator", default="gradient_boosting")
    parser.add_argument("--n-splits", type=int, default=5)
    parser.add_argument("--test-size", type=int, default=0)
    parser.add_argument("--initial-train-size", type=int, default=None)
    parser.add_argument("--tag", default="")
    return parser.parse_args()


def main():
    args = parse_args()
    ns = SimpleNamespace(
        dataset=args.dataset,
        target=args.target,
        features=None,
        time_column=None,
        lags=None,
        estimator=args.estimator,
        n_splits=args.n_splits,
        test_size=args.test_size,
        initial_train_size=args.initial_train_size,
        step_size=None,
        tag=args.tag,
    )
    metadata = run_training(ns)
    print('Training finished. Model metadata written to:', metadata.get('folds', [{}])[0].get('model_path'))


if __name__ == "__main__":
    main()
