import argparse
import os
import time
import tracemalloc
from datetime import datetime

from text_to_vocabulary.domain.vocabulary import LEXICAL_CATEGORIES
from text_to_vocabulary.storage.ods_exporter import export_storage_to_single_file
from text_to_vocabulary.storage.sqlite_vocabulary_storage import SQLiteVocabularyStorage


def build_dataset(per_category: int, duplicate_rate: float) -> dict[str, list[str]]:
    dataset = {}
    dup_count = max(int(per_category * duplicate_rate), 0)
    for category in LEXICAL_CATEGORIES:
        words = [f"{category}_word_{index}" for index in range(per_category)]
        if dup_count:
            words.extend(words[:dup_count])
        dataset[category] = words
    return dataset


def run_merge(storage: SQLiteVocabularyStorage, dataset: dict[str, list[str]]) -> None:
    total_words = sum(len(words) for words in dataset.values())
    start = time.perf_counter()
    counts = storage.merge_categories(dataset, source="perf")
    elapsed = time.perf_counter() - start
    added = sum(counts.values())
    rate = added / elapsed if elapsed else 0.0
    print(f"Merge: {added}/{total_words} added in {elapsed:.3f}s ({rate:.1f} words/s)")


def run_export(storage: SQLiteVocabularyStorage, export_path: str) -> None:
    os.makedirs(os.path.dirname(export_path) or ".", exist_ok=True)
    tracemalloc.start()
    tracemalloc.reset_peak()
    start = time.perf_counter()
    export_storage_to_single_file(storage, export_path)
    elapsed = time.perf_counter() - start
    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    size_bytes = os.path.getsize(export_path)
    size_mb = size_bytes / (1024 * 1024)
    peak_mb = peak / (1024 * 1024)
    print(f"Export: {elapsed:.3f}s, peak tracemalloc {peak_mb:.1f} MiB, file {size_mb:.1f} MiB")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Profile SQLite merge throughput and ODS export memory usage."
    )
    parser.add_argument(
        "--per-category",
        type=int,
        default=10000,
        help="Number of words per lexical category.",
    )
    parser.add_argument(
        "--duplicate-rate",
        type=float,
        default=0.0,
        help="Fraction of duplicates to append per category.",
    )
    parser.add_argument("--db-path", type=str, default="")
    parser.add_argument("--export-path", type=str, default="")
    parser.add_argument("--skip-export", action="store_true")

    args = parser.parse_args()
    if args.per_category <= 0:
        raise SystemExit("--per-category must be > 0")
    if args.duplicate_rate < 0:
        raise SystemExit("--duplicate-rate must be >= 0")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    perf_dir = os.path.join(os.getcwd(), "perf_data")
    os.makedirs(perf_dir, exist_ok=True)
    db_path = args.db_path or os.path.join(perf_dir, f"perf_{timestamp}.db")
    export_path = args.export_path or os.path.join(perf_dir, f"export_{timestamp}.ods")

    print(f"DB: {db_path}")
    print(f"Words per category: {args.per_category}")
    print(f"Duplicate rate: {args.duplicate_rate:.2f}")

    dataset = build_dataset(args.per_category, args.duplicate_rate)
    storage = SQLiteVocabularyStorage(db_path)
    run_merge(storage, dataset)

    if not args.skip_export:
        run_export(storage, export_path)
        print(f"Export file: {export_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
