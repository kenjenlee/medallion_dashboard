import argparse
import sys
from src.pipeline_prefect import pipeline

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--local-csv", action="store_true", dest="local_csv", help="Use local csv instead of writing to broze and sikver tables.")
    args = parser.parse_args()
    pipeline(args.local_csv)