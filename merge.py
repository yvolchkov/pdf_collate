#!/usr/bin/env python3

from datetime import datetime
import subprocess
import logging
import os
import shutil
import argparse
import time

INPUT_DIR = '/users/yurivolchkov/src/tmp/scanned'
OUT_DIR = '/Users/yurivolchkov/src/tmp/output'
ARCHIVE_DIR = '/Users/yurivolchkov/src/tmp/archive'

logging.basicConfig(level=logging.INFO)

class NonFatalError(Exception):
    """Processing failed only for one file, we can continue"""

def collate(front, back, output):
    """Collate this pdf with the previous one"""
    logging.info("merging %s with %s" % (front, back))
    collate_cmd = [
        "qpdf", "--collate", "--empty",
        "--pages",
        front,
        back,
        "z-1", "--",
        output,
    ]

    try:
        subprocess.check_call(collate_cmd)
    except subprocess.CalledProcessError:
        logging.error("Failed to collate: %s", " ".join(collate_cmd))
        raise NonFatalError

def process():
    files = []
    for filename in os.listdir(INPUT_DIR):
        cur_mtime = os.path.getmtime(os.path.join(INPUT_DIR, filename))
        cur_mtime = datetime.fromtimestamp(cur_mtime)
        files.append((cur_mtime, filename))
    files = sorted(files, key=lambda f: f[0])

    front = None
    for cur_file in files:
        name = os.path.join(INPUT_DIR, cur_file[1])
        if front:
            try:
                output = os.path.basename(front)
                output = os.path.join(OUT_DIR, output)
                collate(front, name, output)
                shutil.move(front, ARCHIVE_DIR)
                shutil.move(name, ARCHIVE_DIR)
                front = None
            except (NonFatalError, FileNotFoundError):
                logging.error("Failed processing %s", name)
                continue
        front = name

def poll_loop(interval):
    logging.info("entering polling loop")
    while True:
        time.sleep(interval)
        try:
            process()
        except (NonFatalError, FileNotFoundError):
            logging.error("Failed processing %s", event.name)
            continue


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--poll-interval", default=0, type=int,
                        help="How often to look for new files")
    args = parser.parse_args()

    poll_loop(args.poll_interval)

if __name__ == "__main__":
    main()
