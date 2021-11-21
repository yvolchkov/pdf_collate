#!/usr/bin/env python3

"""Monitor for new files, and run ocrmypdf when they appeared.  Also
collates pdfs if they appeared close to each other (assuming they are
parts of the same 2-sided doccument)
"""
from datetime import datetime

import os
import time
import tempfile
import shutil
import subprocess
import logging
import pickle
from bisect import bisect_right, bisect_left
from inotify_simple import INotify, flags
import argparse

MON_DIR = '/input'
OUT_DIR = '/out'
PICKLE_FILE = os.path.join(OUT_DIR, '.meta')

logging.basicConfig(level=logging.INFO)

class NonFatalError(Exception):
    """Processing failed only for one file, we can continue"""

class Pdf:
    """Represents one input pdf file"""

    def __init__(self, name, prev):
        self.name = name
        mtime = os.path.getmtime(name)
        self.mtime = datetime.fromtimestamp(mtime)
        self.prev = prev
        self.out = None
        self.timestamp = self.mtime.strftime("%Y-%m-%dT%H_%M_%S")

    def get_new_out_dir_name(self):
        """Find a new home for results"""
        out = os.path.join(OUT_DIR, self.timestamp)
        if os.path.exists(out):
            for i in range(1, 11):
                test_name = "%s_%02d" % (out, i)
                if not os.path.exists(test_name):
                    out = test_name
                    break

        if os.path.exists(out):
            logging.error("Too many files with mtime %s", self.timestamp)
            raise NonFatalError()
        return out

    def get_out_name(self, lang, out_dir=None, collated=False):
        """Generate a name for result"""
        collated_str = ""
        if collated:
            collated_str = "_collated"

        if out_dir:
            out = out_dir
        elif self.out:
            out = self.out
        else:
            raise ValueError("No output directory")

        out_base_name = os.path.join(out, self.mtime.date().isoformat())

        return "%s_%s%s.pdf" % (out_base_name, lang, collated_str)

    def ocr(self, out_dir):
        """Generate a pdf with ocr layer"""
        for lang in ("eng", "deu"):
            ocr_cmd = [
                "ocrmypdf",
                "-q",
                self.name,
                "-l", lang,
                "--",
                self.get_out_name(lang, out_dir=out_dir)
            ]
            try:
                subprocess.check_call(ocr_cmd)
            except subprocess.CalledProcessError:
                logging.error("Failed to ocrmypdf: %s", " ".join(ocr_cmd))
                raise NonFatalError

        return True

    def collate(self):
        """Collate this pdf with the previous one"""
        for lang in ("eng", "deu"):
            collate_cmd = [
                "qpdf", "--collate", "--empty",
                "--pages",
                self.prev.get_out_name(lang),
                self.get_out_name(lang),
                "z-1", "--",
                self.prev.get_out_name(lang, collated=True)
            ]

            try:
                subprocess.check_call(collate_cmd)
            except subprocess.CalledProcessError:
                logging.error("Failed to collate: %s", " ".join(collate_cmd))
                raise NonFatalError

    def process(self):
        """Process one input file"""
        temp_dir = tempfile.mkdtemp()
        try:
            self.ocr(temp_dir)
            self.out = self.get_new_out_dir_name()
            shutil.move(temp_dir, self.out)
        except:
            shutil.rmtree(temp_dir)
            raise

        if self.prev is None:
            return

        if (self.mtime - self.prev.mtime).seconds > 60:
            return
        self.collate()

def process_one_file(fname, prev):
    """Process one input file"""
    logging.info("processing %s", fname)
    name = os.path.join(MON_DIR, fname)

    for i in range(10):
        if not os.path.exists(name):
            raise NonFatalError()
        
        if os.path.getsize(name) != 0:
            break
        logging.warning("File %s has zero length. It probably has not been written yet. Retry after 5 senconds", fname)
        time.sleep(5)

    pdf = Pdf(name, prev)
    pdf.process()
    if prev:
        prev.prev = None
    save_latest(pdf)
    logging.info("file %s is done", fname)
    return pdf

def save_latest(pdf):
    """Save the state of the tool. If crashed or updated we will start
    from where we left it"""
    with open(PICKLE_FILE, 'wb') as cache_f:
        pickle.dump(pdf, cache_f)

def load_latest():
    """Load the state from the previous run"""
    ret = None
    if not os.path.exists(PICKLE_FILE):
        return None

    with open(PICKLE_FILE, 'rb') as cache_f:
        ret = pickle.load(cache_f)

    return ret

def process_offline_files(latest_pdf):
    """Process files which were created while the tool was not running"""
    ret = latest_pdf

    files = []
    for filename in os.listdir(MON_DIR):
        cur_mtime = os.path.getmtime(os.path.join(MON_DIR, filename))
        cur_mtime = datetime.fromtimestamp(cur_mtime)
        files.append((cur_mtime, filename))
    files = sorted(files, key=lambda f: f[0])

    # Keep only files older then last_pdf
    keys = [x[0] for x in files]

    if latest_pdf and latest_pdf.mtime >= files[-1][0]:
        return latest_pdf

    if latest_pdf:
        i = 0
        if len(keys) >= 2:
            i = bisect_right(keys, latest_pdf.mtime)
        if files[i][1] == os.path.basename(latest_pdf.name):
            i += 1
        files = files[i:]

    for cur_file in files:
        name = cur_file[1]
        try:
            ret = process_one_file(name, ret)
        except (NonFatalError, FileNotFoundError):
            logging.error("Failed processing %s", name)
            continue
    return ret

def inotify_loop():
    inotify = INotify()
    watch_flags = flags.CREATE
    inotify.add_watch(MON_DIR, watch_flags)

    logging.info("entering inotify loop")
    while True:
        for event in inotify.read():
            try:
                prev_pdf = process_one_file(event.name, prev_pdf)
            except (NonFatalError, FileNotFoundError):
                logging.error("Failed processing %s", event.name)
                continue

def poll_loop(interval, latest_pdf):
    logging.info("entering polling loop")
    while True:
        time.sleep(interval)
        try:
            latest_pdf = process_offline_files(latest_pdf)
        except (NonFatalError, FileNotFoundError):
            logging.error("Failed processing %s", event.name)
            continue

def main():
    """Where all the things start"""

    parser = argparse.ArgumentParser()
    parser.add_argument("--inotify", default=True,
                        help="Register inotify event instead of polling filesystem")
    parser.add_argument("--poll-interval", default=0, type=int,
                        help="How often to look for new files")
    args = parser.parse_args()

    print(args.inotify)

    prev_pdf = load_latest()
    if prev_pdf:
        logging.info("restored state. Latest processed file is %s", prev_pdf.name)
    prev_pdf = process_offline_files(prev_pdf)

    if not args.inotify or args.poll_interval > 0:
        poll_loop(args.poll_interval, prev_pdf)
    else:
        inotify_loop()


if __name__ == "__main__":
    main()
