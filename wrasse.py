#!/usr/bin/env python2.7
"""Wrasse

Usage:
    wrasse (pull|push) [options]
    wrasse package (add|remove) [options] <file>

Options:
    -h --help       Show this help
    -v --verbose    Be more verbose
    --debug         Be way more verbose
"""

import os
from functools import partial
from os.path import join
import logging
from hashlib import md5

from docopt import docopt

from boto.s3.connection import S3Connection, Key

logger = logging.getLogger("wrasse")
logger.setLevel(logging.WARNING)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
# create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
# add the handlers to the logger
logger.addHandler(ch)


def entry_console():
    if args['--debug']:
        logger.setLevel(logging.DEBUG)
    elif args['--verbose']:
        logger.setLevel(logging.INFO)
    if args['push'] or args['pull']:
        traverse()


def examine_file(candidate, directory=None):
    path = join(directory, candidate)
    logger.debug("Examining file: %s", path)

    with open(path) as in_file:
        local_hash = md5(in_file.read()).hexdigest()

    key = bucket.get_key(path)

    # The etag string contains quotes...
    etag = key.etag[1:-1] if key else ""
    if etag != local_hash:
        if args['push']:
            upload_file(path)
        else:
            logger.info("Local differs from remote: %s", path)


def upload_file(path):
    logger.info("Pushing local to remote: %s", path)
    key = Key(bucket, path)
    key.set_contents_from_filename(path, reduced_redundancy=True)


def traverse():
    os.chdir('repo')
    for root, dirs, files in os.walk('.'):
        root = root[2:]
        dir_wrapper = partial(examine_file, directory=root)
        map(dir_wrapper, files)
    for key in bucket.list():
        logger.debug(key)


if __name__ == "__main__":
    args = docopt(__doc__)
    conn = S3Connection()
    bucket = conn.get_bucket("com.fishsilo.skipjack")
    entry_console()
