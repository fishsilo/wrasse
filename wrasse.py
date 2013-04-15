#!/usr/bin/env python2.7
"""Wrasse

Usage:
    wrasse
"""

import os
from functools import partial
from os.path import join

import boto
from docopt import docopt

from boto.s3.connection import S3Connection, Location, Key


def entry_console():
    traverse()


def examine_file(candidate, directory=None):
    path = join(directory, candidate)
    print path

    from hashlib import md5
    with open(path) as in_file:
        local_hash = md5(in_file.read()).hexdigest()

    key = bucket.get_key(path)

    # The etag string contains quotes...
    etag = key.etag[1:-1] if key else ""
    if etag != local_hash:
        upload_file(path)


def upload_file(path):
    print "Uploading {0}".format(path)
    key = Key(bucket, path)
    key.set_contents_from_filename(path, reduced_redundancy=True)


def traverse():
    os.chdir('repo')
    for root, dirs, files in os.walk('.'):
        root = root[2:]
        dir_wrapper = partial(examine_file, directory=root)
        map(dir_wrapper, files)


if __name__ == "__main__":
    args = docopt(__doc__)
    conn = S3Connection()
    bucket = conn.get_bucket("com.fishsilo.skipjack")
    entry_console()
