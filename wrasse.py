#!/usr/bin/env python2.7
"""Wrasse

Usage:
    wrasse (pull|push) [options] <bucket>
    wrasse package (add|remove) [options] <distro> <file>

Options:
    -h --help         Show this help
    -v --verbose      Be more verbose
    --debug           Be way more verbose
"""

import os
from functools import partial
from os.path import join, exists
import logging
from hashlib import md5
import shutil
from sh import vagrant

from docopt import docopt

from boto.s3.connection import S3Connection, Key

UPLOAD_DIR = "uploading"
REPO_DIR = "repo"

args = None

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
    global args, bucket
    args = docopt(__doc__)

    if args['--debug']:
        logger.setLevel(logging.DEBUG)
    elif args['--verbose']:
        logger.setLevel(logging.INFO)

    if args['push'] or args['pull']:
        conn = S3Connection()
        bucket = conn.get_bucket(args['<bucket>'])
        traverse()
    elif args['package']:
        package()


def package():
    package_file = args['<file>']
    distro = args['<distro>']
    if args["add"]:
        assert exists(REPO_DIR)
        if not exists(UPLOAD_DIR):
            os.mkdir(UPLOAD_DIR)
        shutil.copy(package_file, UPLOAD_DIR)
        vagrant.ssh(
                c="reprepro -b /vagrant/{0} " +
                "includedeb {1} /vagrant/{2}/{3}".format(REPO_DIR,
                                                         distro,
                                                         UPLOAD_DIR,
                                                         package_file))
        os.remove(join(UPLOAD_DIR, package_file))


def examine_remote(path, key=None):
    logger.debug("Examining file: %s", path)

    if not exists(path):
        logger.info("Remote not present on local: %s", path)
        download_file(path, key)
        return

    with open(path) as in_file:
        local_hash = md5(in_file.read()).hexdigest()

    key = bucket.get_key(path)

    # The etag string contains quotes...
    etag = key.etag[1:-1] if key else ""
    if etag != local_hash:
        logger.info("Local differs from remote: %s", path)
        upload_file(path)


def download_file(path, key):
    if not args['pull']:
        return
    if not key:
        logger.error("Download method expects key")
        raise ValueError("Need to provide key")

    logger.info("Pulling remote to local: %s", path)
    key.get_contents_to_filename(path)


def upload_file(path):
    if not args['push']:
        return
    logger.info("Pushing local to remote: %s", path)
    key = Key(bucket, path)
    key.set_contents_from_filename(path, reduced_redundancy=True)


def traverse():
    os.chdir(REPO_DIR)
    memory = set()
    for root, dirs, files in os.walk('.'):
        root = root[2:]
        paths = map(lambda x: join(root, x), files)
        memory = memory.union(set(paths))
        map(examine_remote, paths)
    for key in bucket.list():
        if key.name in memory:
            logger.debug("Skipping pull because already checked: %s", key.name)
            continue
        examine_remote(key.name, key=key)


if __name__ == "__main__":
    entry_console()
