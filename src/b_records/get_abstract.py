#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# -*- mode: python -*-
"""
:mod:`b_records.get_abstracts` -- Get abstracts

=============
Get Abstracts
=============

Harvest abstracts from LOWELL and writes them to file.

"""
import joblib
import json
import logging
import os
from psycopg2 import connect
import psycopg2.extras

import recommender_common.load_compass_data as ld

logger = logging.getLogger(__name__)


class _Cursor():
    """ postgres cursor """
    def __init__(self, postgres_url):
        self.postgres_url = postgres_url

    def __enter__(self):

        self.conn = connect(self.postgres_url)
        self.cur = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        return self.cur

    def __exit__(self, type, value, traceback):

        self.conn.commit()
        self.cur.close()
        self.conn.close()


def _type(item):
    if item.startswith('Bog') or item.startswith('Lydbog') or item.startswith('Ebog'):
        return True
    return False


def _dk5(item):
    if item == 'sk' or item == '99.4' or item.startswith('99.4 '):
        return True
    return False


def _get_abstracts(compass_pids, limit=None, min_length=1):
    logger.info(f"Fetching abstracts from LOWELL limit={limit}, min_length={min_length}")
    stmt = """SELECT pid, metadata->>'dk5' AS dk5, metadata->>'type' AS type, metadata->>'abstract' AS abstract
              FROM metadata
              WHERE metadata->>'abstract' IS NOT NULL
                AND metadata->'collection' ?| ARRAY['870970-basis']
                AND metadata->'audience' ?| ARRAY['voksenmaterialer']
                AND metadata->'language' ?| ARRAY['dan']"""
    if limit:
        stmt += f" limit {limit}"
    with _Cursor(os.environ['LOWELL_URL']) as cur:
        cur.execute(stmt)
        for row in cur:
            if 'abstract' in row and row['pid'] in compass_pids:
                yield(row['pid'], json.loads(row['abstract'])[0])
            elif _type and _dk5 and len(row['abstract']) >= min_length:
                yield(row['pid'], json.loads(row['abstract'])[0])


def get_abstracts(archive_file, outfile_prefix='abstracts', limit=None, min_length=100):
    """
    :param archive file:
        Content-first archive file
    :param output_prefix:
        If given, the generated tags are written to file
    :param limit:
        Limits the number of harvested abstracts
    :param min_length:
        Minimum length of harvested abstracts
    """
    tag_archive_content = ld.load_tag_data(archive_file)
    compass_pids = {k for k, v in ld.pid2tags(tag_archive_content)}
    abstracts = {p: a for p, a in _get_abstracts(compass_pids, limit=limit, min_length=min_length)}
    if outfile_prefix:
        name = f"{outfile_prefix}-{min_length}-{len(abstracts)}.pkl"
        logger.info(f"Writing data to {name}")
        joblib.dump(abstracts, name)
    return abstracts


def cli():
    """ Commandline interface """
    import argparse

    parser = argparse.ArgumentParser(description='retreives all abstracts and writes thjem to file')
    parser.add_argument('archive',
                        help='compass archive to fetch compass pids from')
    parser.add_argument('-o', '--outfile-prefix',
                        help='file to write result to. Default is abstracts', default='abstracts')
    parser.add_argument('-m', '--min-length', type=int,
                        help='minimun number of chars in abstract', default=100)
    parser.add_argument('-l', '--limit', dest='limit', type=int,
                        help='limit number of harvested items)', default=None)
    parser.add_argument('-v', '--verbose', dest='verbose', action='store_true',
                        help='verbose output')
    args = parser.parse_args()

    level = logging.INFO
    if args.verbose:
        level = logging.DEBUG
    logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=level)

    get_abstracts(args.archive, args.outfile_prefix, args.limit, args.min_length)
