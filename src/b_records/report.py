#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# -*- mode: python -*-
"""
:mod:`b_records.report` -- report

======
Report
======

Generate human readable report from generated tags file
"""
import logging
import json
import joblib
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


def load_id2tag_map(archive_file):
    id2tag = ld.load_id_expanded_tag_map(archive_file)
    return id2tag


def load_predicted_tags(predicted_tag_file):
    return joblib.load(predicted_tag_file)


def get_metadata(cur, pid):
    cur.execute("""SELECT pid, metadata->>'creator' as creator, metadata->>'title' as title
                   FROM metadata
                   WHERE pid=%(pid)s""", {'pid': pid})
    for row in cur:
        creator = row.get('creator')
        if not creator:
            creator = '[""]'
        title = row.get('title')
        if not title:
            title = '[""]'
        creator = json.loads(creator)[0]
        title = json.loads(title)[0]

        rep = "* " + row['pid'].ljust(30) + creator.ljust(40) + title
        return rep


def make_report(predicted_tag_file, archive_file, limit=None, maxn=None, min_value=None):
    """
    Creates human-readable report
    :param predicted_tag_file:
        File with generated tags
    :param archive_file:
        content-first archive file
    :param limit:
        limits number of generated items
    """
    id2tag = load_id2tag_map(archive_file)
    predicted = load_predicted_tags(predicted_tag_file)
    with _Cursor(os.environ['LOWELL_URL']) as cur:
        for i, (pid, tags) in enumerate(predicted.items()):
            print(get_metadata(cur, pid) + '\n')
            if tags:
                if maxn:
                    tags = tags[:10]
                for tag, value in tags:
                    if not min_value or min_value < value:
                        print("  ", tag.ljust(5), f'{value:4.2f}', id2tag[int(tag)])

            if limit and i >= limit:
                break

            print('\n\n')


def cli():
    """ Commandline interface """
    import argparse

    parser = argparse.ArgumentParser(description='Creates human-readable report')
    parser.add_argument('predicted_tags_file',
                        help='file containing predicted tags')
    parser.add_argument('archive_file',
                        help='archive file')
    parser.add_argument('-l', '--limit', type=int,
                        help='limits the number of geneated items', default=None)
    parser.add_argument('-m', '--min-value', type=int,
                        help='minimum-value for displayed items', default=None)
    parser.add_argument('-n', '--maxn', type=int,
                        help='maximum displayed items for each pid', default=None)
    parser.add_argument('-v', '--verbose', dest='verbose', action='store_true',
                        help='verbose output')
    args = parser.parse_args()

    level = logging.INFO
    if args.verbose:
        level = logging.DEBUG
    logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=level)

    make_report(args.predicted_tags_file, args.archive_file, args.limit, args.maxn, args.min_value)
