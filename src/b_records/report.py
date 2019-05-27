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
import random
import recommender_common.load_compass_data as ld
from colored import fg, bg, attr

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

        rep = "* " + attr('bold') + row['pid'].ljust(30) + creator.ljust(40) + title + attr('reset')
        return rep


def make_report(predicted_tag_file, archive_file, recommendations_file=None, limit=None, maxn=None, min_value=None, shuffle=False):
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

    pids = list(predicted.keys())
    if shuffle:
        random.shuffle(pids)
    recommendations = {}
    if recommendations_file:
        recommendations = joblib.load(recommendations_file)
    with _Cursor(os.environ['LOWELL_URL']) as cur:
        for i, pid in enumerate(pids):
            tags = predicted[pid]

            print(get_metadata(cur, pid) + '\n')
            if tags:
                if maxn:
                    tags = tags[:10]
                for tag, value in tags:
                    if not min_value or min_value < value:
                        if id2tag[int(tag)].startswith('stemning'):
                            print("  ", fg('yellow') + tag.ljust(5), f'{value:4.2f}', id2tag[int(tag)], attr('reset'))
                        else:
                            print("  ", tag.ljust(5), f'{value:4.2f}', id2tag[int(tag)])

            if pid in recommendations:
                print("\n    " + attr("underlined") + " Recommendations\n" + attr('reset'))
                for rec in recommendations[pid]:
                    print("   ", rec['pid'].ljust(30), rec['creator'].ljust(40), rec['title'].ljust(40), rec['loancount'])

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
    parser.add_argument('-r', '--recommendation-file',
                        help='file containing recommendations', default=None)
    parser.add_argument('-l', '--limit', type=int,
                        help='limits the number of geneated items', default=None)
    parser.add_argument('-m', '--min-value', type=int,
                        help='minimum-value for displayed items', default=None)
    parser.add_argument('-n', '--maxn', type=int,
                        help='maximum displayed items for each pid', default=None)
    parser.add_argument('-s', '--shuffle', dest='shuffle', action='store_true',
                        help='shuffle')
    parser.add_argument('-v', '--verbose', dest='verbose', action='store_true',
                        help='verbose output')
    args = parser.parse_args()

    level = logging.INFO
    if args.verbose:
        level = logging.DEBUG
    logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=level)

    make_report(args.predicted_tags_file, args.archive_file, args.recommendation_file, args.limit, args.maxn, args.min_value, args.shuffle)
