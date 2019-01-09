#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# -*- mode: python -*-
"""
Høster pidder med forlagsbeskrivelser
"""
from datetime import datetime
import logging
import requests
from tqdm import tqdm
logger = logging.getLogger(__name__)
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)


def fetch_pids(solr_url, limit=None, rows=10):
    fetched = 0
    end = limit
    if not end:
        end = rows + 1

    params = {'q': 'rec.collectionIdentifier:150015-f*',
              'start': 0,
              'rows': rows,
              'fl': 'rec.repositoryId'}

    while params['start'] < end:
        r = requests.get(solr_url.rstrip('/') + '/query', params=params)
        if not r.ok:
            r.raise_for_status()
        if not limit:
            end = limit = r.json()['response']['numFound']

        params['start'] += params['rows']

        docs = r.json()['response']['docs']
        for doc in docs:
            if fetched >= end:
                break
            yield doc['rec.repositoryId']
            fetched += 1


def get_pids_from_solr(solr_url, limit=None, outfile=None):
    start = datetime.now()
    logger.info("Harvesting pids from '%s'", solr_url)
    pids = [pid for pid in tqdm(fetch_pids(solr_url, limit=limit))]
    if outfile:
        with open(outfile, 'w') as fh:
            for pid in pids:
                fh.write(pid+'\n')
    logger.info("harvested forlagsbeskrivelsepids in [%s]", datetime.now() - start)
    return pids


def cli():
    """ Commandline interface """
    import argparse

    solr_url = 'http://cisterne-solr.dbc.dk:8984/solr/corepo_20180330_1818_stored/'

    parser = argparse.ArgumentParser(description='høst af pid\'er med forlagsbeskrivelser')
    parser.add_argument('-l', '--limit', dest='limit', type=int,
                        help='limits number of harvested reviews)', default=None)
    parser.add_argument('-o', '--outfile', dest='outfile',
                        help="file to dump pids into. default is 'forlagsbeskrivelse_pids.txt'",
                        default='forlagsbeskrivelse_pids.txt')
    parser.add_argument('-v', '--verbose', dest='verbose', action='store_true',
                        help='verbose output')
    args = parser.parse_args()

    logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.DEBUG)
    get_pids_from_solr(solr_url, limit=args.limit, outfile=args.outfile)


if __name__ == '__main__':
    cli()
