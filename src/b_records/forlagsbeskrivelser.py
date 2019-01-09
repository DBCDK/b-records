#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# -*- mode: python -*-
from datetime import datetime
import json
import logging
import os
from forlagsbeskrivelse_pids import get_pids_from_solr
from corepo_harvester.fetch_commondata import fetch_commondata_content
from lxml import etree

logger = logging.getLogger(__name__)


""" Namespacemap for commonData xml"""
NSMAP = {'ting': 'http://www.dbc.dk/ting',
         'dkabm': 'http://biblstandard.dk/abm/namespace/dkabm/',
         'ac': 'http://biblstandard.dk/ac/namespace/',
         'dkdcplus': 'http://biblstandard.dk/abm/namespace/dkdcplus/',
         'oss': 'http://oss.dbc.dk/ns/osstypes',
         'dc': 'http://purl.org/dc/elements/1.1/',
         'dcterms': 'http://purl.org/dc/terms/',
         'xsi': 'http://www.w3.org/2001/XMLSchema-instance',
         'docbook': 'http://docbook.org/ns/docbook',
         'marcx': 'info:lc/xmlns/marcxchange-v1'}


def normalize(s):
    """
    normalize string s:
    (1) replace double-a
    (2) strip whitespace around field
    """
    return s.strip().replace('\uA732', 'Aa').replace('\uA733', 'aa')


def forlagsbeskrivelse(row):
    """ helper function to make parse_commondatas signature conform"""
    return _get_forlagsbeskrivelse(row['content'], pid=row['pid'])


def _get_forlagsbeskrivelse(xml_string, pid=None):
    xml = etree.fromstring(xml_string)

    path = '/ting:container/docbook:article/docbook:section/docbook:para/text()'

    value = xml.xpath(path, namespaces=NSMAP)
    value = value[0] if value else ''
    path = '/ting:container/ting:originalData/DbcWrapper/wroot/wfaust/text()'
    pid = '870970-basis:' + xml.xpath(path, namespaces=NSMAP)[0]

    return (pid, normalize(value))


def get_pids(pidfile, solr_url, limit):
    pids = None
    if not pidfile:
        logger.info("Harvseting pids from solr")
        return get_pids_from_solr(solr_url, limit)
    else:
        with open(pidfile) as fh:
            pids = [line.strip() for line in fh.read()]
            if limit:
                return pids[:limit]
            return pids


def main(pidfile=None, solr_url=None, limit=None, outfile='forlagsbeskrivelser.json'):

    start = datetime.now()
    corepo_url = os.environ['COREPO_URL']

    pids = get_pids(pidfile, solr_url, limit)
    result = fetch_commondata_content(corepo_url, pids, parser_function=forlagsbeskrivelse)
    content = {pid: text for pid, text in result}

    if outfile:
        logger.info("Saving result to %s", outfile)
        with open(outfile, 'w') as fh:
            fh.write(json.dumps(content))
    logger.info("Got all forlagsbekrivelser in [%s]", datetime.now() - start)


def cli():
    """ Commandline interface """
    import argparse

    solr_url = 'http://cisterne-solr.dbc.dk:8984/solr/corepo_20180330_1818_stored/'

    parser = argparse.ArgumentParser(description='høst forlagsbeskrivelser')
    parser.add_argument('-p', '--pidfile',
                        help='File with pids forslagsbeskrivelse pids. If not provided pids are harvested from solr')
    parser.add_argument('-o', '--outfile',
                        help='file to write result to. Default is forlagsbeskrivelser.json', default='forlagsbeskrivelser.json')
    parser.add_argument('-l', '--limit', dest='limit', type=int,
                        help='limit number of harvested items)', default=None)
    parser.add_argument('-v', '--verbose', dest='verbose', action='store_true',
                        help='verbose output')
    args = parser.parse_args()

    logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.DEBUG)

    main(args.pidfile, solr_url, args.limit, args.outfile)


if __name__ == '__main__':
    cli()
