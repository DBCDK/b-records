#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# -*- mode: python -*-
"""
:mod:`b_records.build_doc2vec_model` -- build doc2vec model

====================
Builds Doc2vec Model
====================

Builds doc2vec model based on harvested abstracts
"""
from datetime import datetime
import logging
from gensim.models.doc2vec import TaggedDocument
from gensim.models.doc2vec import Doc2Vec
import gensim.parsing.preprocessing as pre
import joblib
import json
logger = logging.getLogger(__name__)


FILTERS = [pre.strip_tags, pre.strip_punctuation, pre.strip_multiple_whitespaces, pre.strip_numeric, pre.strip_short]


class Docs():
    """ Document iterator """
    def __init__(self, data):
        self.data = data
        self.pids = list(self.data.keys())
        self.i = 0

    def __iter__(self):
        return self

    def __next__(self):
        self.i += 1
        if self.i > len(self.data):
            self.i = 0
            raise StopIteration
        pid = self.pids[self.i-1]
        text = self.data[pid]
        tokens = pre.preprocess_string(text, filters=FILTERS)
        return TaggedDocument(tokens, [pid])


def train(abstracts_file, emb_size=300, min_count=2, epochs=200, limit=None, outfile_prefix='abstract-model'):
    """
    Trains doc2vec model

    :param abstracts_file:
        Path to file containing abstracts
    :param emb_size:
        Size of trained embedding
    :param min_count:
        Minimum number of wordoccurence
    :param epochs:
        Number of epochs to train
    :param limit:
        Limits the number of abstracts to use
    :param outfile-prefix:
        filename prefix
    """
    start = datetime.now()
    num, data = _load_data(abstracts_file, limit)
    model = Doc2Vec(vector_size=emb_size, dm=1, min_count=min_count, workers=12)
    model.build_vocab(Docs(data))
    model.train(Docs(data), total_examples=model.corpus_count, epochs=epochs)
    if outfile_prefix:
        outfile = f"{outfile_prefix}-{emb_size}-{num}.d2v"
        logger.info("Writing model to %s", outfile)
        joblib.dump(model, outfile)
    logger.info("Created model in [%s]", datetime.now() - start)
    return model


def _load_data(abstracts_file, limit):
    data = joblib.load(abstracts_file)
    num = len(data)
    if limit:
        num = min(num, limit)
        data = dict([(k, v) for k, v in data.items()][:num])
    return num, data


def cli():
    """ Commandline interface """
    import argparse

    parser = argparse.ArgumentParser(description='Create document vector model')
    parser.add_argument('abstracts',
                        help='file containing harvested abstracts')
    parser.add_argument('-o', '--outfile-prefix',
                        help='file to write result to. Default is abstract-model', default='abstract-model')
    parser.add_argument('-l', '--limit', dest='limit', type=int,
                        help='limit number of harvested items', default=None)
    parser.add_argument('-z', '--embedding-size', dest='z', type=int,
                        help='Embedding size. Default is 300', default=300)
    parser.add_argument('-e', '--epochs', dest='epochs', type=int,
                        help='number of epochs. Default is 200', default=200)
    parser.add_argument('-v', '--verbose', dest='verbose', action='store_true',
                        help='verbose output')
    args = parser.parse_args()

    level = logging.INFO
    if args.verbose:
        level = logging.DEBUG
    logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=level)

    train(args.abstracts, emb_size=args.z, epochs=args.epochs, limit=args.limit, outfile_prefix=args.outfile_prefix)
