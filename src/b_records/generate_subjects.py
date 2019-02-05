#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# -*- mode: python -*-
"""
:mod:`b_records.generate_subjects` -- Generate Subjects

=================
Generate Subjects
=================

Generates subjects for each pid in the doc2vec model that is not a
content-first pid.


For each pid the closest n other pids a found (cosimilarity in the
doc2vec model), and their labels are grouped and counted. The most
frequent tags are then assigned to the pid.

The doc2vec model is build from the abstracts of each pid.
There are several parameters to tweak to get the result you want.
"""
import logging
from tqdm import tqdm
from collections import defaultdict
import joblib
import recommender_common.load_compass_data as ld
logger = logging.getLogger(__name__)


def generate_tags(model_file, archive_file, output_prefix=None, topn=None, min_similarity=0.45, by_value=False, min_value=None):
    """
    Generates tags for pids based on likeness to content-first pids
    :param model_file:
        File containing doc2vec file
    :param archive file:
        Content-first archive file
    :param output_prefix:
        If given, the generated tags are written to file
    :param topn:
        Retain max topn tags for each pid
    :param min_similarity:
        Minimum similarity between vecots for consideration
    :param by_value:
        if False - tag value is calculated by occurence
        if True - tag value is calculated by accumulating similarity scores
    :param min_value:
        <inimum value for tags
    """
    logger.info("Loading data")
    model = _load_model(model_file)
    pid2tags = _load_pid2tag(archive_file)
    labels = [model.docvecs.offset2doctag[i] for i in range(len(model.docvecs))]

    result = {label: tags for label, tags in _generate_tags(model, pid2tags, labels, topn, min_similarity, by_value, min_value)}
    if output_prefix:
        name = f"{output_prefix}-{len(result)}.pkl"
        logger.info("Writing result to file %s", name)
        joblib.dump(result, name)
    return result


def _generate_tags(model, pid2tags, labels, topn, min_similarity, by_value, min_value):
    for i, label in tqdm(enumerate(labels)):
        if label not in pid2tags:
            sims = model.docvecs.most_similar(positive=[model.docvecs[i]], topn=len(model.docvecs))
            sims = [s for s in sims if s[0] in pid2tags and s[1] > min_similarity]
            logger.debug("found %d similar items for %s", len(sims), labels)
            tags = _get_subjects(sims, pid2tags, topn, by_value)
            if min_value:
                tags = [(t, v) for t, v in tags if v >= min_value]
            logger.debug("Identified following tags for %s: %s", label, tags)
            if tags:
                yield label, tags


def _load_model(path):
    logger.debug("Loading model from %s", path)
    return joblib.load(path)


def _load_pid2tag(archive_file):
    logger.debug("Loading ta archive from %s", archive_file)
    tag_archive_content = ld.load_tag_data(archive_file)
    return {k: {p[0] for p in v} for k, v in ld.pid2tags(tag_archive_content)}


def _get_subjects(sims, pid2tags, topn, by_value):
    tag_value = defaultdict(lambda: 0)
    for pid, value in sims:
        for tag in pid2tags[pid]:
            if by_value:
                tag_value[tag] += value
            else:
                tag_value[tag] += 1
    tags = sorted(list(tag_value.items()), key=lambda x: x[1], reverse=True)
    if topn:
        tags = tags[:topn]
    return tags


def cli():
    """ Commandline interface """
    import argparse

    parser = argparse.ArgumentParser(description='Generate tags based on abstract')
    parser.add_argument('model_file',
                        help='file containing abstract_model')
    parser.add_argument('archive_file',
                        help='archive file')
    parser.add_argument('-o', '--outfile-prefix',
                        help='file to write result to. Default is predicted-tags', default='predicted-tags')
    parser.add_argument('--topn', type=int,
                        help='limits the number of predicted tags for each item', default=None)
    parser.add_argument('--min-similarity', type=float,
                        help='minimum similarity to be considered. default is 0.45', default=0.45)
    parser.add_argument('--by-value', action='store_true',
                        help='calculates tag value by similarity rather than occurence')
    parser.add_argument('--min-value', type=float,
                        help='minimum value of a tag to be returned (This will be at a different scale if you have chosen by-value)')
    parser.add_argument('-v', '--verbose', dest='verbose', action='store_true',
                        help='verbose output')
    args = parser.parse_args()

    level = logging.INFO
    if args.verbose:
        level = logging.DEBUG
    logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=level)

    generate_tags(args.model_file, args.archive_file, args.outfile_prefix, args.topn, args.min_similarity, args.by_value, args.min_value)
