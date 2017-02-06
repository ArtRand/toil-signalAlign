#!/usr/bin/env python2.7
from __future__ import print_function

import sys
import argparse
import os
import textwrap
import yaml
import cPickle
import uuid

from urlparse import urlparse
from functools import partial

from bd2k.util.humanize import human2bytes
from toil.common import Toil
from toil.job import Job

from toil_lib.files import generate_file
from toil_lib import require, UserError

from margin.toil.alignment import shardAlignmentByRegionJobFunction
from margin.toil.localFileManager import LocalFile, urlDownlodJobFunction, urlDownload
from signalalign.toil.ledger import makeReadstoreJobFunction
from signalalign.toil.signalAlignment import signalAlignJobFunction

from minionSample import ReadstoreSample, SignalAlignSample


def signalAlignCheckInputJobFunction(job, config, sample):
    require(config["ref"], "[signalAlignCheckInputJobFunction]Missing reference URL")
    require(config["ledger_url"], "[signalAlignCheckInputJobFunction]Missing ledger URL")
    require(config["HMM_file"], "[signalAlignCheckInputJobFunction]Missing HMM file URL")
    require(config["HDP_file"], "[signalAlignCheckInputJobFunction]Missing HDP file URL")
    job.addFollowOnJobFn(signalAlignRootJobFunction, config, sample)


def signalAlignRootJobFunction(job, config, sample):
    # download the reference
    config["reference_FileStoreID"] = job.addChildJobFn(urlDownlodJobFunction,
                                                        config["ref"],
                                                        disk=config["ref_size"]).rv()

    # download the BAM, and shard by region
    alignment_fid = job.addChildJobFn(urlDownlodJobFunction, sample.URL, disk=sample.size).rv()

    # download the models
    config["HMM_fid"] = job.addChildJobFn(urlDownlodJobFunction, config["HMM_file"], disk="10M").rv()
    config["HDP_fid"] = job.addChildJobFn(urlDownlodJobFunction, config["HDP_file"], disk="250M").rv()

    # setup labels
    config["sample_label"] = sample.sample_label

    # download and load the ledger
    # TODO use new function here
    ledger = LocalFile(workdir=job.fileStore.getLocalTempDir(), filename="%s.tmp" % uuid.uuid4().hex)
    urlDownload(job, config["ledger_url"], ledger)
    config["ledger"] = cPickle.load(open(ledger.fullpathGetter(), "r"))
    job.addFollowOnJobFn(shardAlignmentJobNode, config, alignment_fid)


def shardAlignmentJobNode(job, config, alignment_fid):
    # shard the alignment into smaller pieces, that we'll variant call independently
    alignment_shards = job.addChildJobFn(shardAlignmentByRegionJobFunction,
                                         config["reference_FileStoreID"],
                                         alignment_fid,
                                         config["split_chromosome_this_length"]).rv()
    job.addFollowOnJobFn(signalAlignJobFunction, config, alignment_shards)


def print_help():
    """this is the help for toil-signalAlign!
    """
    return print_help.__doc__


def generateManifest(command):
    run_manifest = textwrap.dedent("""
        #   Edit this manifest to include information for each sample to be run.
        #   Lines should contain three tab-seperated fields:
        #       Alignment URL
        #       Sample_label
        #       size
        #   Place your samples below, one sample per line.
        """[1:])

    readstore_manifest = textwrap.dedent("""
        #   Edit this manifest to include information for each sample to be run.
        #   N.B. See README for description of 'ledger'
        #   Lines should contain three tab-seperated fields:
        #       kind [tar, gz-tar]
        #       tarbal URL
        #       sample_label
        #       size
        #   Eg:
        #       tar s3://bucket/giantSetofReads.tar 30G
        #   Place your samples below, one sample per line.
        """[1:])
    return run_manifest if command == "generate" else readstore_manifest


def generateConfig(command):
    run_config = textwrap.dedent("""
        # UCSC Nanopore Pipeline configuration file
        # This configuration file is formatted in YAML. Simply write the value (at least one space) after the colon.
        # Edit the values in this configuration file and then rerun the pipeline: "toil-nanopore run"
        #
        # URLs can take the form: http://, ftp://, file://, s3://, gnos://
        # Local inputs follow the URL convention: file:///full/path/to/input
        # S3 URLs follow the convention: s3://bucket/directory/file.txt
        #
        # some options have been filled in with defaults
        # output directory
        output_dir:
        # reference sequences (FASTA)
        ref:      s3://arand-sandbox/chr20.fa
        ref_size: 100M

        # ledger, pickle containing (read_label, npRead_url) pairs
        ledger_url: s3://arand-sandbox/signalAlign_ci/chr20_part5__ledger.pkl

        # models
        HMM_file: s3://arand-sandbox/signalAlign_ci/models/template_trained.hmm
        HDP_file: s3://arand-sandbox/signalAlign_ci/models/template.singleLevelPrior.nhdp

        # sharding/batching options (only change these if you know what you're doing!)
        split_chromosome_this_length:    1000000
        max_alignment_length_per_job:    700000
        max_alignments_per_job:          300
        cut_batch_at_alignment_this_big: 20000

        # signalAlign options
        motif_key:
        substitute_char:
        degenerate:
        
        debug: True
    """[1:])

    readstore_config = textwrap.dedent("""
        # UCSC SignalAlign READSTORE Pipeline configuration file
        # This configuration file is formatted in YAML. Simply write the value (at least one space) after the colon.
        # Edit the values in this configuration file and then rerun the pipeline: "toil-signalAlign run-readstore"
        #
        # URLs can take the form: http://, ftp://, file://, s3://, gnos://
        # Local inputs follow the URL convention: file:///full/path/to/input
        # S3 URLs follow the convention: s3://bucket/directory/file.txt
        #
        # some options have been filled in with defaults
        readstore_dir: s3://arand-sandbox/ci_readstore/
        readstore_ledger_dir: s3://arand-sandbox/
        ledger_name: test
        # batching options
        # `split_tars_bigger_than_this` will split up tar'ed fast5 archives into smaller files (n.b this
        # is based on the input size in the manifest)  it will then split them into pieces of
        # `put_this_many_reads_in_a_tar` if the tar is smaller than `split_tars_bigger_than_this`
        # it will just pass it on to the next step that makes NanoporeRead objects and uploads
        # them to the readstore
        split_tars_bigger_than_this:
        put_this_many_reads_in_a_tar:
        NanoporeRead_batchsize: 2
        debug: True
    """[1:])

    return run_config if command == "generate" else readstore_config


def parseManifestReadstore(path_to_manifest):
    require(os.path.exists(path_to_manifest), "[parseManifest]Didn't find manifest file, looked "
            "{}".format(path_to_manifest))
    allowed_file_types = ("tar", "gz-tar")

    def parse_line(line):
        # double check input, shouldn't need to though
        require(not line.isspace() and not line.startswith("#"), "[parse_line]Invalid {}".format(line))
        sample_line = line.strip().split("\t")
        require(len(sample_line) == 4, "[parse_line]Invalid, len(line) != 4, offending {}".format(line))
        filetype, url, sample_label, size = sample_line
        # checks:
        # check filetype
        require(filetype in allowed_file_types, "[parse_line]Unrecognized file type {}".format(filetype))
        # check URL
        require(urlparse(url).scheme and urlparse(url),
                "Invalid URL passed for {}".format(url))

        return ReadstoreSample(file_type=filetype, URL=url, size=human2bytes(size), sample_label=sample_label)

    with open(path_to_manifest, "r") as fH:
        return map(parse_line, [x for x in fH if (not x.isspace() and not x.startswith("#"))])


def parseManifest(path_to_manifest):
    require(os.path.exists(path_to_manifest), "[parseManifest]Didn't find manifest file, looked "
            "{}".format(path_to_manifest))

    def parse_line(line):
        # double check input, shouldn't need to though
        require(not line.isspace() and not line.startswith("#"), "[parse_line]Invalid {}".format(line))
        sample_line = line.strip().split("\t")
        require(len(sample_line) == 3, "[parse_line]Invalid, len(line) != 3, offending {}".format(line))
        url, sample_label, size = sample_line
        # check alignment URL
        require(urlparse(url).scheme and urlparse(url), "Invalid URL passed for {}".format(url))

        return SignalAlignSample(URL=url, size=size, sample_label=sample_label)

    with open(path_to_manifest, "r") as fH:
        return map(parse_line, [x for x in fH if (not x.isspace() and not x.startswith("#"))])


def main():
    """toil-signalAlign master script
    """
    def parse_args():
        parser = argparse.ArgumentParser(description=print_help.__doc__,
                                         formatter_class=argparse.RawTextHelpFormatter)
        subparsers = parser.add_subparsers(dest="command")

        # parsers for running the full pipeline
        run_parser = subparsers.add_parser("run", help="runs full workflow on a BAM")
        run_parser.add_argument('--config', default='config-toil-signalAlign.yaml', type=str,
                                help='Path to the (filled in) config file, generated with "generate".')
        run_parser.add_argument('--manifest', default='manifest-toil-signalAlign.tsv', type=str,
                                help='Path to the (filled in) manifest file, generated with "generate". '
                                     '\nDefault value: "%(default)s".')
        subparsers.add_parser("generate", help="generates a config file for your run, do this first")

        # parsers for running the readstore pipeline
        readstore_parser = subparsers.add_parser("run-readstore",
                                                 help="generates a readstore from a tar of .fast5s")
        readstore_parser.add_argument('--config', default='config-toil-signalAlign-readstore.yaml', type=str,
                                      help='Path to the (filled in) config file, generated with "generate".')
        readstore_parser.add_argument('--manifest', default='manifest-toil-signalAlign-readstore.tsv', type=str,
                                      help='Path to the (filled in) manifest file, generated with "generate". '
                                      '\nDefault value: "%(default)s".')
        subparsers.add_parser("generate-readstore", help="generates a config file for making a readstore")

        Job.Runner.addToilOptions(run_parser)
        Job.Runner.addToilOptions(readstore_parser)

        return parser.parse_args()

    def exitBadInput(message=None):
        if message is not None:
            print(message, file=sys.stderr)
        sys.exit(1)

    if len(sys.argv) == 1:
        exitBadInput(print_help())

    cwd = os.getcwd()

    args = parse_args()

    if args.command == "generate" or args.command == "generate-readstore":
        if args.command == "generate":
            config_filename   = "config-toil-signalAlign.yaml"
            manifest_filename = "manifest-toil-signalAlign.tsv"
        else:
            config_filename   = "config-toil-signalAlign-readstore.yaml"
            manifest_filename = "manifest-toil-signalAlign-readstore.tsv"

        configGenerator   = partial(generateConfig, command=args.command)
        manifestGenerator = partial(generateManifest, command=args.command)

        try:
            config_path = os.path.join(cwd, config_filename)
            generate_file(config_path, configGenerator)
        except UserError:
            print("[toil-nanopore]NOTICE using existing config file {}".format(config_path))
            pass
        try:
            manifest_path = os.path.join(cwd, manifest_filename)
            generate_file(manifest_path, manifestGenerator)
        except UserError:
            print("[toil-nanopore]NOTICE using existing manifest {}".format(manifest_path))

    elif args.command == "run":
        require(os.path.exists(args.config), "{config} not found run generate".format(config=args.config))
        # Parse config
        config  = {x.replace('-', '_'): y for x, y in yaml.load(open(args.config).read()).iteritems()}
        samples = parseManifest(args.manifest)
        for sample in samples:
            with Toil(args) as toil:
                if not toil.options.restart:
                    root_job = Job.wrapJobFn(signalAlignCheckInputJobFunction, config, sample)
                    return toil.start(root_job)
                else:
                    toil.restart()
    elif args.command == "run-readstore":
        require(os.path.exists(args.config), "{config} not found run generate-readstore".format(config=args.config))
        # Parse config
        config  = {x.replace('-', '_'): y for x, y in yaml.load(open(args.config).read()).iteritems()}
        samples = parseManifestReadstore(args.manifest)
        with Toil(args) as toil:
            if not toil.options.restart:
                root_job = Job.wrapJobFn(makeReadstoreJobFunction, config, samples)
                return toil.start(root_job)
            else:
                toil.restart()


if __name__ == '__main__':
    try:
        main()
    except UserError as e:
        print(e.message, file=sys.stderr)
        sys.exit(1)
