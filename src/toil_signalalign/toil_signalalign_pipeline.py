#!/usr/bin/env python2.7
from __future__ import print_function

import sys
import argparse
import os
import textwrap
import yaml

from urlparse import urlparse
from functools import partial

from toil.common import Toil
from toil.job import Job

from toil_lib.files import generate_file
from toil_lib import require, UserError

from signalalign.toil.ledger import makeNanoporeReadLedgerJobFunction

from minionSample import ReadstoreSample, SignalAlignSample


def signalAlignRootJobFunction(job, config, sample):
    raise NotImplementedError


def print_help():
    """this is the help for toil-signalAlign!
    """
    return print_help.__doc__


def generateManifest(command):
    run_manifest = textwrap.dedent("""
        #   Edit this manifest to include information for each sample to be run.
        #   N.B. See README for description of 'ledger'
        #   Lines should contain three tab-seperated fields:
        #       MinION_filetype  - options : ledger, tar-f5
        #       Alignment URL
        #       MinION_URL
        #       Sample_label
        #       Alignment file size
        #       Minion_tar file size  nb. for 'ledger' this isn't used but put a place holder
        #   Eg:
        #   tar-f5  s3://yourbucket/chr20.bam   s3://groupdata/chr20.fast5s.tar mySample    4G  20G
        #   ledger  s3://yourbucker/chr17.bam   s3://parseddata/chr17.ledger    Sample2     4G  10M
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

        ## Universal Options/Inputs ##
        # Required: Which subprograms to run, typically you run all 4, but you can run them piecemeal if you like
        # prepare_fast5 -  extract and upload .fast5s from an archive to S3 as NanoporeReads, required for
        # all downstream analysis, but only needs to be performed once per dataset
        prepare_fast5: True
        prepare_batch_size:
        debug: True
    """[1:])

    readstore_config = textwrap.dedent("""
        # UCSC SignalAlign READSTORE Pipeline configuration file
        # This configuration file is formatted in YAML. Simply write the value (at least one space) after the colon.
        # Edit the values in this configuration file and then rerun the pipeline: "toil-nanopore run"
        #
        # URLs can take the form: http://, ftp://, file://, s3://, gnos://
        # Local inputs follow the URL convention: file:///full/path/to/input
        # S3 URLs follow the convention: s3://bucket/directory/file.txt
        #
        # some options have been filled in with defaults

        ## Universal Options/Inputs ##
        # Required: Which subprograms to run, typically you run all 4, but you can run them piecemeal if you like
        # prepare_fast5 -  extract and upload .fast5s from an archive to S3 as NanoporeReads, required for
        # all downstream analysis, but only needs to be performed once per dataset
        readstore_dir: s3://arand-sandbox/ci_readstore/
        readstore_ledger_dir: s3://arand-sandbox/
        batchsize: 5
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
        filetype, url, size, sample_label = sample_line
        # checks:
        # check filetype
        require(filetype in allowed_file_types, "[parse_line]Unrecognized file type {}".format(filetype))
        # check URL
        require(urlparse(url).scheme and urlparse(url),
                "Invalid URL passed for {}".format(url))

        return ReadstoreSample(file_type=filetype, URL=url, size=size, sample_label=sample_label)

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
        url, size, sample_label = sample_line
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
                    root_job = Job.wrapJobFn(signalAlignRootJobFunction, config, sample)
                    return toil.start(root_job)
                else:
                    toil.restart()
    elif args.command == "run-readstore":
        require(os.path.exists(args.config), "{config} not found run generate-readstore".format(config=args.config))
        # Parse config
        config  = {x.replace('-', '_'): y for x, y in yaml.load(open(args.config).read()).iteritems()}
        samples = parseManifestReadstore(args.manifest)
        for sample in samples:
            with Toil(args) as toil:
                if not toil.options.restart:
                    root_job = Job.wrapJobFn(makeNanoporeReadLedgerJobFunction, config, sample)
                    return toil.start(root_job)
                else:
                    toil.restart()


if __name__ == '__main__':
    try:
        main()
    except UserError as e:
        print(e.message, file=sys.stderr)
        sys.exit(1)
