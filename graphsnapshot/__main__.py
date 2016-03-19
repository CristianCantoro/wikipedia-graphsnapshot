"""Main module that parses command line arguments."""
import argparse
import codecs
import os
import subprocess
import gzip
import io
import csv

import mw.xml_dump
import mwxml
import pathlib
from typing import IO, Optional, Union
import compressed_stream as cs

from . import processors, utils


def open_csv_file(path: Union[str, IO]):
    """Open a csv file, decompressing it if necessary."""
    f = cs.functions.open_file(
        cs.functions.file(path)
    )
    return f


def compressor_7z(file_path: str):
    """"Return a file-object that compresses data written using 7z."""
    p = subprocess.Popen(
        ['7z', 'a', '-si', file_path],
        stdin=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
    )
    return io.TextIOWrapper(p.stdin, encoding='utf-8')


def output_writer(path: str, compression: Optional[str]):
    """Write data to a compressed file."""
    if compression == '7z':
        return compressor_7z(path + '.7z')
    elif compression == 'gzip':
        return gzip.open(path + '.gz', 'wt', encoding='utf-8')
    else:
        return open(path, 'wt', encoding='utf-8')


def create_path(path: Union[pathlib.Path, str]):
    """Create a path, which may or may not exist."""
    path = pathlib.Path(path)
    if not path.parent.exists():
        path.parent.mkdir(parents=True)


def get_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        prog='graphsnapshot',
        description='Graph snapshot features extractor.',
    )
    parser.add_argument(
        'files',
        metavar='FILE',
        type=pathlib.Path,
        nargs='+',
        help='XML Wikidump file to parse. It accepts only 7z.',
    )
    parser.add_argument(
        'output_dir_path',
        metavar='OUTPUT_DIR',
        type=pathlib.Path,
        help='XML output directory.',
    )
    parser.add_argument(
        '--output-compression',
        choices={None, '7z', 'gzip'},
        required=False,
        default=None,
        help='Output compression format.',
    )
    parser.add_argument(
        '--dry-run', '-n',
        action='store_true',
        help="Don't write any file",
    )

    subparsers = parser.add_subparsers(help='sub-commands help')
    processors.snapshot_extractor.configure_subparsers(subparsers)

    parsed_args = parser.parse_args()
    if 'func' not in parsed_args:
        parser.print_usage()
        parser.exit(1)

    return parsed_args


def main():
    """Main function."""
    args = get_args()

    if not args.output_dir_path.exists():
        args.output_dir_path.mkdir(parents=True)

    for input_file_path in args.files:
        utils.log("Analyzing {}...".format(input_file_path))

        dump = csv.reader(open_csv_file(str(input_file_path)))

        basename = input_file_path.name

        if args.dry_run:
            pages_output = open(os.devnull, 'wt')
            stats_output = open(os.devnull, 'wt')
        else:
            pages_output = output_writer(
                path=str(args.output_dir_path/(basename + '.features.csv')),
                compression=args.output_compression,
            )
            stats_output = output_writer(
                path=str(args.output_dir_path/(basename + '.stats.xml')),
                compression=args.output_compression,
            )
        args.func(
            dump,
            pages_output,
            stats_output,
            args,
        )


if __name__ == '__main__':
    main()
