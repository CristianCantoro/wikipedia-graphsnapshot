#!/usr/bin/env python3

import os
import random
import pathlib
import argparse
import subprocess
import more_itertools

import compressed_stream as cs


def get_first_column(file_path, cast_type=None, skip_header=False):
    f = cs.functions.open_file(cs.functions.file(file_path))

    if skip_header:
        next(f)

    values = set()
    for line in f:
        v = line.split(',')[0]

        if cast_type is not None:
            v = cast_type(v)

        values.add(v)

    return values


def cli_args():
    parser = argparse.ArgumentParser(
        description='Sample all line pertaining to randomly chosen articles '
                    'from file.')
    parser.add_argument("FILE",
                        type=pathlib.Path,
                        help="Input file."
                        )
    parser.add_argument('-n', '--num',
                        default=100,
                        type=int,
                        help='Number of articles to sample [default: 100].'
                        )
    parser.add_argument('--skip-header',
                        action='store_true',
                        help='Skip the first row (header) of the input file.'
                        )
    parser.add_argument('--print-ids',
                        action='store_true',
                        help='Print sample article ids.'
                        )
    parser.add_argument('-o', '--output',
                        help='Name of the output file '
                             '[default: {basename}.sample.a{a}.{ext}].'
                        )


    args = parser.parse_args()

    return args


def main():

    args = cli_args()

    infile = args.FILE

    numa = args.num
    output = args.output
    skip_header = args.skip_header

    dirname = os.path.dirname(infile)
    basename = os.path.basename(infile)
    basename, ext = os.path.splitext(basename)
    ext = ext.lstrip('.')

    if ext in ('7z', 'gzip', 'gz', 'bz2'):
        basename, ext = os.path.splitext(basename)
        ext = ext.lstrip('.')

    if output is None:
        samplename = '{basename}.samplearticles.n{n}.{ext}'.format(
            basename=basename,n=numa,ext=ext)
        output = os.path.join(dirname, samplename)

    ids = get_first_column(infile, cast_type=int, skip_header=skip_header)
    
    samplearticles = set(random.sample(ids, numa))

    if args.print_ids:
        for _id in sorted(samplearticles):
            print(_id)

    infp = cs.functions.open_file(cs.functions.file(infile))
    header = None
    if skip_header:
        header = next(infp)

    with open(output, 'w+') as outfp:
        if header:
            outfp.write(header)

        for line in infp:
            line_id = int(line.split(',')[0])

            if line_id in samplearticles:
                outfp.write(line)


if __name__ == '__main__':
    main()
