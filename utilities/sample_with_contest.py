#!/usr/bin/env python3

import os
import random
import pathlib
import argparse
import subprocess
import more_itertools

import compressed_stream as cs

# How to get line count cheaply in Python?
#   https://stackoverflow.com/q/845058/2377454
def count_file_lines(file_path):
    f = cs.functions.open_file(cs.functions.file(file_path))

    for i, l in enumerate(f):
        pass

    return i + 1


def cli_args():
    parser = argparse.ArgumentParser(
        description='Sample a file with context.')
    parser.add_argument("FILE",
                        type=pathlib.Path,
                        help="Input file."
                        )
    parser.add_argument('-C', '--context',
                        default=2,
                        type=int,
                        help='Number of lines to take before and after '
                             '[default: 2].'
                        )
    parser.add_argument('-n', '--num',
                        default=100,
                        type=int,
                        help='Number of line to sample, not including context '
                             '[default: 100].'
                        )
    parser.add_argument('-o', '--output',
                        help='Name of the output file '
                             '[default: {basename}.sample.n{n}c{C}.{ext}].'
                        )


    args = parser.parse_args()

    return args


def main():

    args = cli_args()

    infile = args.FILE
    L = count_file_lines(infile)

    C = args.context
    n = args.num
    output = args.output

    assert (L > n*(2*C+1)), \
           ('The number of lines in FILE must be greater than the sample size '
            '(n*(2*C+1)).')

    dirname = os.path.dirname(infile)
    basename = os.path.basename(infile)
    basename, ext = os.path.splitext(basename)
    ext = ext.lstrip('.')

    if ext in ('7z', 'gzip', 'gz', 'bz2'):
        basename, ext = os.path.splitext(basename)
        ext = ext.lstrip('.')

    if output is None:
        samplename = '{basename}.sample.n{n}c{C}.{ext}'.format(
            basename=basename,n=n,C=C,ext=ext)
        output = os.path.join(dirname, samplename)

    samplelines = set(more_itertools.flatten([
                    list(range(i-C, i+C+1))
                    for i in
                    # the interval in which we generate the numbers is
                    # [C+2, L-C]
                    # because:
                    #   * C+2: we want to skip the first line and then
                    #     generate numbers i at least as big as C+1 to be able
                    #     to have i-C other numbers (left context)
                    #   * L-C: right context
                    sorted(random.sample(range(C+2, L-C), n))
                  ]))


    numl = 0
    infp = cs.functions.open_file(cs.functions.file(infile))
    with open(output, 'w+') as outfp:
        for line in infp:
            numl = numl + 1

            if numl in samplelines:
                outfp.write(line)


if __name__ == '__main__':
    main()