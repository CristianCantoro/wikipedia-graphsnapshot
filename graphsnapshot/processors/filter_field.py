"""
Extract links from list of revisions.

The output format is csv.
"""

import io
import sys
import csv
import collections
import datetime
import functools

import jsonable
import more_itertools
import mwxml
import regex
import arrow
from typing import Iterable, Iterator, Mapping, NamedTuple, Optional

from .. import utils
from .. import file_utils as fu
from .. import dumper


stats_template = \
'''<stats>
    <performance>
        <start_time>${stats['performance']['start_time']}</start_time>
        <end_time>${stats['performance']['end_time']}</end_time>
        <revisions_analyzed>${stats['performance']['revisions_analyzed']}</revisions_analyzed>
    </performance>
</stats>
'''


def first_uppercase(string: str) -> str:
    """
    Make the first charachter of a str "string" uppercase and leave the rest
    unchanged.
    """
    if len(string) > 0:
        return string[0].upper() + string[1:]
    else:
        return ''


def process_lines(
        dump: Iterable[list],
        stats: Mapping,
        net: set,
        compiled_redirects: set) -> Iterator[list]:
    """Assign each revision to the snapshot to which they
       belong.
    """

    # skip header
    next(dump)

    old_linkline = None
    linkline = None

    # -------------------------------------------------------------------------
    # OUTLINE OF THE ALGORITHM
    #
    # * read a line at a time from the input file, we call each line a linkline
    #   (because each line of the input has a wikilink)
    #
    # -------------------------------------------------------------------------

    # Loop over all lines, this is equivalent to
    # for link in dump:
    while True:
        old_linkline = linkline
        linkline = next(dump, None)

        if linkline is None:
            # this is the last line, end loop.
            break

        stats['performance']['revisions_analyzed'] += 1
        page_title = first_uppercase(linkline[1].replace(' ', '_'))
        link_title = linkline[9]

        if page_title in net and \
            any(reg.match(link_title)
                for reg in compiled_redirects):

            yield linkline


def configure_subparsers(subparsers):
    """Configure a new subparser ."""
    parser = subparsers.add_parser(
        'filter-field',
        help='Filter files by field value',
    )
    parser.add_argument(
        '--match-file',
        required=True,
        type=str,
        help='File containing the list of fields to match.'
    )
    parser.set_defaults(func=main)


def main(
        dump: Iterable[list],
        basename: str,
        args) -> None:
    """Main function that parses the arguments and writes the output."""
    stats = {
        'performance': {
            'start_time': None,
            'end_time': None,
            'revisions_analyzed': 0,
            'pages_analyzed': 0,
        },
    }

    matches = set([term.strip() for term in open(args.match_file).readlines()])
    import ipdb; ipdb.set_trace()

    if args.dry_run:
        pages_output = open(os.devnull, 'wt')
        stats_output = open(os.devnull, 'wt')
    else:
        filename = str(args.output_dir_path /
                       (basename + '.features.{date}.csv'))
        filename = filename.format(date=args.date)

        pages_output = fu.output_writer(
            path=filename,
            compression=args.output_compression,
        )
        stats_output = fu.output_writer(
            path=str(args.output_dir_path/(basename + '.stats.xml')),
            compression=args.output_compression,
        )

    with pages_output:
        stats['performance']['start_time'] = datetime.datetime.utcnow()

        dump = csv.reader(dump)
        pages_generator = process_lines(
            dump,
            stats,
            net=net,
            compiled_redirects=compiled_redirects,
        )

        writer = csv.writer(pages_output)
        writer.writerow(csv_header)
        for linkline in pages_generator:
            writer.writerow(linkline)
        stats['performance']['end_time'] = datetime.datetime.utcnow()

    with stats_output:
        dumper.render_template(
            stats_template,
            stats_output,
            stats=stats,
        )
