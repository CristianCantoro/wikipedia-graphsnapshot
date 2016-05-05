"""
Build graph from a snapshot match link page titles to their page IDs.

The output format is csv.
"""

import os
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


Wikilink = NamedTuple('Wikilink', [
    ('link', str),
    ('anchor', int),
    ('section_name', str),
    ('section_level', int),
    ('section_number', int),
    ('is_active', int),
])


Revision = NamedTuple('Revision', [
    ('id', int),
    ('parent_id', int),
    ('user_type', str),
    ('username', str),
    ('user_id', int),
    ('minor', bool),
    ('timestamp', arrow.arrow.Arrow),
    ('wikilink', Wikilink)
])


Page = NamedTuple('Page', [
    ('id', int),
    ('title', str),
    ('revision', Revision),
])


basename_re = regex.compile(
    r'''link_snapshot\.([0-9]{4})-([0-9]{2})-([0-9]{2})\.csv\.gz''')


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
        pages_in_snapshot: str) -> Iterator[list]:
    """Assign each revision to the snapshot to which they
       belong.
    """
    dump_page = None
    dump_prevpage = None

    linkline = None

    # -------------------------------------------------------------------------
    # OUTLINE OF THE ALGORITHM
    #
    # * read a line at a time from the input file, we call each line a linkline
    #   (because each line of the input has a wikilink)
    #
    # * if the linkline has a page and revision id that are contained in the
    #   snapshot process them, otherwise skip
    # -------------------------------------------------------------------------
    for linkline in dump:
        # Mapping from the input csv
        # 'page_id', 0
        # 'page_title', 1
        # 'revision_id', 2
        # 'revision_parent_id', 3
        # 'revision_timestamp', 4
        # 'user_type', 5
        # 'user_username', 6
        # 'user_id', 7
        # 'revision_minor', 8
        # 'wikilink.link', 9
        # 'wikilink.anchor', 10
        # 'wikilink.section_name', 11
        # 'wikilink.section_level', 12
        # 'wikilink.section_number' 13
        # 'wikilink.is_active' 14
        dump_page = Page(int(linkline[0]),
                         linkline[1],
                         Revision(int(linkline[2]),
                                  int(linkline[3])
                                  if linkline[3] else None,
                                  linkline[5],
                                  linkline[6],
                                  linkline[7],
                                  linkline[8],
                                  linkline[4],
                                  Wikilink(linkline[9],
                                           linkline[10],
                                           linkline[11],
                                           int(linkline[12]),
                                           int(linkline[13]),
                                           int(linkline[14]),
                                           )))
        # Print page id
        if dump_prevpage is None or dump_prevpage.id != dump_page.id:
            utils.log("Processing page id {}".format(dump_page.id))

        # print a dot for each link analyzed
        utils.dot()

        wikilink = (first_uppercase(dump_page.revision.wikilink.link)
                    .strip()
                    )
        wikilink = ' '.join(wikilink.split())

        if dump_page.revision.wikilink.is_active \
                and wikilink in pages_in_snapshot:
            wikilink_id = pages_in_snapshot[wikilink]
            yield (dump_page.id, wikilink_id)

        dump_prevpage = dump_page


def configure_subparsers(subparsers):
    """Configure a new subparser ."""
    parser = subparsers.add_parser(
        'match-id',
        help='Extract graph with page ID from link snapshots',
    )
    parser.add_argument(
        '--snapshot-dir',
        type=str,
        help='Snapshot file.'
    )
    parser.set_defaults(func=main)


def main(
        dump: Iterable[list],
        basename: str,
        args) -> None:
    """Main function that parses the arguments and writes the output."""

    match = basename_re.match(basename)
    year = int(match.group(1))
    month = int(match.group(2))
    day = int(match.group(3))

    date = arrow.Arrow(year, month, day)

    snapshot_filename = str(os.path.join(args.snapshot_dir,
                                         'snapshot.{date}.csv.gz'))
    snapshot_filename = snapshot_filename.format(
        date=date.format('YYYY-MM-DD'))
    snapshot_infile = fu.open_csv_file(snapshot_filename)
    snapshot_reader = csv.reader(snapshot_infile)

    pages_in_snapshot = dict()
    for row_data in snapshot_reader:
        pages_in_snapshot[first_uppercase(row_data[1])] = int(row_data[0])

    if args.dry_run:
        pages_output = open(os.devnull, 'wt')
    else:
        filename = str(args.output_dir_path /
                       ('wikilink_graph.{date}.csv'))
        filename = filename.format(date=date.format('YYYY-MM-DD'))

        pages_output = fu.output_writer(
            path=filename,
            compression=args.output_compression,
        )

    writer = csv.writer(pages_output, delimiter=' ')

    dump = csv.reader(dump)
    pages_generator = process_lines(
        dump,
        pages_in_snapshot=pages_in_snapshot
    )

    for page_id, wikilink_id in pages_generator:
        writer.writerow((
            page_id,
            wikilink_id
        ))
