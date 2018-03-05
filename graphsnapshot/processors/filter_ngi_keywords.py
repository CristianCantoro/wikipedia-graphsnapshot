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


Wikilink = NamedTuple('Wikilink', [
    ('link', str),
    ('anchor', int),
    ('section_name', str),
    ('section_level', int),
    ('section_number', int),
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


splitline_re = regex.compile(
    r'''^([0-9]{2,}),.+?,([0-9]+),''', regex.VERBOSE)


output_csv_header = ('page_id',
                     'page_title',
                     'revision_id',
                     'revision_parent_id',
                     'revision_timestamp',
                     'user_type',
                     'user_username',
                     'user_id',
                     'revision_minor',
                     'wikilink.link',
                     'wikilink.anchor',
                     'wikilink.section_name',
                     'wikilink.section_level',
                     'wikilink.section_number',
                     'wikinlink.is_active'
                     )


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
        pages_in_snapshot: set,
        pagetitles_in_snapshot: set,
        revisions_in_snapshot: set) -> Iterator[list]:
    """Assign each revision to the snapshot to which they
       belong.
    """
    # skip header
    next(dump)

    dump_page = None
    dump_prevpage = None

    old_linkline = None
    linkline = None

    skip_page = False

    dump_prevpage_id = 0
    dump_page_id = 0
    dump_prevpage_revision_id = 0
    dump_page_revision_id = 0

    # -------------------------------------------------------------------------
    # OUTLINE OF THE ALGORITHM
    #
    # * read a line at a time from the input file, we call each line a linkline
    #   (because each line of the input has a wikilink)
    #
    # * if the linkline has a page and revision id that are contained in the
    #   snapshot process them, otherwise skip
    # -------------------------------------------------------------------------

    # Loop over all lines, this is equivalent to
    # for link in dump:
    while True:
        old_linkline = linkline
        linkline = next(dump, None)

        if linkline is None:
            # this is the last line, end loop.
            break

        # Split the line to get page id and revision id, if something goes
        # wrong we ignore that line.
        revmatch = splitline_re.match(linkline)
        if revmatch is not None:
            dump_page_id = int(revmatch.group(1))
            dump_page_revision_id = int(revmatch.group(2))
        else:
            continue

        # Print page id
        if dump_prevpage == 0 or dump_prevpage_id != dump_page_id:
            utils.log("Processing page id {}".format(dump_page_id))

        yield (dump_page, active_link)

        dump_prevpage_id = dump_page_id
        dump_prevpage_revision_id = dump_page_revision_id
        dump_prevpage = dump_page


def configure_subparsers(subparsers):
    """Configure a new subparser ."""
    parser = subparsers.add_parser(
        'link-snapshot-extractor',
        help='Extract link snapshots from page list',
    )
    parser.add_argument(
        '--date',
        type=str,
        help='Reference date'
    )
    parser.add_argument(
        '--snapshot-file',
        type=str,
        help='Snapshot file.'
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
        'section_names': {
            'global': collections.Counter(),
            'last_revision': collections.Counter(),
        },
    }

    date = arrow.get(args.date)

    snapshot_infile = fu.open_csv_file(args.snapshot_file)
    snapshot_reader = csv.reader(fu.open_csv_file(snapshot_infile))

    pages_in_snapshot = set()
    revisions_in_snapshot = set()
    pagetitles_in_snapshot = set()
    for row_data in snapshot_reader:
        pages_in_snapshot.add(int(row_data[0]))
        pagetitles_in_snapshot.add(first_uppercase(row_data[1]))
        revisions_in_snapshot.add(int(row_data[2]))

    if args.dry_run:
        pages_output = open(os.devnull, 'wt')
        stats_output = open(os.devnull, 'wt')
    else:
        filename = str(args.output_dir_path /
                       (basename + '.features.{date}.csv'))
        filename = filename.format(date=date.format('YYYY-MM-DD'))

        pages_output = fu.output_writer(
            path=filename,
            compression=args.output_compression,
        )
        stats_output = fu.output_writer(
            path=str(args.output_dir_path/(basename + '.stats.xml')),
            compression=args.output_compression,
        )

    writer = csv.writer(pages_output)

    pages_generator = process_lines(
        dump,
        pages_in_snapshot=pages_in_snapshot,
        pagetitles_in_snapshot=pagetitles_in_snapshot,
        revisions_in_snapshot=revisions_in_snapshot,
    )

    writer.writerow(output_csv_header)

    for page, active_link in pages_generator:
        writer.writerow((
            page.id,
            page.title,
            page.revision.id,
            page.revision.parent_id,
            page.revision.timestamp,
            page.revision.user_type,
            page.revision.username,
            page.revision.user_id,
            page.revision.minor,
            page.revision.wikilink.link,
            page.revision.wikilink.anchor,
            page.revision.wikilink.section_name,
            page.revision.wikilink.section_level,
            page.revision.wikilink.section_number,
            active_link
        ))
