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

import pathlib
import jsonable
import more_itertools
import mwxml
import regex
import arrow
from typing import Iterable, Iterator, Mapping, NamedTuple, Optional

from .. import utils
from .. import file_utils as fu
from .. import dumper

csv_header = ('page_id',
              'page_title',
              'wikilink_id',
              'wikilink_title',
              'wikilink_original',
              )

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
    r'''ngi_graph\.([0-9]{4})-([0-9]{2})-([0-9]{2})\.csv''')


stats_template = \
'''<stats>
    <performance>
        <start_time>${stats['performance']['start_time']}</start_time>
        <end_time>${stats['performance']['end_time']}</end_time>
        <revisions_analyzed>${stats['performance']['revisions_analyzed']}</revisions_analyzed>
    </performance>
    <links>
        <good_links>
            <direct>${stats['links']['good_links']['direct']}</direct>
            <redirect>${stats['links']['good_links']['redirect']}</redirect>
        </good_links>
        <bad_links>
            % for tkey, tval in stats['links']['bad_links'].items():
                <term count=${tval}>${tkey}</term>
            % endfor
        </bad_links>

    </links>

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
        pages_in_snapshot: Mapping,
        redirects: Mapping) -> Iterator[list]:
    """Assign each revision to the snapshot to which they
       belong.
    """
    dump_page = None
    dump_prevpage = None

    linkline = None

    # skip header
    next(dump)

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
        stats['performance']['revisions_analyzed'] += 1
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
        try:
            dump_page = Page(int(linkline[0]),
                             linkline[1].replace(' ', '_'),
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
        except ValueError:
            continue

        # Print page id
        if dump_prevpage is None or dump_prevpage.id != dump_page.id:
            utils.log("Processing page id {}".format(dump_page.id))

        # print a dot for each link analyzed
        utils.dot()

        original_wikilink = dump_page.revision.wikilink.link
        wikilink = first_uppercase(original_wikilink).strip()
        wikilink = '_'.join(wikilink.split())

        if wikilink in pages_in_snapshot:

            if wikilink in redirects:
                stats['links']['good_links']['redirect'] += 1

                new_wikilink = redirects[wikilink]
                new_wikilink_id = pages_in_snapshot.get(new_wikilink, None)

                if new_wikilink_id is None:
                    stats['links']['good_links']['redirect'] -= 1
                    stats['links']['bad_links'][new_wikilink] += 1

                wikilink = new_wikilink
                wikilink_id = new_wikilink_id
            else:
                stats['links']['good_links']['direct'] += 1
                wikilink_id = pages_in_snapshot[wikilink]

            yield (dump_page.id,dump_page.title, wikilink_id, wikilink,
                   original_wikilink)

        else:
            stats['links']['bad_links'][wikilink] += 1


        dump_prevpage = dump_page


def configure_subparsers(subparsers):
    """Configure a new subparser ."""
    parser = subparsers.add_parser(
        'match-ngi-id',
        help='Extract graph with page ID from link snapshots',
    )
    parser.add_argument(
        '--snapshot-dir',
        type=pathlib.Path,
        help='Directory with snapshot files.'
    )
    parser.add_argument(
        '--redirects',
        type=pathlib.Path,
        help='List with redirects.'
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
        },
        'links': {
            'good_links': {
                'direct': 0,
                'redirect': 0,
            },
            'bad_links': collections.defaultdict(int),
        }
    }

    match = basename_re.match(basename)
    year = int(match.group(1))
    month = int(match.group(2))
    day = int(match.group(3))

    date = arrow.Arrow(year, month, day)

    snapshot_filename = str(args.snapshot_dir /
                                         ('snapshot.{date}.csv.gz'))
    snapshot_filename = snapshot_filename.format(
        date=date.format('YYYY-MM-DD'))
    snapshot_infile = fu.open_csv_file(snapshot_filename)
    snapshot_reader = csv.reader(snapshot_infile)

    redirects = dict()
    with open(str(args.redirects), 'r') as redirects_file:
        reader = csv.reader(redirects_file, delimiter='\t')

        # skip header
        next(reader)

        redirects = dict((k,v) for k,v in reader)

    # import ipdb; ipdb.set_trace()

    pages_in_snapshot = dict()
    for row_data in snapshot_reader:
        page_title = first_uppercase(row_data[1]).replace(' ', '_')
        pages_in_snapshot[page_title] = int(row_data[0])

    if args.dry_run:
        pages_output = open(os.devnull, 'wt')
        stats_output = open(os.devnull, 'wt')
    else:
        filename = str(args.output_dir_path /
                               ('wikilink_ngi_graph.{date}.csv'))
        filename = filename.format(date=date.format('YYYY-MM-DD'))

        pages_output = fu.output_writer(
            path=filename,
            compression=args.output_compression,
        )

        filename_stats = str(args.output_dir_path /
                               ('wikilink_ngi_graph.{date}.stats.xml'))
        filename_stats = filename_stats.format(date=date.format('YYYY-MM-DD'))
        stats_output = fu.output_writer(
            path=filename_stats,
            compression=args.output_compression,
        )

    with pages_output:
        stats['performance']['start_time'] = datetime.datetime.utcnow()

        dump = csv.reader(dump)
        pages_generator = process_lines(
            dump,
            stats,
            pages_in_snapshot=pages_in_snapshot,
            redirects=redirects,
        )

        writer = csv.writer(pages_output, delimiter='\t')
        writer.writerow(csv_header)
        for edge in pages_generator:
            writer.writerow(edge)
        stats['performance']['end_time'] = datetime.datetime.utcnow()


    with stats_output:
        dumper.render_template(
            stats_template,
            stats_output,
            stats=stats,
        )