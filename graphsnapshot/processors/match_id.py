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
from .. import dumper


# templates
stats_template = '''
<stats>
    <performance>
        <start_time>${stats['performance']['start_time'] | x}</start_time>
        <end_time>${stats['performance']['end_time'] | x}</end_time>
        <pages_analyzed>${stats['performance']['pages_analyzed'] | x}</pages_analyzed>
        <links_analyzed>${stats['performance']['links_analyzed'] | x}</links_analyzed>
    </performance>
    <links>
        <active>${stats['links']['active'] | x}</active>
        <active>${stats['links']['redirected'] | x}</active>
    </links>
</stats>
'''


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


# page.from.id
# page.from.title
# page.to.id
# page.to.title
csv_header_output_titles = ('page_id_from',
                            'page_title_from',
                            'page_id_to',
                            'page_title_to',
                            )


# page.from.id
# page.to.id
csv_header_output_notitles = ('page_id_from',
                              'page_id_to',
                              )


basename_re = regex.compile(
    r'''.*link_snapshot\.([0-9]{4})-([0-9]{2})-([0-9]{2})\.csv\.gz''')


def process_lines(
        dump: Iterable[list],
        stats: Mapping,
        pages_in_snapshot: Mapping,
        pages_redirected: Mapping,
        ids_redirected: Mapping,
        add_titles: bool,
        trim_redirects: bool
        ) -> Iterator[list]:
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
        try:
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
        except ValueError:
            continue

        # Print page id
        if dump_prevpage is None or dump_prevpage.id != dump_page.id:
            utils.log("Processing page id {}".format(dump_page.id))
            stats['performance']['pages_analyzed'] += 1

        # print a dot for each link analyzed
        utils.dot()
        stats['performance']['links_analyzed']

        wikilink = (utils.normalize_wikititle(dump_page.revision.wikilink.link)
                    .strip()
                    )
        wikilink = ' '.join(wikilink.split())

        if dump_page.revision.wikilink.is_active \
                and wikilink in pages_in_snapshot:

            stats['links']['active'] += 1
            wikilink_id = pages_in_snapshot[wikilink]
            if wikilink in pages_redirected:
                stats['links']['redirected'] += 1
                redirect = pages_redirected[wikilink]
                redirect_id = pages_in_snapshot[redirect]

                # redirect is the new wikilink
                wikilink = redirect
                wikilink_id = redirect_id

            if trim_redirects:
                if dump_page.id in ids_redirected and \
                        ids_redirected[dump_page.id] == wikilink_id:
                    # it's a redirect page, we skip it
                    continue

            # yield (page_from, page_to)
            if add_titles:
                yield (Page(dump_page.id, dump_page.title, None),
                       Page(wikilink_id, wikilink, None)
                       )
            else:
                yield (Page(dump_page.id, None, None),
                       Page(wikilink_id, None, None)
                       )

        dump_prevpage = dump_page


def read_snapshot(reader, resolved_redirects=False):

    pages_in_snapshot = dict()
    pages_redirected = dict()
    ids_redirected = dict()
    # 1: page_id
    # 2: page_title
    # 3: revision_id
    # 4: revision_parent_id
    # 5: revision_timestamp
    # if resolved_redirects:
    #     6: redirect_id
    #     7: redirect_title
    #     8: redirect_revision_id
    #     9: redirect_revision_parent_id
    #     10: redirect_revision_timestamp

    for row_data in reader:
        norm_page_title = utils.normalize_wikititle(row_data[1])
        page_id = int(row_data[0])
        norm_redirect_title = ''
        redirect_id = -1

        if resolved_redirects:
            norm_redirect_title = utils.normalize_wikititle(row_data[6])
            redirect_id = int(row_data[5])

        pages_in_snapshot[norm_page_title] = page_id
        if redirect_id != -1 and redirect_id != page_id:
            pages_redirected[norm_page_title] = norm_redirect_title
            ids_redirected[page_id] = redirect_id
            pages_in_snapshot[norm_redirect_title] = redirect_id

    return pages_in_snapshot, pages_redirected, ids_redirected


def configure_subparsers(subparsers):
    """Configure a new subparser ."""
    parser = subparsers.add_parser(
        'match-id',
        help='Extract graph with page ID from link snapshots',
    )
    parser.add_argument(
        '--snapshot-dir',
        type=str,
        required=True,
        help='Snapshot file.'
    )
    parser.add_argument(
        '--delimiter',
        type=str,
        default='\t',
        help="Output CSV delimiter [default: '\\t']."
    )
    parser.add_argument(
        '--resolved-redirects',
        action='store_true',
        help="Snapshot files have also resolved redirects."
    )
    parser.add_argument(
        '--snapshot-filename-template',
        type=str,
        default='snapshot.{date}.csv.gz',
        help="Snapshot filename template [default: 'snapshot.{date}.csv.gz']."
    )
    parser.add_argument(
        '--skip-header',
        action='store_true',
        help="Skip input header."
    )
    parser.add_argument(
        '--skip-snapshot-header',
        action='store_true',
        help="Skip snapshot header."
    )
    parser.add_argument(
        '--titles',
        action='store_true',
        help="Output article titles in addition to ids."
    )
    parser.add_argument(
        '--trim-redirects',
        action='store_true',
        help="Do not out redirect links."
    )
    parser.add_argument(
        '--keep-duplicate-links',
        action='store_true',
        help="Keep duplicate links."
    )
    parser.add_argument(
        '--output-suffix',
        type=str,
        default='',
        help="Suffix to output name."
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
            'pages_analyzed': 0,
            'links_analyzed': 0,
        },
        'links': {
            'active': 0,
            'redirected': 0,
        },
    }
    stats['performance']['start_time'] = datetime.datetime.utcnow()

    if args.trim_redirects and not args.resolved_redirects:
        utils.log("Got --trim-redirect but no --resolved.redirects. "
                  "This is unexected. Exiting.")
        exit(1)

    match = basename_re.match(basename)
    year = int(match.group(1))
    month = int(match.group(2))
    day = int(match.group(3))

    date = arrow.Arrow(year, month, day)

    snapshot_filename = str(os.path.join(args.snapshot_dir,
                                         args.snapshot_filename_template))
    snapshot_filename = snapshot_filename.format(
        date=date.format('YYYY-MM-DD'))

    snapshot_infile = fu.open_csv_file(snapshot_filename)
    snapshot_reader = csv.reader(snapshot_infile)
    if args.skip_snapshot_header:
        next(snapshot_reader)

    pages_in_snapshot, pages_redirected, ids_redirected = (
        read_snapshot(reader=snapshot_reader,
                      resolved_redirects=args.resolved_redirects)
        )

    if args.dry_run:
        pages_output = open(os.devnull, 'wt')
    else:
        outname = ('wikilink_graph{suffix}.{{date}}.csv'
                   .format(suffix=args.output_suffix)
                   )
        filename = str(args.output_dir_path/(outname))
        filename = filename.format(date=date.format('YYYY-MM-DD'))
        stats_filename = str(args.output_dir_path /
                       ('wikilink_graph.{date}.stats.xml'))
        stats_filename = stats_filename.format(date=date.format('YYYY-MM-DD'))

        pages_output = fu.output_writer(
            path=filename,
            compression=args.output_compression,
        )
        stats_output = fu.output_writer(
            path=stats_filename,
            compression=args.output_compression,
        )

    writer = csv.writer(pages_output, delimiter=args.delimiter)

    dump = csv.reader(dump)

    if args.skip_header:
        next(dump)

    pages_generator = process_lines(
        dump,
        stats,
        pages_in_snapshot=pages_in_snapshot,
        pages_redirected=pages_redirected,
        ids_redirected=ids_redirected,
        add_titles=args.titles,
        trim_redirects=args.trim_redirects
        )

    if args.titles:
        writer.writerow(csv_header_output_titles)
    else:
        writer.writerow(csv_header_output_notitles)

    pages = pages_generator
    if not args.keep_duplicate_links:
        utils.log("Deduplicating output")
        pages = sorted(set([(page_from, page_to)
                            for page_from, page_to in pages_generator]))

    for page_from, page_to in pages:
        if args.titles:
            writer.writerow((
                page_from.id,
                page_from.title,
                page_to.id,
                page_to.title
            ))
        else:
            writer.writerow((
                page_from.id,
                page_to.id
            ))

    stats['performance']['end_time'] = datetime.datetime.utcnow()

    with stats_output:
        dumper.render_template(
            stats_template,
            stats_output,
            stats=stats,
        )
