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


stats_template = '''
<stats>
    <performance>
        <start_time>${stats['performance']['start_time'] | x}</start_time>
        <end_time>${stats['performance']['end_time'] | x}</end_time>
        <pages_analyzed>${stats['performance']['pages_analyzed'] | x}</pages_analyzed>
        <revisions_analyzed>${stats['performance']['revisions_analyzed'] | x}</revisions_analyzed>
    </performance>
    <snapshot>
        <links>${stats['snapshot']['links'] | x}</links>
        <revisions>${stats['snapshot']['revisions'] | x}</revisions>
    </snapshot>
</stats>
'''


def process_lines(
        dump: Iterable[list],
        stats: Mapping,
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

        stats['performance']['revisions_analyzed'] += 1

        # Split the line to get page id and revision id, if something goes
        # wrong we ignore that line.
        revmatch = splitline_re.match(linkline)
        if revmatch is not None:
            dump_page_id = int(revmatch.group(1))
            dump_page_revision_id = int(revmatch.group(2))
        else:
            continue
        # if the id is the same as the previous and we decided to skip the page
        # we can continue to the next linkine
        if skip_page and dump_prevpage_id == dump_page_id:
            dump_prevpage_id = dump_page_id
            continue

        # The code below code is executed when we encounter a new page id for
        # the first time.

        # Print page id
        if dump_prevpage == 0 or dump_prevpage_id != dump_page_id:
            utils.log("Processing page id {}".format(dump_page_id))
            stats['performance']['pages_analyzed'] += 1

        # If the page id is not in the set of the page ids contained in this
        # snapshot we set skip_page to true so that we skip it.
        if dump_page_id not in pages_in_snapshot:
            skip_page = True
            dump_prevpage_id = dump_page_id

            print(" -> skip", end='', file=sys.stderr, flush=True)
            continue
        else:
            # This page id is contained in this snapshot
            skip_page = False

            # This revision id is not contained in this snapshot, so we should
            # check another line.
            # Each snapshot should have at most one revision for each page.
            # It may happen that a page is in the snapshot because it
            # existed at the time, but it had no links in it.
            # See this case:
            # https://en.wikipedia.org/w/index.php\
            #   ?title=BoMis&direction=next&oldid=237880
            if dump_page_revision_id not in revisions_in_snapshot:
                dump_prevpage_id = dump_page_id
                continue
            else:

                # this linkline is from a page and a revision that is contained
                # in the snapshot
                try:
                    # We read the line a CSV reader and let it do the
                    # splitting work.
                    revcsv = [el
                              for el in csv.reader(io.StringIO(linkline))][0]
                except csv.Error:
                    dump_prevpage_id = dump_page_id
                    continue

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
                dump_page = Page(int(revcsv[0]),
                                 revcsv[1],
                                 Revision(int(revcsv[2]),
                                          int(revcsv[3])
                                          if revcsv[3] else None,
                                          revcsv[5],
                                          revcsv[6],
                                          revcsv[7],
                                          revcsv[8],
                                          arrow.get(revcsv[4]),
                                          Wikilink(revcsv[9],
                                                   revcsv[10],
                                                   revcsv[11],
                                                   int(revcsv[12]),
                                                   int(revcsv[13]),
                                                   )))

                # Print pagetitle for each diferent revision analyzed, that is
                # at most once.
                if dump_prevpage_revision_id != dump_page_revision_id:
                    # we print the page title in parenthesys
                    page_title = " ({})".format(dump_page.title)
                    print(page_title, end='', file=sys.stderr)
                    stats['snapshot']['revisions'] += 1

                # print a dot for each link analyzed
                utils.dot()

                wikilink = (fu.normalize_wikititle(dump_page.revision.wikilink.link)
                            .strip()
                            )
                wikilink = ' '.join(wikilink.split())

                active_link = 0
                if wikilink in pagetitles_in_snapshot:
                    active_link = 1

                yield (dump_page, active_link)
                stats['snapshot']['links'] += 1

                dump_prevpage_id = dump_page_id
                dump_prevpage_revision_id = dump_page_revision_id
                dump_prevpage = dump_page



def configure_subparsers(subparsers):
    """Configure a new subparser ."""
    parser = subparsers.add_parser(
        'extract-link-snapshot',
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
    parser.add_argument(
        '--skip-snapshot-header',
        action='store_true',
        help='Skip the snapshot file header line.'
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
        'snapshot': {
            'links': 0,
            'revisions': 0,
        },
    }
    stats['performance']['start_time'] = datetime.datetime.utcnow()

    date = arrow.get(args.date)

    snapshot_infile = fu.open_csv_file(args.snapshot_file)
    snapshot_reader = csv.reader(fu.open_csv_file(snapshot_infile))

    pages_in_snapshot = set()
    revisions_in_snapshot = set()
    pagetitles_in_snapshot = set()

    if args.skip_snapshot_header:
        next(snapshot_reader)
    for row_data in snapshot_reader:
        pages_in_snapshot.add(int(row_data[0]))
        pagetitles_in_snapshot.add(fu.normalize_wikititle(row_data[1]))
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
        stats,
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
            page.revision.timestamp.to('utc').strftime('%Y-%m-%dT%H:%M:%SZ'),
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

    stats['performance']['end_time'] = datetime.datetime.utcnow()

    with stats_output:
        dumper.render_template(
            stats_template,
            stats_output,
            stats=stats,
        )
