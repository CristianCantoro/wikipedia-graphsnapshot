"""
Resolve redirects in a snapshot.

The output format is csv.
"""

import os
import csv
import glob
import mwxml
import arrow
import regex as re
import pathlib
import jsonable
import datetime
import functools
import collections
from typing import Iterable, Iterator, Mapping, NamedTuple

import more_itertools

from .. import utils
from .. import file_utils as fu
from .. import dumper


NLINES = 10000

DATE_START = arrow.get('2001-01-16', 'YYYY-MM')
DATE_NOW = arrow.now()


# example filenames:
#   * enwiki-20150901-pages-meta-history1.xml-p000000010p000002861.7z.features.xml.gz
chunk_regex =  r'([a-z]{2})wiki-(\d{8})'
chunk_regex += r'-pages-meta-history(\d{1,2})\.xml'
chunk_regex += r'-p(\d+)p(\d+)\.(gz|bz2|7z)'
chunk_regex += r'\.features\.xml(\.[^\.]+)(\.(gz|bz2|7z))?'
re_chunk = re.compile(chunk_regex, re.IGNORECASE | re.DOTALL)

# new chunk glob
#   * enwiki-20180301-pages-meta-history1.xml-p10p2115.7z.features.xml.gz
#     {lang}wiki-{date}-pages-meta-history{historyno}.xml
#       -p{pageid_first}p{pageid_last}.{dumpext}.features.xml.{ext}
new_chunk_pattern = '{lang}wiki-{date}'
new_chunk_pattern += '-pages-meta-history{historyno}.xml'
new_chunk_pattern += '-p{pageid_first}p{pageid_last}.{dumpext}'
new_chunk_pattern += '.features.xml.{ext}'


Revision = NamedTuple('Revision', [
    ('id', int),
    ('parent_id', int),
    ('timestamp', jsonable.Type),
    ('minor', bool),
])


Page = NamedTuple('Page', [
    ('id', str),
    ('title', str),
    ('revision', Revision),
])


# - Chunk:
#   - lang
#   - date
#   - historyno
#   - pageid_first
#   - pageid_last
#   - dumpext
#   - ext
Chunkfile = NamedTuple('Chunkfile', [
    ('lang', str),
    ('date', str),
    ('historyno', int),
    ('pageid_first', int),
    ('pageid_last', int),
    ('dumpext', str),
    ('ext', str),
])


csv_header_input = ('page_id',
                    'page_title',
                    'revision_id',
                    'revision_parent_id',
                    'revision_timestamp'
                    )


csv_header_output = csv_header_input + ('redirect_id',
                                        'redirect_title',
                                        'redirect_revision_id',
                                        'redirect_revision_parent_id',
                                        'redirect_revision_timestamp'
                                        )


stats_template = '''
<stats>
    <performance>
        <start_time>${stats['performance']['start_time'] | x}</start_time>
        <end_time>${stats['performance']['end_time'] | x}</end_time>
        <redirects_analyzed>${stats['performance']['redirects_analyzed'] | x}</redirects_analyzed>
        <pages_analyzed>${stats['performance']['pages_analyzed'] | x}</pages_analyzed>
    </performance>
</stats>
'''


# def process_lines(
#         dump: Iterable[list],
#         stats: Mapping,
#         selected_chunks: Iterable[list],
#         ignore_newer: bool) -> Iterator[list]:
def process_lines(
        dump: Iterable[list],
        timestamps: Iterable[arrow.arrow.Arrow],
        stats: Mapping,
        only_last_revision: bool,
        skip_header: bool) -> Iterator[list]:
    """Assign each revision to the snapshot or snapshots to which they
       belong.
    """

    # skip header
    if skip_header:
        next(dump)

    header = csv_header

    old_page = None
    old_prevpage = None
    new_page = None
    new_prevpage = None

    old_rev_prev = None
    old_rev = None
    new_rev_prev = None
    new_rev = None

    old_is_last_rev = None
    new_is_last_rev = None

    break_flag = False

    i = 0
    page_revisions = []
    counter = 0

    # equivalent to
    # for revision in dump:
    while True:
        if break_flag:
            break

        old_rev_prev = revision
        old_rev = next(dump, None)

        if old_rev is None:
                old_rev = old_rev_prev
                old_is_last_revision = True

        # read the line in a StringIO object and parse it with the csv module
        try:
            old_rev_parsed = [l for l in csv.reader(StringIO(old_rev))][0]
        except (csv.Error, TypeError) as err:
            continue

        old_revdata = dict(zip(header, old_rev_parsed))
        old_page = Page(
            old_revdata['page_id'],
            old_revdata['page_title'],
            Revision(old_revdata['revision_id'],
                     old_revdata['revision_parent_id'],
                     arrow.get(old_revdata['revision_timestamp'])
                     ))

        if old_prevpage is None or \
                old_prevpage.id != old_page.id:
            # we are starting now or we have a new page
            counter = 0
            utils.log("Processing", dump_page.title)
            stats['performance']['pages_analyzed'] += 1

        if old_prevpage is None or \
                old_prevpage.revision.id != old_page.revision.id:
            stats['performance']['revisions_analyzed'] += 1

        if not old_is_last_rev and \
                (old_prevpage is None or old_prevpage.id == old_page.id):
            # it is not the last revision, futhermore two cases:
            #   * dump_prevpage is None: we are reading the first line of the
            #     dump file
            #   * dump_prevpage.id == dump_page.id we are reading a page whose
            #     is is the same as the previous one we read
            # so, either we just started or we are in the middle of the
            # history of a page. What we do is we just add the revision to the
            # list.

            counter = counter + 1

            if counter % NPRINTREVISION == 1:
                utils.dot()

            old_page_revisions.append(dump_page)
            old_prevpage = old_page

        else:
            # cases:
            #   * this is the last revision of the dump
            #     (is_last_revision is True)
            #   * we have changed to a new page (dump_prevpage is not None
            #     and dump_prevpage.id != dump_page.id)

            # sort all the revision by timestamp (they are not guaranted to be
            # ordered)
            sorted_revisions = sorted(page_revisions,
                                      key=lambda pg: pg.revision.timestamp)

            # if we only want the sorted_revisions list is limited to the
            # last element.

            # Note: don't try to bee to smart and think that one can skip
            # reading all revisions and just take the last that is encountered,
            # that is when page.id changes, i.e.
            #     when dump_prevpage.id != dump_page.id
            # because as said in the previous comment we are not assured that
            # all revisions will be in the correct order, so we still need to
            # collect them all, sort them and take the last one.
            if only_last_revision:
                sorted_revisions = [sorted_revisions[-1]]

            # we are not interested to the new page for the moment, put it in
            # the list
            dump_prevpage = dump_page
            page_revisions = [dump_page]

            i = 0
            j = 0
            prevpage = None
            while j < len(sorted_revisions):
                page = sorted_revisions[j]

                ct = page.revision.timestamp
                pt = prevpage.revision.timestamp if prevpage else EPOCH

                while i < len(timestamps):
                    ts = timestamps[i]

                    if not prevpage:
                        # page contains the first revision for this page

                        if ct > ts:
                            # the page did not exist at the time
                            # check another timestamp
                            # print("ct {} > ts {}".format(ct, ts))

                            i = i + 1
                            continue

                        else:
                            # ct <= ts
                            # check another revision
                            # print("ct {} <= ts {}".format(ct, ts))

                            # update step
                            prevpage = page
                            j = j + 1

                            if j < len(sorted_revisions):
                                break
                    else:

                        if pt > ts:
                            # check the other timestamps
                            # print("pt {} > ts {}" .format(pt, ts))

                            i = i + 1
                            continue

                        elif ct > ts:
                            # the previous revision is in the snapshot
                            # check another timestamp
                            # print("ct {} > ts {}".format(ct, ts))

                            i = i + 1

                            # print("{} -> {}".format(prevpage, ts), end='')
                            # print(" - j: {}".format(j))
                            yield (prevpage, ts)

                            continue

                        else:
                            # check another revision
                            # print("ct {} <= ts {}, pt {} <= ts {}"
                            #       .format(ct, ts, pt, ts))

                            # update step
                            prevpage = page
                            j = j + 1
                            if j < len(sorted_revisions):
                                break

                    if j >= len(sorted_revisions):
                        i = i + 1

                        # print("--- {} -> {}".format(page, ts), end='')
                        # print(" - j: {}".format(j))
                        yield (page, ts)

            if is_last_revision:
                break_flag = True

        if not is_last_revision:
            stats['performance']['link_analyzed'] += 1


def configure_subparsers(subparsers):
    """Configure a new subparser ."""
    def is_dir(value):
        path_value = pathlib.Path(value)
        if not path_value.is_dir():
            import argparse
            raise argparse.ArgumentTypeError(
                "{} is not a directory".format(value))

        return path_value


    parser = subparsers.add_parser(
        'compare-extractions',
        help='Compare extractions.',
    )
    parser.add_argument(
        '--new-extractions-dir',
        type=is_dir,
        help='Directory with the new extractions.'
    )

    parser.add_argument(
        '--old-extraction-date',
        default=None,
        help='Last date from new extraction [default: infer from filename].'
    )

    parser.add_argument(
        '--new-extraction-date',
        action='store_true',
        default=None,
        help='Last date from new extraction [default: infer from filename].'
    )

    parser.add_argument(
        '--ignore-newer',
        action='store_true',
        help='Ignore additions in the new extractions that are newer '
             'than the last date in the old extraction.'
    )

    parser.set_defaults(func=main)


def idtoint(pageid):
    return int(pageid.lstrip('0'))


def extract_name_elements(re_chunk_match):
    lang = re_chunk_match.group(1)
    date = re_chunk_match.group(2)
    historyno = idtoint(re_chunk_match.group(3))
    pageid_first = idtoint(re_chunk_match.group(4))
    pageid_last = idtoint(re_chunk_match.group(5))
    dumpext = re_chunk_match.group(6)
    ext = ''
    try:
        # ignore groups 7 and 8
        ext = re_chunk_match.group(9)
    except IndexError:
        pass

    return Chunkfile(lang,
                     date,
                     historyno,
                     pageid_first,
                     pageid_last,
                     dumpext,
                     ext)


def select_intervals(old_interval, new_intervals_list):
    old_start = old_interval[0]
    old_end = old_interval[1]

    intervals = set()
    for start, end in new_intervals_list:
        if (start <= old_start and end >= old_start) or \
                (start <= old_start and end >= old_start) or \
                (end >= old_end and start <= old_end):
            intervals.add((start, end))

    return intervals


def sort_chunks(chunk_path):
    chunk_basename = os.path.basename(chunk_path)
    match = re_chunk.match(chunk_basename)
    achunk = extract_name_elements(match)

    return achunk.pageid_first


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
        }
    }
    stats['performance']['start_time'] = datetime.datetime.utcnow()


    inputfile_full_path = [afile for afile in args.files
                           if afile.name == basename][0]

    new_extractions_dir = args.new_extractions_dir
    ignore_newer = args.ignore_newer

    if args.dry_run:
        pages_output = open(os.devnull, 'wt')
        stats_output = open(os.devnull, 'wt')
    else:
        filename = str(args.output_dir_path /
                       (basename + '.compare_extractions.features.csv'))
        pages_output = fu.output_writer(
            path=filename,
            compression=args.output_compression,
        )
        stats_output = fu.output_writer(
            path=str(args.output_dir_path/
                    (basename + '.compare_extractions.stats.xml')),
            compression=args.output_compression,
        )
    writer = csv.writer(pages_output)

    base_match = re_chunk.match(basename)
    if base_match:
        base_chunk = extract_name_elements(base_match)
    else:
        msg = ("Unexpected name for input file: {filename} ({path})"
               .format(filename=basename, path=inputfile_full_path))
        raise ValueError(msg)
        del msg

    if args.old_extraction_date is None:
        old_extraction_date = arrow.get(base_chunk.date, 'YYYYMMDD')
    else:
        old_extraction_date = arrow.get(args.old_extraction_date)

    assert (old_extraction_date > DATE_START and \
                old_extraction_date < DATE_NOW)

    # {lang}wiki-{date}-pages-meta-history{historyno}.xml
    #   -p{pageid_first}p{pageid_last}.{dumpext}.features.xml.{ext}
    new_chunk_glob = (new_chunk_pattern
                      .format(lang=base_chunk.lang,
                               date='*',
                               historyno='*',
                               pageid_first='*',
                               pageid_last='*',
                               dumpext='*',
                               ext='*')
                      )

    new_chunks_ids = []
    new_chunks_dates = set()
    for cf in glob.glob(os.path.join(new_extractions_dir.as_posix(),
                                     new_chunk_glob
                                     )):

        cf_basename = os.path.basename(cf)
        new_match = re_chunk.match(cf_basename)
        if new_match:
            new_chunk = extract_name_elements(new_match)
            new_chunks_ids.append((new_chunk.pageid_first,
                                  new_chunk.pageid_last)
                                 )
            new_chunks_dates.add(new_chunk.date)
        else:
            msg = ("Unexpected name for new chunk: {filename} ({path})"
                   .format(filename=cf_basename,
                           path=cf))
            raise ValueError(msg)
            del msg
    new_chunks_ids.sort()

    if args.new_extraction_date is None:
        if len(new_chunks_dates) > 1:
            msg = ("Multiple dates extracted from new chunks filenames")
            raise ValueError(msg)
            del msg

        new_date = new_chunks_dates.pop()
        new_extraction_date = arrow.get(new_date, 'YYYYMMDD')
    else:
        new_extraction_date = arrow.get(args.new_extraction_date)

    assert (new_extraction_date > DATE_START and \
                new_extraction_date < DATE_NOW)

    assert ( new_extraction_date > old_extraction_date )

    selected_intervals = select_intervals(
        (base_chunk.pageid_first, base_chunk.pageid_last),
        new_chunks_ids
        )

    new_chunk_glob = (new_chunk_pattern
                      .format(lang=base_chunk.lang,
                               date=new_extraction_date.format('YYYYMMDD'),
                               historyno='*',
                               pageid_first='*',
                               pageid_last='*',
                               dumpext='*',
                               ext='*')
                      )

    selected_chunks = set()
    for cf in glob.glob(os.path.join(new_extractions_dir.as_posix(),
                                     new_chunk_glob
                                     )):
        cf_basename = os.path.basename(cf)
        new_match = re_chunk.match(cf_basename)
        if new_match:
            new_chunk = extract_name_elements(new_match)
            if (new_chunk.pageid_first, new_chunk.pageid_last) in \
                    selected_intervals:
                selected_chunks.add(cf)

    selected_chunks = sorted(selected_chunks, key=sort_chunks)
    import ipdb; ipdb.set_trace()

    dump = csv.reader(dump)
    pages_generator = process_lines(
        dump,
        stats,
        selected_chunks=selected_chunks,
        ignore_newer=ignore_newer
        )

    writer.writerow(csv_header_output)
    for page in pages_generator:
        # csv_header_output
        #
        # page_id
        # page_title
        # revision_id
        # revision_parent_id
        # revision_timestamp
        # redirect_id
        # redirect_title
        # redirect_revision_id
        # redirect_revision_parent_id
        # redirect_revision_timestamp
        writer.writerow(page)

    stats['performance']['end_time'] = datetime.datetime.utcnow()

    with stats_output:
        dumper.render_template(
            stats_template,
            stats_output,
            stats=stats,
        )
