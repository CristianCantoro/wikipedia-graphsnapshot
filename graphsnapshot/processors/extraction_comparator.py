"""
Compare extractions at two different dates.

The output format is csv.
"""

import os
import sys
import csv
import json
import glob
import mwxml
import arrow
import regex as re
import pathlib
import jsonable
import datetime
import itertools
import functools
import collections
from io import StringIO
from typing import (Iterable, Iterator, Mapping, NamedTuple, Optional)

import more_itertools

from .. import utils
from .. import file_utils as fu
from .. import dumper


NPRINTREVISION = 10000

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


# Page:
#   - page_id
#   - page_title
#   - Revision:
#     - revision_id
#     - revision_parent_id
#     - revision_timestamp
#     - revision_minor
#     - User:
#       - user_type
#       - user_username
#       - user_id
#     - Link:
#       - wikilink.link
#       - wikilink.tosection
#       - wikilink.anchor
#       - wikilink.section_name
#       - wikilink.section_level
#       - wikilink.section_number
Revision = NamedTuple('Revision', [
    ('id', int),
    ('parent_id', int),
    ('timestamp', jsonable.Type),
    ('minor', bool),
])


PageData = NamedTuple('Page', [
    ('id', str),
    ('title', str),
    ('revision', Revision),
    ('data', dict),
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


def progress(what: Optional[str]='.') -> None:
    print(what, end='', file=sys.stderr, flush=True)


def sort_revisions(rev):
    return (rev[1].revision.timestamp, rev[0])


def process_pages(dump: Iterable[list],
                  header: Iterable[list],
                  stats: Mapping,
                  only_last_revision: Optional[bool]=False,
                  which: Optional[str]='old') -> Iterator[list]:

    line = None
    prevline = None
    is_last_line = None

    page = None
    prevpage = None

    revisions = []
    sorted_revisions = None

    counter = 0
    lineno = 1

    break_flag = False

    while True:
        if break_flag:
            break

        prevline = line
        prevpage = page

        is_last_line = False
        line = next(dump, None)
        lineno = lineno + 1

        if line is None:
            line = prevline
            is_last_line = True

        # read the line in a StringIO object and parse it with the csv module
        try:
            parsed = [l for l in csv.reader(StringIO(line))][0]
        except (csv.Error, TypeError) as err:
            return None

        data = dict(zip(header, parsed))
        page = PageData(data['page_id'],
                        data['page_title'],
                        Revision(data['revision_id'],
                                 data['revision_parent_id'],
                                 arrow.get(data['revision_timestamp']),
                                 data['revision_minor'],
                                 ),
                        data
                        )

        if prevpage is None:
            if which == 'old':
                utils.log("Processing < {title} {{id:{id}}} "
                          .format(title=page.title, id=page.id))

            else:
                utils.log("Processing > {title} {{id:{id}}} "
                          .format(title=page.title, id=page.id))

        if prevpage is None or prevpage.id != page.id:
            # we are starting now or we have a new page
            counter = 0
            stats['pages_analyzed'] += 1

        if prevpage is None or \
                prevpage.revision.id != page.revision.id:
            stats['revisions_analyzed'] += 1

        if not is_last_line and \
                (prevpage is None or prevpage.id == page.id):
            # it is not the last revision, futhermore two cases:
            #   * prevpage is None: we are reading the first line of the
            #     dump file
            #   * dump_prevpage.id == dump_page.id we are reading a page whose
            #     is is the same as the previous one we read
            # so, either we just started or we are in the middle of the
            # history of a page. What we do is we just add the revision to the
            # list.

            counter = counter + 1

            if counter % NPRINTREVISION == 1:
                if which == 'old':
                    progress('.')
                else:
                    progress(':')

            revisions.append( (lineno, page) )
            prevpage = page

        else:
            # cases:
            #   * this is the last revision of the dump
            #     (is_last_line is True)
            #   * we have changed to a new page (dump_prevpage is not None
            #     and dump_prevpage.id != dump_page.id)

            # sort all the revision by timestamp (they are not guaranted to be
            # ordered)
            sorted_revisions = sorted(revisions, key=sort_revisions)

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

            yield sorted_revisions

            if prevpage.id != page.id:
                if which == 'old':
                    utils.log("Processing < {title} {{id:{id}}} "
                              .format(title=page.title, id=page.id))
                else:
                    utils.log("Processing > {title} {{id:{id}}} "
                              .format(title=page.title, id=page.id))

            # we are not interested to the new page for the moment, put it in
            # the list
            prevpage = page
            revisions = [ (lineno, page) ]


def get_header(dump: Iterable[list]) -> Iterable[list]:
    hline = next(dump)
    header = [l for l in csv.reader(StringIO(hline))][0]

    return header 


def compare_pages(old_hist: Iterable[list],
                  new_hist: Iterable[list],
                  header_old: Iterable[list],
                  header_new: Iterable[list]) -> Iterable[list]:

    utils.log('<{old} ({nrevold}), {new} ({nrevnew})> '
              .format(old=old_hist[0][1].title,
                      new=new_hist[0][1].title,
                      nrevold=len(old_hist),
                      nrevnew=len(new_hist)
                      )
              )

    i = 0
    j = 0

    equal_count = 0
    mod_count = 0
    add_count = 0
    sub_count = 0
    diff = []
    while (i < len(old_hist) or j < len(new_hist)):
        lineno, old = old_hist[i] if i < len(old_hist) else (None, None)
        lineno, new = new_hist[j] if j < len(new_hist) else (None, None)

        if old and i % NPRINTREVISION == 0:
            print('.', end='', file=sys.stderr, flush=True)
        if new and j % NPRINTREVISION == 0:
            print(':', end='', file=sys.stderr, flush=True)

        if old and new:
            if compare_data(old.data, new_data):
                # old == new:
                equal_count = equal_count + 1
                # --- if old and new and old == new

            else:
                # old != new:
                mod_count = mod_count + 1

                diff.append( ('-', lineno, old.data) )
                diff.append( ('+', lineno, new.data) )
                # --- if old and new and old != new

        if new and old is None:
            add_count = add_count + 1

            diff.append( ('+', lineno, new.data) )
            # --- if new and old is None

        if old and new is None:
            sub_count = sub_count + 1

            diff.append( ('-', lineno, old.data) )
            # --- if old and new is None

        i = i + 1
        j = j + 1

    # print string
    diff_string = ('={equal},~{mod},-{sub},+{add}'
                   .format(equal=equal_count,
                           mod=mod_count,
                           add=add_count,
                           sub=sub_count)
                   )
    print(' ({})'.format(diff_string), file=sys.stderr, flush=True)

    return diff


def process_dumps(
        old_dump: Iterable[list],
        stats: Mapping,
        selected_chunks: Iterable[list],
        ignore_newer_than: arrow.arrow.Arrow,
        header_old: Iterable[list],
        header_new: Iterable[list],
        only_last_revision: bool=False) -> Iterator[list]:
    """Compare revisions in `old_dump` with revisions from `selected_chunks`.
    """

    new_dump = itertools.chain.from_iterable([afile
                for afile in map(fu.open_csv_file, selected_chunks)])

    if header_old is None:
        header_old = get_header(old_dump)

    if header_new is None:
        header_new = get_header(new_dump)

    old_generator = process_pages(dump=old_dump,
                                  header=header_old,
                                  stats=stats['performance']['old'],
                                  only_last_revision=only_last_revision,
                                  which='old'
                                  )

    new_generator = process_pages(dump=new_dump,
                                  header=header_new,
                                  stats=stats['performance']['new'],
                                  only_last_revision=only_last_revision,
                                  which='new'
                                  )

    read_old = True
    read_new = True
    break_flag = False
    while True:
        if break_flag:
            break

        if read_old:
            oldpagehist = [(line, page) for line, page in next(old_generator)
                           if page.revision.timestamp <= ignore_newer_than]

        if read_new:
            newpagehist = [(line, page) for line, page in next(new_generator)
                           if page.revision.timestamp <= ignore_newer_than]

        old_pageid = oldpagehist[0][1].id
        new_pageid = newpagehist[0][1].id
        old_pagetitle = oldpagehist[0][1].title
        new_pagetitle = newpagehist[0][1].title

        if old_pageid < new_pageid:
            symbol = '<'

            read_new = False
            read_old = True

        elif old_pageid > new_pageid:
            symbol = '>'

            read_new = True
            read_old = False

        else:
            symbol = '=='

            read_new = True
            read_old = True

        if old_pageid != new_pageid:
            utils.log(('Do not compare '
                       '{old_title} ({old_id}) '
                       '{symbol} '
                       '{new_title} ({new_id})'
                       ).format(old_title=old_pagetitle,
                                old_id=old_pageid,
                                symbol=symbol,
                                new_title=new_pagetitle,
                                new_id=new_pageid,
                                )
                      )
        else:
            # compare_pages(old_hist, new_hist, header_old, header_new)
            difflist = compare_pages(old_hist=oldpagehist,
                                     new_hist=newpagehist,
                                     header_old=header_old,
                                     header_new=header_new
                                     )

            yield difflist


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
        required=True,
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

    parser.add_argument(
        '--header-old',
        type=str,
        help='List of comma-separated column names to compare from '
             'the old extraction '
             '[default: use all columns, reading from the first line of the'
             ' file, and compare only the ones that appear in both files].'
    )
    parser.add_argument(
        '--header-new',
        type=str,
        help='List of comma-separated column names to compare from '
             'the new extractions '
             '[default: use all columns, reading from the first line of the'
             ' file, and compare only the ones that appear in both files].'
    )
    parser.add_argument(
        '--only-last-revision',
        action='store_true',
        help='Consider only the last revision for each page'
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
            'old': {
                'pages_analyzed': 0,
                'revisions_analyzed': 0,
                'lines': 0
                },
            'new': {
                'pages_analyzed': 0,
                'revisions_analyzed': 0,
                'lines': 0
            }
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

    assert (new_extraction_date > old_extraction_date)

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

    header_old = args.header_old.split(',') if args.header_old else None
    header_new = args.header_new.split(',') if args.header_new else None

    ignore_newer_than = DATE_NOW
    if ignore_newer:
        ignore_newer_than = old_extraction_date

    difflist_generator = process_dumps(
        dump,
        stats,
        selected_chunks=selected_chunks,
        ignore_newer_than=ignore_newer_than,
        header_old=header_old,
        header_new=header_new,
        only_last_revision=args.only_last_revision,
        )

    writer = csv.writer(pages_output)
    for difflist in difflist_generator:
        for diff in difflist:
            change = diff[0]
            lineno = diff[1]
            data = json.dumps(diff[2])
            writer.writerow([change, lineno, data])

    stats['performance']['end_time'] = datetime.datetime.utcnow()

    with stats_output:
        dumper.render_template(
            stats_template,
            stats_output,
            stats=stats,
        )
