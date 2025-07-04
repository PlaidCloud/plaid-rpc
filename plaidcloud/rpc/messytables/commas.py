import csv
import codecs
import re
import chardet

from .core import RowSet, TableSet, Cell, seekable_stream
from .error import ReadError

class UTF8Recoder:
    """
    Iterator that reads an encoded stream and re-encodes the input to UTF-8
    """

    # maps between chardet encoding and codecs bom keys
    BOM_MAPPING = {
        'utf-16le': 'BOM_UTF16_LE',
        'utf-16be': 'BOM_UTF16_BE',
        'utf-32le': 'BOM_UTF32_LE',
        'utf-32be': 'BOM_UTF32_BE',
        'utf-8': 'BOM_UTF8',
        'utf-8-sig': 'BOM_UTF8',

    }

    def __init__(self, f, encoding):
        sample = f.read(2000)
        if not encoding:
            results = chardet.detect(sample)
            encoding = results['encoding']
            if not encoding:
                # Don't break, just try and load the data with
                # a semi-sane encoding
                encoding = 'utf-8'
        f.seek(0)
        self.reader = codecs.getreader(encoding)(f, 'ignore')

        # The reader only skips a BOM if the encoding isn't explicit about its
        # endianness (i.e. if encoding is UTF-16 a BOM is handled properly
        # and taken out, but if encoding is UTF-16LE a BOM is ignored).
        # However, if chardet sees a BOM it returns an encoding with the
        # endianness explicit, which results in the codecs stream leaving the
        # BOM in the stream. This is ridiculously dumb. For UTF-{16,32}{LE,BE}
        # encodings, check for a BOM and remove it if it's there.
        if encoding.lower() in self.BOM_MAPPING:
            bom = getattr(codecs, self.BOM_MAPPING[encoding.lower()], None)
            if bom:
                # Try to read the BOM, which is a byte sequence, from
                # the underlying stream. If all characters match, then
                # go on. Otherwise when a character doesn't match, seek
                # the stream back to the beginning and go on.
                for c in bom:
                    if f.read(1) != c:
                        f.seek(0)
                        break

    def __iter__(self):
        return self

    def __next__(self):
        line = self.reader.readline()
        if not line or line == '\0':
            raise StopIteration
        result = line.encode("utf-8")
        return result

    next = __next__


def to_unicode_or_bust(obj, encoding='utf-8'):
    if isinstance(obj, bytes):
        obj = str(obj, encoding)
    return obj


class CSVTableSet(TableSet):
    """ A CSV table set. Since CSV is always just a single table,
    this is just a pass-through for the row set. """

    def __init__(self, fileobj, delimiter=None, quotechar=None, name=None,
                 encoding=None, window=None, doublequote=None,
                 lineterminator=None, skipinitialspace=None, **kw):
        super().__init__(fileobj)
        self.fileobj = seekable_stream(fileobj)
        self.name = name or 'table'
        self.delimiter = delimiter
        self.quotechar = quotechar
        self.encoding = encoding
        self.window = window
        self.doublequote = doublequote
        self.lineterminator = lineterminator
        self.skipinitialspace = skipinitialspace

    def make_tables(self):
        """ Return the actual CSV table. """
        return [CSVRowSet(self.name, self.fileobj,
                          delimiter=self.delimiter,
                          quotechar=self.quotechar,
                          encoding=self.encoding,
                          window=self.window,
                          doublequote=self.doublequote,
                          lineterminator=self.lineterminator,
                          skipinitialspace=self.skipinitialspace)]


class CSVRowSet(RowSet):
    """ A CSV row set is an iterator on a CSV file-like object
    (which can potentially be infinetly large). When loading,
    a sample is read and cached so you can run analysis on the
    fragment. """

    def __init__(self, name, fileobj, delimiter=None, quotechar=None,
                 encoding='utf-8', window=None, doublequote=None,
                 lineterminator=None, skipinitialspace=None):
        self.name = name
        seekable_fileobj = seekable_stream(fileobj)
        self.fileobj = UTF8Recoder(seekable_fileobj, encoding)

        def fake_ilines(fobj):
            for row in fobj:
                yield row.decode('utf-8')
        self.lines = fake_ilines(self.fileobj)
        self._sample = []
        self.delimiter = delimiter
        self.quotechar = quotechar
        self.window = window or 1000
        self.doublequote = doublequote
        self.lineterminator = lineterminator
        self.skipinitialspace = skipinitialspace
        try:
            for i in range(self.window):
                self._sample.append(next(self.lines))
        except StopIteration:
            pass
        super(CSVRowSet, self).__init__()

    @property
    def _dialect(self):
        delim = '\n'  # NATIVE
        sample = delim.join(self._sample)
        try:
            dialect = BetterSniffer().sniff(
                sample,
                delimiters='\t,;|'  # NATIVE
            )
            dialect.delimiter = str(dialect.delimiter)
            dialect.quotechar = str(dialect.quotechar)
            dialect.lineterminator = delim
            dialect.doublequote = True
            return dialect
        except csv.Error:
            return csv.excel

    @property
    def _overrides(self):
        # some variables in the dialect can be overridden
        d = {}
        if self.delimiter:
            d['delimiter'] = self.delimiter
        if self.quotechar:
            d['quotechar'] = self.quotechar
        if self.doublequote:
            d['doublequote'] = self.doublequote
        if self.lineterminator:
            d['lineterminator'] = self.lineterminator
        if self.skipinitialspace is not None:
            d['skipinitialspace'] = self.skipinitialspace
        return d

    def raw(self, sample=False):
        def rows():
            for line in self._sample:
                yield line
            if not sample:
                for line in self.lines:
                    yield line

        # Fix the maximum field size to something a little larger
        csv.field_size_limit(256000)

        try:
            for row in csv.reader(rows(),
                                  dialect=self._dialect, **self._overrides):
                yield [Cell(to_unicode_or_bust(c)) for c in row]
        except csv.Error as err:
            if 'newline inside string' in str(err) and sample:
                pass
            elif 'line contains NULL byte' in str(err):
                pass
            else:
                raise ReadError('Error reading CSV: %r', err)


class BetterSniffer(csv.Sniffer):

    def _guess_quote_and_delimiter(self, data, delimiters):
        """
        Looks for text enclosed between two identical quotes
        (the probable quotechar) which are preceded and followed
        by the same character (the probable delimiter).
        For example:
                         ,'some text',
        The quote with the most wins, same with the delimiter.
        If there is no quotechar the delimiter can't be determined
        this way.
        """

        matches = []
        for restr in (r'(?P<delim>[^\w\n"\'])(?P<space> ?)(?P<quote>["\'])[^\n]*?(?P=quote)(?P=delim)', # ,".*?",
                      r'(?:^|\n)(?P<quote>["\'])[^\n]*?(?P=quote)(?P<delim>[^\w\n"\'])(?P<space> ?)',   #  ".*?",
                      r'(?P<delim>[^\w\n"\'])(?P<space> ?)(?P<quote>["\'])[^\n]*?(?P=quote)(?:$|\n)',   # ,".*?"
                      r'(?:^|\n)(?P<quote>["\'])[^\n]*?(?P=quote)(?:$|\n)'):                            #  ".*?" (no delim, no space)
            regexp = re.compile(restr, re.DOTALL | re.MULTILINE)
            matches = regexp.findall(data)
            if matches:
                break

        if not matches:
            # (quotechar, doublequote, delimiter, skipinitialspace)
            return ('', False, None, 0)
        quotes = {}
        delims = {}
        spaces = 0
        groupindex = regexp.groupindex
        for m in matches:
            n = groupindex['quote'] - 1
            key = m[n]
            if key:
                quotes[key] = quotes.get(key, 0) + 1
            try:
                n = groupindex['delim'] - 1
                key = m[n]
            except KeyError:
                continue
            if key and (delimiters is None or key in delimiters):
                delims[key] = delims.get(key, 0) + 1
            try:
                n = groupindex['space'] - 1
            except KeyError:
                continue
            if m[n]:
                spaces += 1

        quotechar = max(quotes, key=quotes.get)

        if delims:
            delim = max(delims, key=delims.get)
            skipinitialspace = delims[delim] == spaces
            if delim == '\n': # most likely a file with a single column
                delim = ''
        else:
            # there is *no* delimiter, it's a single column of quoted data
            delim = ''
            skipinitialspace = 0

        # if we see an extra quote between delimiters, we've got a
        # double quoted format
        dq_regexp = re.compile(
                               r"((%(delim)s)|^)\W*%(quote)s[^%(delim)s\n]*%(quote)s[^%(delim)s\n]*%(quote)s\W*((%(delim)s)|$)" % \
                               {'delim':re.escape(delim), 'quote':quotechar}, re.MULTILINE)

        if dq_regexp.search(data):
            doublequote = True
        else:
            doublequote = False

        return (quotechar, doublequote, delim, skipinitialspace)
