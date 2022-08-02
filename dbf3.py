import datetime
import decimal

# http://www.dbfree.org/webdocs/1-documentation/b-dbf_header_specifications.htm
# http://www.dbfree.org/webdocs/start.msp?XY=43916643&VK=c:\\dbw\\srv\\s1\\web\\webdocs\\1-Documentation&VM=*.*


class ReprMixin:
    def __repr__(self):
        return '%s(%s)' % (self.__class__.__name__, self.__dict__)


class FileWrapper(object):

    def __init__(self, fp, out=None):
        self.fp = fp
        self.out = out

    def read(self, n=None):
        return self.fp.read(n)

    def write(self, s):
        return self.out.write(s)

    def writetmp(self):
        self.write(self.type) # self.type = self.read(1)
        self.write_date(self.last_update) # self.last_update = self.read_date()
        self.write_uint(4, self.num_records) # self.num_records = self.read_uint(4)
        self.write_uint(2, self.pos_records) # self.pos_records = self.read_uint(2)
        self.write_uint(2, self.len_records) # self.len_records = self.read_uint(2)
        self.write_zero(16) # self.read_zero(16) # reserved
        self.write_zero(1) # self.flags = self.read_zero(1) # TODO: handle this
        self.write_zero(1) # self.code_page_mark = self.read_zero(1) # TODO: handle this
        self.write_zero(2) # self.read_zero(2) # reserved
        for field in self.fields:
            print(field)
            self.write_field_type(field)
        self.write(self.FIELD_ARRAY_TERMINATOR)
        # self.diff = self.len_records - record_len
        # # database (.dbc) file, information. If the first byte is 0x00, the
        # # file is not associated with a database. Therefore, database files
        # # always contain 0x00.
        self.write_zero(1) # self.read_zero(1)
        assert self.out.tell() == self.pos_records
        # self.fp.seek(self.pos_records + self.len_records * self.num_records)
        # rest = self.read()
        # assert not rest or rest == self.TABLE_TERMINATOR, rest
        for record in self:
            self.write_record(record)

    def write_field_type(self, field):
        self.write(field.name.encode() + b'\x00' * (11 - len(field.name))) # self.name = table.read(11).rstrip(b'\x00').decode()
        self.write(field.type.encode()) # field.type = self.read_str(1)
        self.write_zero(4) # field.displacement = self.read_zero(4) # TODO: handle this
        self.write_uint(1, field.length) # field.length = self.read_uint(1)
        self.write_uint(1, field.num_decimals) # field.num_decimals = self.read_uint(1)
        self.write_zero(1) # field.flags = self.read_zero(1) # TODO: handle this
        self.write_zero(4) # field.next = self.read_zero(4) # TODO: handle this
        self.write_zero(1) # field.step = self.read_zero(1) # TODO: handle this
        self.write_zero(8) # self.read_zero(8) # reserved

    def skip(self, n):
        self.fp.seek(self.fp.tell()+n)

    def read_uint(self, n):
        res = 0
        for x in reversed(self.read(n)):
            res <<= 8
            res |= x
        return res

    def write_uint(self, n, x):
        xs = []
        for _ in range(n):
            xs.append(x & 0xff)
            x >>= 8
        self.write(bytes(xs))

    def read_zero(self, n):
        res = self.read_uint(n)
        assert res == 0, res
        return res

    def write_zero(self, n):
        self.write(b'\x00' * n)

    def read_date(self):
        a, b, c = self.read(3)
        return datetime.date(1900 + a, b, c)

    def write_date(self, date):
        self.write_uint(1, date.year - 1900)
        self.write_uint(1, date.month)
        self.write_uint(1, date.day)

    def read_str(self, n):
        return self.read(n).decode()

    def read_bool_field(self, field):
        assert field.length == 1
        ch = self.read_str(field.length)
        if ch == ' ':
            return None
        if ch == 'F':
            return False
        if ch == 'T':
            return True
        raise Exception('Not a bool: %r' % ch)

    def write_bool_field(self, field, value):
        assert field.length == 1
        if value is None:
            self.write(b' ')
        elif value is False:
            self.write(b'F')
        elif value is True:
            self.write(b'T')
        else:
            raise Exception('Not a bool: %r' % value)

    def read_char_field(self, field):
        data = self.read(field.length)
        try:
            return data.decode().rstrip(' ')
        except UnicodeDecodeError:
            return data

    def write_char_field(self, field, value):
        assert field.length >= len(value)
        if isinstance(value, str):
            value = value.encode()
            self.write(value + b' ' * (field.length - len(value)))
        else:
            assert len(value) == field.length
            self.write(value)

    def read_date_field(self, field):
        assert field.length == 8
        try:
            s = self.read_str(field.length)
        except UnicodeDecodeError:
            return None
        try:
            return datetime.datetime.strptime(s, '%Y%m%d').date()
        except ValueError:
            assert not s.strip(), s
            return None

    def write_date_field(self, field, value):
        assert field.length == 8
        if value is None:
            self.write_date(b'\x00' * field.length)
        self.write(value.strftime('%Y%m%d').encode())

    def read_numeric_field(self, field):
        data = self.read_str(field.length).lstrip(' ')
        if field.num_decimals:
            return decimal.Decimal(data) if data else None
        return int(data) if data else None

    def write_numeric_field(self, field, value):
        if value is None:
            self.write(b' ' * field.length)
        else:
            value = str(value).encode()
            assert len(value) <= field.length
            value = b' ' * (field.length - len(value)) + value
            self.write(value)
        assert not field.num_decimals, (field, value)
        # XXX finish this
        # data = self.read_str(field.length).lstrip(' ')
        # if field.num_decimals:
        #     return decimal.Decimal(data) if data else None
        # return int(data) if data else None

    def read_field(self, field):
        if field.type == 'C':
            return self.read_char_field(field)
        elif field.type == 'D':
            return self.read_date_field(field)
        elif field.type == 'L':
            return self.read_bool_field(field)
        elif field.type == 'M':
            return self.read_numeric_field(field)
        elif field.type == 'N':
            return self.read_numeric_field(field)
        else:
            val = self.read(field.length)
            raise Exception((field, val))

    def write_field(self, field, value):
        if field.type == 'C':
            self.write_char_field(field, value)
        elif field.type == 'D':
            self.write_date_field(field, value)
        elif field.type == 'L':
            self.write_bool_field(field, value)
        elif field.type == 'M':
            self.write_numeric_field(field, value)
        elif field.type == 'N':
            self.write_numeric_field(field, value)
        else:
            raise Exception((field, value))

    def skip_field(self, field):
        self.read_zero(field) # TODO seek instead


class Table(FileWrapper, ReprMixin):

    SUPPORTED_TYPES = [
        b'\x03', # FoxBASE+/Dbase III plus, no memo
        b'\x83', # FoxBASE+/dBASE III PLUS, with memo
    ]

    FIELD_ARRAY_TERMINATOR = b'\x0d'
    TABLE_TERMINATOR = b'\x1a'

    def __init__(self, fp, out=None):
        super().__init__(fp, out)
        self.type = self.read(1)
        assert self.type in self.SUPPORTED_TYPES, self.type
        self.last_update = self.read_date()
        self.num_records = self.read_uint(4)
        self.pos_records = self.read_uint(2)
        self.len_records = self.read_uint(2)
        self.read_zero(16) # reserved
        self.flags = self.read_zero(1) # TODO: handle this
        self.code_page_mark = self.read_zero(1) # TODO: handle this
        self.read_zero(2) # reserved
        self.fields = []
        record_len = 1
        while self.fp.read(1) != self.FIELD_ARRAY_TERMINATOR:
            self.skip(-1)
            field = Field(self)
            record_len += field.length
            self.fields.append(field)
        self.diff = self.len_records - record_len
        # database (.dbc) file, information. If the first byte is 0x00, the
        # file is not associated with a database. Therefore, database files
        # always contain 0x00.
        self.read_zero(1)
        assert self.fp.tell() == self.pos_records
        self.fp.seek(self.pos_records + self.len_records * self.num_records)
        rest = self.read()
        assert not rest or rest == self.TABLE_TERMINATOR, rest
        # del self.fp

    def __iter__(self):
        self.fp.seek(self.pos_records)
        for _ in range(self.num_records):
            yield self.next_record()

    def next_record(self):
        deleted = self.read_str(1) # deleted bit
        assert deleted in ' *', deleted
        if self.diff:
            self.skip(self.diff)
        record = []
        for field in self.fields:
            record.append(self.read_field(field))
        return Record(record, deleted)

    def write_record(self, record):
        self.write(record.deleted.encode()) # deleted = self.read_str(1) # deleted bit
        # assert deleted in ' *', deleted
        if self.diff: # if self.diff:
            self.write_zero(self.diff) # self.skip(self.diff)
        assert len(self.fields) == len(record)
        for field, value in zip(self.fields, record):
            self.write_field(field, value) # record.append(self.read_field(field))

    def get_record(self, i):
        pos = self.pos_records + self.len_records * i
        self.fp.seek(pos)
        return self.next_record()


class Field(ReprMixin):

    def __init__(self, table):
        self.name = table.read(11).rstrip(b'\x00').decode()
        self.type = table.read_str(1)
        self.displacement = table.read_zero(4) # TODO: handle this
        self.length = table.read_uint(1)
        self.num_decimals = table.read_uint(1)
        self.flags = table.read_zero(1) # TODO: handle this
        self.next = table.read_zero(4) # TODO: handle this
        self.step = table.read_zero(1) # TODO: handle this
        table.read_zero(8) # reserved


class Record:

    def __init__(self, columns, deleted=None):
        self.columns = columns
        self.deleted = deleted

    def __len__(self):
        return len(self.columns)

    def __iter__(self):
        return iter(self.columns)
