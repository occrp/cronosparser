from itertools import count
import os
import struct

from .constants import (
    KOD,
    ENC,
    NULL, RECORD_SEP,
    get_sentinel,
    PK_SENTINEL,
    BANK_DAT_FILE_NAME, BANK_TAD_FILE_NAME,
    STRU_DAT_FILE_NAME
)


class CronosException(Exception):
    """General parsing errors."""


def vword(bytes_data, offset=0):
    # A vodka word is a russian data unit, encompassing three bytes on good
    # days, with a flag in the fourth.
    word, = struct.unpack_from('<I', bytes_data, offset=offset)
    num = word & 0x00ffffff
    flags = (word & 0xff000000) >> 24
    return num, flags


def decode(bytes_data):
    return bytes_data.decode(
        ENC,
        # TODO: do we need to return anything if some chars were not decoded?
        errors='replace',
    )


def align_sections(bytes_data):
    # We don't know how to decode all of the CroStru file, so we're guessing
    # the offsets for particular sections which we can decipher. This is
    # done by applying a sliding window, and looking for a key phrase (i.e.
    # the russian string for the primary key column).
    sections = []
    for offset in range(256):
        buf = []
        for i, byte in enumerate(bytes_data):
            # this is from the web (CRO.H)
            # buf[i] = kod[buf[i]] - (unsigned char) i - (unsigned char) offset
            better_byte = (KOD[byte] - i - offset) % 256
            buf.append(better_byte)

        bs = bytes(buf)
        pk_index = bs.find(PK_SENTINEL)
        if pk_index != -1:
            sections.append({
                'bytes': bs,
                'offset': offset,
                'pk_index': pk_index
            })
            # with open('%s.bin' % offset, 'wb') as fh:
            #     fh.write(text)
    sections.sort(key=lambda s: s['pk_index'])
    return sections


def parse_column(bytes_data, start_index):
    """
    Column structure is
    1. four bytes with column content length
    2. two bytes with column type
    3. two bytes with column id
    4. 4 bytes with length of column name
    5. bytes with column name
    """
    offset = start_index
    # unpack 1-4
    col_len, col_type, col_id, col_name_len = struct.unpack_from(
        (
            '>'  # big-endian unpack mode
            'I'  # unsigned int for column content length (1)
            'H'  # unsigned short for column type (2)
            'H'  # unsigned short for column id (3)
            'I'  # unsigned int for column name length (4)
        ),
        bytes_data,
        offset=offset,
    )
    offset += 4 + 2 + 2 + 4
    # unpack 5
    col_name = struct.unpack_from(
        f'{col_name_len}c',  # char bytes of column name (5)
        bytes_data,
        offset=offset,
    )
    # collect and decode column name
    col_name = decode(b''.join(col_name))
    return {
        'id': col_id,
        'type': col_type,
        'name': col_name,
        'start_index': start_index,
        'end_index': start_index + col_len,
    }


def iparse_columns(bytes_data, start_index):
    offset = start_index
    # columns start with unsigned int with number of columns
    col_count, = struct.unpack_from(
        '>I',
        bytes_data,
        offset=offset,
    )
    offset += 4
    for _ in range(col_count):
        column = parse_column(bytes_data, start_index=offset)
        yield column
        # two columns contents can't intersect
        # therefore we just move offset to column end
        offset = column['end_index']
        # there are two bytes between columns (idk what is it)
        offset += 2


def parse_table(bytes_data, start_index):
    """
    Table structure looks is
    1. table_id byte
    2. three nulls
    3. byte with length of the table name
    4. bytes with table name
    5. byte with 0x02 value
    6. two bytes of table abbreviation
    7. byte with 0x01 value
    """
    offset = start_index
    # unpack 1-3
    table_id, name_len = struct.unpack_from(
        (
            'B'  # unsigned char for table_id (1)
            'xxx'  # three nulls (2)
            'B'  # unsigned char for table name length (3)
        ),
        bytes_data,
        offset=offset,
    )
    # name can't be empty string
    if name_len == 0:
        return
    offset += 1 + 3 + 1
    # unpack 4-7
    *name, pad2, abbr1, abbr2, pad1 = struct.unpack_from(
        (
            f'>{name_len}c'  # char bytes of table name (4)
            'B'  # unsigned char for 0x02 value (5)
            '2c'  # two chars for table abbreviation (6)
            'B'  # unsigned char for 0x01 value (7)
        ),
        bytes_data,
        offset=offset,
    )
    # it looks like table but it's not
    if pad2 != 0x02 or pad1 != 0x01:
        return
    # collect and decode name and abbreviation
    name = decode(b''.join(name))
    abbr = decode(b''.join((abbr1, abbr2)))
    offset += name_len + 1 + 2 + 1

    columns = list(iparse_columns(bytes_data, start_index=offset))
    # table end index is last column end index
    end_index = (
        columns[-1]['end_index']
        if columns
        else offset
    )

    return {
        'id': table_id,
        'name': name,
        'abbr': abbr,
        'columns': columns,
        'start_index': start_index,
        'end_index': end_index,
    }


def iparse_tables(bytes_data):
    # every table has three nulls in a row
    to_find = NULL + NULL + NULL
    # start with offset 1 because table_id is located before three nulls
    offset = 1
    while True:
        index = bytes_data.find(to_find, offset)
        if index == -1:
            # there are no three nulls so there are no more tables
            break
        # don't forget about table_id byte before three nulls
        table_start_index = index - 1
        table = parse_table(bytes_data, start_index=table_start_index)
        # there can be three nulls in data but it's not a table
        if table is None:
            # just move offset to search again
            offset = index + 1
            continue

        yield table
        # two tables contents can't intersect
        # therefore we just move offset to table end
        offset = table['end_index']


def parse_metadata(bytes_data, fields=('BankId', 'BankName')):
    # Extract some nice-to-have metadata, such as the internal name
    # of the database and its ID.
    metadata = {}
    for field in fields:
        sentinel = get_sentinel(field)
        index = bytes_data.find(sentinel)
        if index == -1:
            # TODO: log that field was not found in structure
            continue

        offset = index + len(sentinel)
        length, _ = vword(bytes_data, offset=offset)
        offset += 4
        metadata[field] = decode(bytes_data[offset:offset + length])

    return metadata


def parse_structure(file_path):
    # The structure file holds metadata, such as table and column
    # definitions.
    with open(file_path, 'rb') as fh:
        data = fh.read()

    if not data.startswith(b'CroFile'):
        raise CronosException('Not a CroStru.dat file.')

    sections = align_sections(data)
    if not sections:
        raise CronosException('Could not recover CroStru.dat sections.')

    metadata = parse_metadata(sections[0]['bytes'])

    tables = [
        table
        for section in sections
        for table in iparse_tables(section['bytes'])
    ]

    return metadata, tables


def parse_record(meta, dat_fh):
    # Each data record is stored as a linked list of data fragments. The
    # metadata record holds the first and second offset, while all further
    # chunks are prefixed with the next offset.
    offset, length, next_offset, next_length = struct.unpack('<IHIH', meta)
    dat_fh.seek(offset)
    if length == 0:
        if next_length == 0 or next_length == 0xffff:
            return
    data = dat_fh.read(length)
    while next_length != 0 and next_length != 0xffff:
        dat_fh.seek(next_offset)
        next_data = dat_fh.read(min(252, next_length))
        if len(next_data) < 4:
            break
        next_offset, = struct.unpack_from('<I', next_data)
        data += next_data[4:]
        if next_length > 252:
            next_length -= 252
        else:
            next_length = 0
    return data


def iparse_records(data_tad, data_dat, table=None):
    """
    This function uses the offsets present in the TAD file to extract
    all records for the given ``table_id`` from the DAT file.

    `table` param is optional because structure can be not parsed
    """
    with open(data_tad, 'rb') as tad_fh, open(data_dat, 'rb') as dat_fh:
        # Check the file signature.
        sig = dat_fh.read(7)
        if sig != b'CroFile':
            raise CronosException('Not a CroBank.dat file.')

        # One day, we'll find out what this header means.
        tad_fh.seek(8)
        for i in count(1):
            meta = tad_fh.read(12)
            if len(meta) != 12:
                break
            record = parse_record(meta, dat_fh)
            if record is None or len(record) < 2:
                continue
            if table and table['id'] != record[0]:
                continue
            # First byte is the table ID
            record = record[1:]
            # TODO: figure out how to detect password-encrypted columns.
            record = [
                decode(value)
                for value in record.split(RECORD_SEP)
            ]
            if table:
                if len(record) != len(table['columns']):
                    record.insert(0, i)
                # TODO: convert values according to their column type

            yield record


def get_file(db_folder, file_name):
    """Glob for the poor."""
    if not os.path.isdir(db_folder):
        raise CronosException(f'`{db_folder}` is not a folder path')

    file_path = os.path.join(db_folder, file_name)
    if not os.path.exists(file_path):
        raise CronosException(f'File `{file_path}` not found')

    return file_path


def parse(db_folder):
    """
    Parse a cronos database located in ``db_folder``
    """
    # The database structure, containing table and column definitions as
    # well as other data.
    stru_dat = get_file(db_folder, STRU_DAT_FILE_NAME)
    # Index file for the database, which contains offsets for each record.
    data_tad = get_file(db_folder, BANK_TAD_FILE_NAME)
    # Actual data records, can only be decoded using CroBank.tad.
    data_dat = get_file(db_folder, BANK_DAT_FILE_NAME)

    metadata, tables = parse_structure(stru_dat)

    for table in tables:
        # TODO: how to parse Files table records?
        if table['abbr'] == 'FL' and table['name'] == 'Files':
            continue

        table['records'] = [
            # zip with columns?
            record
            for record in iparse_records(data_tad, data_dat, table=table)
        ]

    return metadata, tables
