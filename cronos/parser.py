# coding: utf-8
import os
import struct
import csv
from itertools import count

from cronos.constants import KOD, PK_SENTINEL, RECORD_SEP, ENC


class CronosException(Exception):
    """General parsing errors."""


def vword(data):
    # A vodka word is a russian data unit, encompassing three bytes on good
    # days, with a flag in the fourth.
    word, = struct.unpack_from('<I', data)
    num = word & 0x00ffffff
    flags = (word & 0xff000000) >> 24
    return num, flags


def align_sections(data):
    # We don't know how to decode all of the CroStru file, so we're guessing
    # the offsets for particular sections which we can decipher. This is
    # done by applying a sliding window, and looking for a key phrase (i.e.
    # the russian string for the primary key column).
    bytes_ = [ord(b) for b in data]
    sections = []
    # guess the offset for each section by using a sentinel
    for offset in range(256):
        buf = []
        for i, byte in enumerate(bytes_):
            # this is from the web (CRO.H)
            # buf[i] = kod[buf[i]] - (unsigned char) i - (unsigned char) offset
            better_byte = (KOD[byte] - i - offset) % 256
            buf.append(better_byte)

        text = ''.join([chr(b) for b in buf])
        if PK_SENTINEL in text:
            sections.append({
                'text': text,
                'buf': buf,
                'offset': offset,
                'index': text.index(PK_SENTINEL)
            })
            # with open('%s.bin' % offset, 'wb') as fh:
            #     fh.write(text)
    sections = sorted(sections, key=lambda s: s['index'])
    return sections


def parse_columns(text, base, count):
    # Parse the columns from the table definition. Columns start with
    # a short record length indicator, followed by type and sequence
    # information (each a short), and the name (prefixed by the length).
    columns = []
    for i in range(count):
        col_len, = struct.unpack_from('H', text, base)
        base = base + 2
        col_data = text[base - 1:base - 1 + col_len]
        type_, col_id = struct.unpack_from('>HH', col_data, 0)
        text_len, = struct.unpack_from('>I', col_data, 4)
        col_name = col_data[8:8 + text_len].decode(ENC)
        columns.append({
            'id': col_id,
            'name': col_name,
            'type': type_
        })
        base = base + col_len
    return columns


def parse_table(text, next_byte):
    # Once we've guessed a table definition location, we can start
    # parsing the name; followed by the two-letter table abbreviation
    # and the count of columns.
    next_len = ord(text[next_byte])
    next_byte = next_byte + 1
    if len(text) < next_byte + next_len:
        return
    if ord(text[next_byte + next_len]) != 2:
        return
    # Get the table name.
    table_name = text[next_byte:next_byte + next_len].decode(ENC)
    next_byte = next_byte + next_len + 1
    # Get the table abbreviation.
    table_abbr = text[next_byte:next_byte + 2].decode(ENC)
    next_byte = next_byte + 2
    if ord(text[next_byte]) != 1:
        # raise CronosException('Table ID not ended by 0x01!')
        return
    next_byte = next_byte + 4
    # Get the number of columns for the table.
    col_count, = struct.unpack_from('I', text, next_byte)
    return {
        'name': table_name,
        'abbr': table_abbr,
        'columns': parse_columns(text, next_byte + 4, col_count),
        'column_count': col_count
    }


def parse_table_section(section, table_id):
    # Try and locate the beginning of a table definition using
    # some quasi-magical heuristics (i.e. the pattern of the
    # table definition).
    #
    # TABLE_ID + NULL + NULL + NULL + NAME_LEN + NAME
    # + 0x02 + ABBR1 + ABBR2 + 0x01 + NUM_TABLES
    text = section['text']
    sig = chr(table_id) + NULL + NULL + NULL
    offset = 0
    while True:
        index = text.find(sig, offset)
        if index == -1:
            break
        offset = index + 1
        next_byte = index + len(sig)
        table = parse_table(text, next_byte)
        if table is not None:
            table['id'] = table_id
            yield table


def parse_metadata(section):
    # Extract some nice-to-have metadata, such as the internal name
    # of the database and its ID.
    text = section['text']
    out = {}
    for field in ['BankId', 'BankName']:
        sentinel = field.encode(ENC)
        sentinel = chr(len(sentinel)) + sentinel
        index = text.index(sentinel)
        if index == -1:
            raise CronosException('Missing %s in structure!' % field)
        offset = index + len(sentinel)
        length, _ = vword(text[offset:])
        offset = offset + 4
        out[field] = text[offset:offset + length].decode(ENC)
    return out


def parse_structure(file_name):
    # The structure file holds metadata, such as table and column
    # definitions.
    with open(file_name, 'rb') as fh:
        data = fh.read()
    if not data.startswith('CroFile'):
        raise CronosException('Not a CroStru.dat file.')
    sections = align_sections(data)
    if not len(sections):
        raise CronosException('Could not recover CroStru.dat sections.')

    meta = parse_metadata(sections[0])

    tables = []
    for table_section in sections:
        for i in range(0, 256):
            for table in parse_table_section(table_section, i):
                tables.append(table)

    return meta, tables


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
        if not len(next_data):
            break
        next_offset, = struct.unpack_from('<I', next_data)
        data += next_data[4:]
        if next_length > 252:
            next_length -= 252
        else:
            next_length = 0
    return data


def parse_data(data_tad, data_dat, table_id):
    # This function uses the offsets present in the TAD file to extract
    # all records for the given ``table_id`` from the DAT file.
    tad_fh = open(data_tad, 'rb')
    dat_fh = open(data_dat, 'rb')

    # Check the file signature.
    sig = dat_fh.read(7)
    if sig != 'CroFile':
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
        if table_id != ord(record[0]):
            continue
        # First byte is the table ID
        record = record[1:]
        tuple_ = record.split(RECORD_SEP)
        # TODO: figure out how to detect password-encrypted columns.
        try:
            tuple_ = [c.decode(ENC) for c in tuple_]
        except UnicodeDecodeError:
            continue
        yield tuple_

    tad_fh.close()
    dat_fh.close()


def parse(db_folder, out_folder):
    """
    Parse a cronos database.

    Convert the database located in ``db_folder`` into CSV files in the
    directory ``out_folder``.
    """
    # The database structure, containing table and column definitions as
    # well as other data.
    stru_dat = os.path.join(db_folder, 'CroStru.dat')
    # Index file for the database, which contains offsets for each record.
    data_tad = os.path.join(db_folder, 'CroBank.tad')
    # Actual data records, can only be decoded using CroBank.tad.
    data_dat = os.path.join(db_folder, 'CroBank.dat')
    meta, tables = parse_structure(stru_dat)

    base_name = '%(BankName)s - ' % meta
    for table in tables:
        # TODO: do we want to export the "FL" table?
        if table['abbr'] == 'FL' and table['name'] == 'Files':
            continue
        fn = base_name + '%(abbr)s - %(name)s.csv' % table
        fn = os.path.join(out_folder, fn)
        fh = open(fn, 'w')
        columns = table.get('columns')
        writer = csv.writer(fh)
        writer.writerow([c['name'].encode('utf-8') for c in columns])
        for row in parse_data(data_tad, data_dat, table.get('id')):
            writer.writerow([c.encode('utf-8') for c in row])
            # for v, k in zip(row, columns):
            #     print v, k
        fh.close()
