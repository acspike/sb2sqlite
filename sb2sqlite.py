import sys
from struct import unpack
from glob import glob
import os.path
import sqlite3

def sbf_blocks(f):
    #read header block
    numrecs, numblocks, blocksize = unpack('<12xIIH38x',f.read(60))
    #read padding
    f.read(blocksize-60)
    #gather data blocks
    blocks = []
    while True:
        raw_header = f.read(4)
        if not raw_header:
            break
        block_header = unpack('<I',raw_header)[0]
        block_data = f.read(blocksize-4)
        first = bool(block_header & 0x80000000)
        deleted = bool(block_header & 0x40000000)
        next_block = block_header & 0x3FFFFFFF
        block = (first, deleted, next_block, block_data)
        blocks.append(block)
    return blocks

def sbf_records(blocks):
    #find first blocks
    firsts = [x for x in blocks if x[0] and not x[1]]
    #reassemble records
    raw_records = []
    for x in firsts:
        datas = []
        _,_,next_block,data = x
        datas.append(data)
        while next_block>0:
            _,_,next_block,data = blocks[next_block-1]
            datas.append(data)
        raw_records.append(''.join(datas))
    return raw_records

numcodes = {'\x02':'<H','\x04':'<I','\x08':'<d'}
def sbf_fields(raw_record, record_length):
    #parse record
    x=raw_record
    record = []
    cursor = 0
    while cursor < len(x) and len(record) < record_length:
        if x[cursor] == '\xff':
            raw_size = x[cursor+1]
            size = unpack('<B',raw_size)[0]
            num = unpack(numcodes[raw_size],x[cursor+2:cursor+2+size])[0]
            cursor = cursor + 2 + size
            record.append(num)
        else:
            end = x.find('\x00',cursor)
            s = x[cursor:end]
            cursor = end+1
            record.append(s)
    return record

def sbd_fields(f):
    sbd = f.read().split('\r\n')
    names = [x.split(';')[0].strip() for x in sbd[1:sbd.index('')] if x[0]!=' ']
    return names

def find_sbdf_pairs(fpath = '.'):
    pairs = {}
    for x in glob(os.path.join(fpath,'*.[Ss][Bb][DdFf]')):
        base = os.path.basename(x)
        fname, ext = os.path.splitext(base.lower())
        if not fname in pairs:
            pairs[fname] = {'.sbd':'','.sbf':''}
        pairs[fname][ext] = x
    return pairs

def parse_sbdf(sbd, sbf):
    fields = sbd_fields(open(sbd,'rb'))
    record_length = len(fields)
    records = []
    blocks = sbf_blocks(open(sbf,'rb'))
    raw_records = sbf_records(blocks)
    for x in raw_records:
        record = sbf_fields(x, record_length)
        records.append(record)
    return (fields, records)

typemap = {str:'text',int:'integer',float:'real'}
def create_table_from_record(name, fields, records):
    rec = records[0]
    for x in records:
        if len(x) == len(fields):
            rec = x 
            break
    statement = []
    for n,v in zip(fields, rec):
        statement.append('%s %s' % (n, typemap[type(v)]))
    return 'create table %s (%s)' % (name, ', '.join(statement))
def insert_from_record(name, fields):
    q = ['?'] * len(fields)
    return 'insert into %s values (%s)' % (name, ', '.join(q))


def superbase_to_sqlite(sbpath, sqlitefile):
    pairs = find_sbdf_pairs(sbpath)
    if not pairs:
        sys.stderr.write('Unable to locate Superbase data files\n')
        return
    conn = sqlite3.connect(sqlitefile)
    conn.text_factory = str
    c = conn.cursor()
    for t in pairs:
        if not (pairs[t]['.sbd'] and pairs[t]['.sbf']):
            continue
        fields, records = parse_sbdf(pairs[t]['.sbd'],pairs[t]['.sbf'])
        if not records:
            continue
        create_table = create_table_from_record(t, fields, records)
        c.execute(create_table)
        insert_statement = insert_from_record(t, fields)
        for x in records:
            c.execute(insert_statement, x+[None]*(len(fields)-len(x)))
    conn.commit()
    c.close()


if __name__ == '__main__':
    from optparse import OptionParser
    parser = OptionParser()
    parser.add_option('-f', '--file', dest='sqlitefile', default='superbase_in_sqlite.db',
                      help='SQLite database file to create')
    parser.add_option('-p', '--path', dest='sbpath', default='.',
                      help='path to search for Superbase data files')
    (options, args) = parser.parse_args()

    superbase_to_sqlite(options.sbpath, options.sqlitefile)
