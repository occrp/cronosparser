NULL = b'\x00'

# Used to separate fields inside a record
RECORD_SEP = b'\x1e'

# True for all cronos databases?
ENC = 'cp1251'

# Found at: http://sergsv.narod.ru/cronos.htm
KOD = [
    0x08, 0x63, 0x81, 0x38, 0xa3, 0x6b, 0x82, 0xa6, 0x18, 0x0d, 0xac, 0xd5,
    0xfe, 0xbe, 0x15, 0xf6, 0xa5, 0x36, 0x76, 0xe2, 0x2d, 0x41, 0xb5, 0x12,
    0x4b, 0xd8, 0x3c, 0x56, 0x34, 0x46, 0x4f, 0xa4, 0xd0, 0x01, 0x8b, 0x60,
    0x0f, 0x70, 0x57, 0x3e, 0x06, 0x67, 0x02, 0x7a, 0xf8, 0x8c, 0x80, 0xe8,
    0xc3, 0xfd, 0x0a, 0x3a, 0xa7, 0x73, 0xb0, 0x4d, 0x99, 0xa2, 0xf1, 0xfb,
    0x5a, 0xc7, 0xc2, 0x17, 0x96, 0x71, 0xba, 0x2a, 0xa9, 0x9a, 0xf3, 0x87,
    0xea, 0x8e, 0x09, 0x9e, 0xb9, 0x47, 0xd4, 0x97, 0xe4, 0xb3, 0xbc, 0x58,
    0x53, 0x5f, 0x2e, 0x21, 0xd1, 0x1a, 0xee, 0x2c, 0x64, 0x95, 0xf2, 0xb8,
    0xc6, 0x33, 0x8d, 0x2b, 0x1f, 0xf7, 0x25, 0xad, 0xff, 0x7f, 0x39, 0xa8,
    0xbf, 0x6a, 0x91, 0x79, 0xed, 0x20, 0x7b, 0xa1, 0xbb, 0x45, 0x69, 0xcd,
    0xdc, 0xe7, 0x31, 0xaa, 0xf0, 0x65, 0xd7, 0xa0, 0x32, 0x93, 0xb1, 0x24,
    0xd6, 0x5b, 0x9f, 0x27, 0x42, 0x85, 0x07, 0x44, 0x3f, 0xb4, 0x11, 0x68,
    0x5e, 0x49, 0x29, 0x13, 0x94, 0xe6, 0x1b, 0xe1, 0x7d, 0xc8, 0x2f, 0xfa,
    0x78, 0x1d, 0xe3, 0xde, 0x50, 0x4e, 0x89, 0xb6, 0x30, 0x48, 0x0c, 0x10,
    0x05, 0x43, 0xce, 0xd3, 0x61, 0x51, 0x83, 0xda, 0x77, 0x6f, 0x92, 0x9d,
    0x74, 0x7c, 0x04, 0x88, 0x86, 0x55, 0xca, 0xf4, 0xc1, 0x62, 0x0e, 0x28,
    0xb7, 0x0b, 0xc0, 0xf5, 0xcf, 0x35, 0xc5, 0x4c, 0x16, 0xe0, 0x98, 0x00,
    0x9b, 0xd9, 0xae, 0x03, 0xaf, 0xec, 0xc9, 0xdb, 0x6d, 0x3b, 0x26, 0x75,
    0x3d, 0xbd, 0xb2, 0x4a, 0x5d, 0x6c, 0x72, 0x40, 0x7e, 0xab, 0x59, 0x52,
    0x54, 0x9c, 0xd2, 0xe9, 0xef, 0xdd, 0x37, 0x1e, 0x8f, 0xcb, 0x8a, 0x90,
    0xfc, 0x84, 0xe5, 0xf9, 0x14, 0x19, 0xdf, 0x6e, 0x23, 0xc4, 0x66, 0xeb,
    0xcc, 0x22, 0x1c, 0x5c,
]


def get_sentinel(text):
    """
    Returns bytes with text length and encoded text
    """
    encoded = text.encode(ENC)
    return bytes([len(encoded)]) + encoded


# Auto-generated and immutable: using this to find valid offsets for data.
PK_SENTINEL = get_sentinel('Системный номер')

# file names
BANK_DAT_FILE_NAME = 'CroBank.dat'
BANK_TAD_FILE_NAME = 'CroBank.tad'
STRU_DAT_FILE_NAME = 'CroStru.dat'
STRU_TAD_FILE_NAME = 'CroStru.tad'
