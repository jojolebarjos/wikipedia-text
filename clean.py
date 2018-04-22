# -*- coding: utf-8 -*-


import os
import io
import regex as re
import unidecode
from tqdm import tqdm


# Character simplification, based on Unidecode
def unidecode_char(char):
  codepoint = ord(char)
  if codepoint < 0x80:
    return char
  if codepoint > 0xeffff:
    return ''
  if 0xd800 <= codepoint and codepoint <= 0xdfff:
    return ''
  section = codepoint >> 8
  position = codepoint % 256
  try:
    table = unidecode.Cache[section]
  except KeyError:
    try:
      mod = __import__('unidecode.x%03x' % section, globals(), locals(), ['data'])
      unidecode.Cache[section] = table = mod.data
    except ImportError:
      unidecode.Cache[section] = table = None
  if table and len(table) > position:
    return table[position]
  return ''


# Replace Unidecode's behavior for some special characters
NORMALIZED_OVERRIDDEN_CHARACTERS = {
  
  # Symbols
  '\u00ab' : '"', # <<
  '\u00bb' : '"', # >>
  '\u2030' : '%', # %0
  '\u2031' : '%', # %00
  '\u2191' : '',  # /
  
  # Grave
  'À' : 'À',
  'à' : 'à',
  'È' : 'È',
  'è' : 'è',
  'Ì' : 'Ì',
  'ì' : 'ì',
  'Ò' : 'Ò',
  'ò' : 'ò',
  'Ù' : 'Ù',
  'ù' : 'ù',
  
  # Circumflex
  'Â' : 'Â',
  'â' : 'â',
  'Ê' : 'Ê',
  'ê' : 'ê',
  'Î' : 'Î',
  'î' : 'î',
  'Ô' : 'Ô',
  'ô' : 'ô',
  'Û' : 'Û',
  'û' : 'û',
  
  # Acute
  'Á' : 'Á',
  'á' : 'á',
  'É' : 'É',
  'é' : 'é',
  'Í' : 'Í',
  'í' : 'í',
  'Ó' : 'Ó',
  'ó' : 'ó',
  'Ú' : 'Ú',
  'ú' : 'ú',
  'Ý' : 'Ý',
  'ý' : 'ý',
  
  # Diaeresis
  'Ä' : 'Ä',
  'ä' : 'ä',
  'Ë' : 'Ë',
  'ë' : 'ë',
  'Ï' : 'Ï',
  'ï' : 'ï',
  'Ö' : 'Ö',
  'ö' : 'ö',
  'Ü' : 'Ü',
  'ü' : 'ü',
  'Ÿ' : 'Ÿ',
  'ÿ' : 'ÿ',
  
  # Miscellaneous diacritics
  'Ç' : 'Ç',
  'ç' : 'ç',
  'Ã' : 'Ã',
  'ã' : 'ã',
  'Ñ' : 'Ñ',
  'ñ' : 'ñ',
  'Õ' : 'Õ',
  'õ' : 'õ',
  
  # TODO other European languages
}

# Apply mapping to Unidecoded characters
NORMALIZED_MAPPED_CHARACTERS = [
  '',   #   0 - NUL
  '',   #   1 - SOH
  '',   #   2 - STX
  '',   #   3 - ETX
  '',   #   4 - EOT
  '',   #   5 - ENQ
  '',   #   6 - ACK
  '',   #   7 - BEL
  '',   #   8 - BS
  ' ',  #   9 - HT
  '\n', #  10 - LF
  '',   #  11 - VT
  '',   #  12 - FF
  '',   #  13 - CR
  '',   #  14 - SO
  '',   #  15 - SI
  '',   #  16 - DLE
  '',   #  17 - DC1
  '',   #  18 - DC2
  '',   #  19 - DC3
  '',   #  20 - DC4
  '',   #  21 - NAK
  '',   #  22 - SYN
  '',   #  23 - ETB
  '',   #  24 - CAN
  '',   #  25 - EM
  '',   #  26 - SUB
  '',   #  27 - ESC
  '',   #  28 - FS
  '',   #  29 - GS
  '',   #  30 - RS
  '',   #  31 - US
  ' ',  #  32 - SP
  '!',  #  33 - !
  '"',  #  34 - "
  '#',  #  35 - #
  '$',  #  36 - $ TODO what about symbols that are encoded to text (e.g. EUR, deg...)? Maybe need to add whitespace
  '%',  #  37 - %
  '&',  #  38 - &
  "'",  #  39 - '
  '(',  #  40 - (
  ')',  #  41 - )
  '*',  #  42 - *
  '+',  #  43 - +
  ',',  #  44 - ,
  '-',  #  45 - -
  '.',  #  46 - .
  '/',  #  47 - /
  '0',  #  48 - 0
  '1',  #  49 - 1
  '2',  #  50 - 2
  '3',  #  51 - 3
  '4',  #  52 - 4
  '5',  #  53 - 5
  '6',  #  54 - 6
  '7',  #  55 - 7
  '8',  #  56 - 8
  '9',  #  57 - 9
  ':',  #  58 - :
  ';',  #  59 - ;
  '<',  #  60 - <
  '=',  #  61 - =
  '>',  #  62 - >
  '?',  #  63 - ?
  '@',  #  64 - @
  'A',  #  65 - A
  'B',  #  66 - B
  'C',  #  67 - C
  'D',  #  68 - D
  'E',  #  69 - E
  'F',  #  70 - F
  'G',  #  71 - G
  'H',  #  72 - H
  'I',  #  73 - I
  'J',  #  74 - J
  'K',  #  75 - K
  'L',  #  76 - L
  'M',  #  77 - M
  'N',  #  78 - N
  'O',  #  79 - O
  'P',  #  80 - P
  'Q',  #  81 - Q
  'R',  #  82 - R
  'S',  #  83 - S
  'T',  #  84 - T
  'U',  #  85 - U
  'V',  #  86 - V
  'W',  #  87 - W
  'X',  #  88 - X
  'Y',  #  89 - Y
  'Z',  #  90 - Z
  '[',  #  91 - [
  '\\', #  92 - \
  ']',  #  93 - ]
  '^',  #  94 - ^
  '_',  #  95 - _
  '`',  #  96 - `
  'a',  #  97 - a
  'b',  #  98 - b
  'c',  #  99 - c
  'd',  # 100 - d
  'e',  # 101 - e
  'f',  # 102 - f
  'g',  # 103 - g
  'h',  # 104 - h
  'i',  # 105 - i
  'j',  # 106 - j
  'k',  # 107 - k
  'l',  # 108 - l
  'm',  # 109 - m
  'n',  # 110 - n
  'o',  # 111 - o
  'p',  # 112 - p
  'q',  # 113 - q
  'r',  # 114 - r
  's',  # 115 - s
  't',  # 116 - t
  'u',  # 117 - u
  'v',  # 118 - v
  'w',  # 119 - w
  'x',  # 120 - x
  'y',  # 121 - y
  'z',  # 122 - z
  '{',  # 123 - {
  '|',  # 124 - |
  '}',  # 125 - }
  '~',  # 126 - ~
  ''    # 127 - DEL
]


# Normalized character set
NORMALIZED_CHARACTERS = sorted(set(NORMALIZED_MAPPED_CHARACTERS).union(set(NORMALIZED_OVERRIDDEN_CHARACTERS.values())))


# Character simplification, with special rules for common symbols and diacritics used in European languages
def normalize_char(char):
  if char in NORMALIZED_OVERRIDDEN_CHARACTERS:
    return NORMALIZED_OVERRIDDEN_CHARACTERS[char]
  return ''.join([NORMALIZED_MAPPED_CHARACTERS[ord(c)] for c in unidecode_char(char)])


# Replace rare/non-latin characters by simplified/latin representation
_whitespace = re.compile('\s+', re.UNICODE)
def normalize(text):
  result = []
  for char in text:
    for c in normalize_char(char):
      result.append(c)
  result = ''.join(result)
  result = _whitespace.sub(' ', result)
  result = result.strip()
  return result


# Use simple rules to tokenize text
_token = re.compile(r'\s*((?:\p{L}|\d)+|.)', re.UNICODE)
def tokenize(text):
  pos = 0
  while True:
    match = _token.match(text, pos)
    if not match:
      break
    yield match.group(1)
    pos = match.end(1)


# Simplify tokens
_digits = re.compile(r'\d+', re.UNICODE)
def simplify(token):
  token = token.lower()
  token = _digits.sub('0', token)
  return token


# Normalize file
def to_normalized(input_path, output_path, min_length=100):
  with io.open(input_path, 'r', newline='\n', encoding='utf-8') as input_file:
    with io.open(output_path, 'w', newline='\n', encoding='utf-8') as output_file:
      for line in tqdm(input_file):
        line = normalize(line)
        if len(line) >= min_length:
          output_file.write(line)
          output_file.write('\n')


# Tokenize file
def to_tokens(input_path, output_path, min_tokens=10):
  with io.open(input_path, 'r', newline='\n', encoding='utf-8') as input_file:
    with io.open(output_path, 'w', newline='\n', encoding='utf-8') as output_file:
      for line in tqdm(input_file):
        tokens = []
        for token, start, end in tokenize(line):
          tokens.append(simplify(token))
        if len(tokens) >= min_tokens:
          output_file.write(' '.join(tokens))
          output_file.write('\n')

