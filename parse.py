#!/usr/bin/env python3

"""
General idea:

    - load selected logs from files into sqlite database
    - allow to specify precise data types for simple and reliable parsing
    - provide rich set of tools to query, analyze and visualize data
    - make it well documented and intuitive to use
"""

import os
import sys

import unittest
import argparse
import sqlite3 as sqlite

from IPython import embed

class ParseLineTestCase(unittest.TestCase):
    def test_parse_line(self):
        """
        """
        tokens = ['71.105.66.111', '71.105.66.111', '-', '-', '[13/Jun/2013:06:26:55 +0000]',
        '"GET /clothing/gottex-seychelles-floral-print-maillot-blue/ HTTP/1.1"',
        '200', '1228',
        '"http://www.google.com/url?sa=t&rct=j&q=&esrc=s&source=web&cd=10&ved=0CHMQFjAJ&url=http%3A%2F%2Fwww.lyst.com%2Fclothing%2Fgottex-seychelles-floral-print-maillot-blue%2F&ei=JWa5UcOOI8P5iwLdx4GQDw&usg=AFQjCNHLTz4yEFawzao8ZAoip7g_rhvF4w&bvm=bv.47883778,d.cGE"',
        '"Mozilla/5.0 (Windows NT 6.1; WOW64; rv:21.0) Gecko/20100101 Firefox/21.0"',
        '"US"', '-', '"0.251"', '-', '-', '-',
        '"http://www.lyst.com/clothing/gottex-seychelles-floral-print-maillot-blue/"',
        '"-"', '"-"',
        '"http://-//clothing/gottex-seychelles-floral-print-maillot-blue/"']
        expected = ['71.105.66.111', '71.105.66.111', '-', '-', '13/Jun/2013:06:26:55 +0000',
        'GET /clothing/gottex-seychelles-floral-print-maillot-blue/ HTTP/1.1',
        '200', '1228',
        'http://www.google.com/url?sa=t&rct=j&q=&esrc=s&source=web&cd=10&ved=0CHMQFjAJ&url=http%3A%2F%2Fwww.lyst.com%2Fclothing%2Fgottex-seychelles-floral-print-maillot-blue%2F&ei=JWa5UcOOI8P5iwLdx4GQDw&usg=AFQjCNHLTz4yEFawzao8ZAoip7g_rhvF4w&bvm=bv.47883778,d.cGE',
        'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:21.0) Gecko/20100101 Firefox/21.0',
        'US', '-', '0.251', '-', '-', '-',
        'http://www.lyst.com/clothing/gottex-seychelles-floral-print-maillot-blue/',
        '-', '-',
        'http://-//clothing/gottex-seychelles-floral-print-maillot-blue/']

        sample = ' '.join(tokens)
        self.assertEqual(parse_line(sample), expected)




def parse_line(line):
    line = line.strip()
    fields = []
    word = []
    stopchar = ' '
    mode = 'start'
    for c in line:
        if mode == 'start':
            if c == '"':
                stopchar = '"'
                mode = 'field'
            elif c == '[':
                stopchar = ']'
                mode = 'field'
            elif c == '-':
                word += c
                fields.append(''.join(word))
                word = []
                mode = 'start'
            elif c == ' ':
                mode = 'start'
            else:
                mode='field'
                word += c
        elif mode == 'field':
            if c == stopchar:
                fields.append(''.join(word))
                word = []
                mode = 'start'
                stopchar = ' '
            else:
                word += c



    return fields


def parse_lines(line_iter):
    for line in line_iter:
        yield parse_line(line)

def create_tables(conn):
    log_table_def = """
    CREATE TABLE logs(
        _lineno int,
        _file text,
        c1 text,
        c2 text,
        c3 text,
        c4 text,
        c5 text,
        c6 text,
        c7 text,
        c8 text,
        c9 text,
        c10 text,
        c11 text,
        c12 text,
        c13 text,
        c14 text,
        c15 text,
        c16 text,
        c17 text,
        c18 text,
        c19 text,
        c20 text,
        c21 text,
        c22 text,
        c23 text,
        c24 text

    )
    """
    cursor = conn.cursor()
    cursor.execute(log_table_def)
    cursor.close()
    conn.commit()


def main():
    """
    """
    parser = argparse.ArgumentParser(description='Process your logs')
    parser.add_argument('files', metavar='FILES', type=str, nargs='+',
                        help='log file names')
    args = parser.parse_args()
    conn = sqlite.connect(os.tmpnam()+'.sqlite3')
    cursor = conn.cursor()
    create_tables(conn)

    for fname in args.files:
        print('Processing: {0}'.format(fname))
        with open(fname) as f:
            for idx, parsed_line in enumerate(parse_lines(f)):
                vals = [idx, fname]
                cols = '_lineno, _file, '
                cols +=  ','.join(['c'+str(i+1) for i in range(len(parsed_line))])
                vals.extend(parsed_line)
                ins = 'insert into logs(' + cols +') values(?, ?'
                ins += ',?'*len(parsed_line) + ')'
                cursor.execute(ins, vals)
        conn.commit()
    
    conn.close()
    start_shell()

def start_shell():
    embed()

def run():
    main()
    

if __name__ == '__main__':
    run()
