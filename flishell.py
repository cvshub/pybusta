#!/usr/bin/python
""" Command-line utility to work with pybusta database """
# -*- coding: utf-8 -*-

import argparse
import lib.bookindex

def main():
    """ main """
    parser = argparse.ArgumentParser(description=\
        'Query and extract books from Flibusta local mirror')
    parser.add_argument('action',\
        help='<search|extract>. Search or Extract book with specified id')
    parser.add_argument('--author', help='set author to query. CASE SENSITIVE')
    parser.add_argument('--title',\
        help='set book title to query. CASE SENSITIVE')
    parser.add_argument('--lang',\
        help='set book language to query. CASE SENSITIVE')
    parser.add_argument('--id', help='set book id to extract')
    args = parser.parse_args()
    book_index = lib.bookindex.BookIndex()
    if args.action == 'extract':
        filename = book_index.extract_book(args.id)
        print filename
    elif args.action == 'search':
        query = {}
        if args.author:
            query['author'] = unicode(args.author, 'utf-8').upper()
        if args.title:
            query['title'] = unicode(args.title, 'utf-8').upper()
        if args.lang:
            query['language'] = args.lang
        for result in book_index.query_fulltext_index(query, response_type=str):
            print result
if __name__ == "__main__":
    main()
