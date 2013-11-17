#!/usr/bin/python
# -*- coding: utf-8 -*-

import os,sys,glob
import argparse
sys.path.append('lib')
import bookindex

parser = argparse.ArgumentParser(description='Query and extract books from Flibusta local mirror')
parser.add_argument('action', help='<search|extract>. Search or Extract book with specified id')
parser.add_argument('--author', help='set author to query. CASE SENSITIVE')
parser.add_argument('--title', help='set book title to query. CASE SENSITIVE')
parser.add_argument('--lang', help='set book language to query. CASE SENSITIVE')
parser.add_argument('--id', help='set book id to extract')

args = parser.parse_args()

bi=bookindex.BookIndex()
if args.action=='extract':
	bi.ExtractBook(args.id)
elif args.action=='search':	
	query={}
	if args.author:
		query['author']=unicode(args.author,'utf-8').upper()
	if args.title:
		query['title']=unicode(args.title,'utf-8').upper()
	if args.lang:
		query['language']=args.lang
	bi.QueryFTIndex(query)