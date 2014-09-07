#!/usr/bin/python
""" module implementing class to create and query books index """
# -*- coding: utf-8 -*-
import os, zipfile, sqlite3, glob, hashlib, tempfile, shutil, sys
class BookIndex(object):
    """ Main class for storing and accessing books database index"""
    def __init__(self):
        self.config = {}
        self.db_cursor = None
        self.db_conn = None
        self._index_recordname_mapping = None
        self._index_file_checksum = None
        self._load_settings()
        if self._check_index_valid():
            #"Re-using existing index"
            self._open_index_database()
        else:
            #"Creating index"
            self._create_index()
    def _load_settings(self):
        """ use configuration file """
        self.config['dataDir'] = os.path.join("data", "fb2.Flibusta.Net")
        self.config['indexFile'] = os.path.join(
            self.config['dataDir'],
            "flibusta_fb2_local.inpx")
        self.config['tmpPath'] = os.path.join(tempfile.gettempdir(), 'flishell')
        self.config['dbPath'] = os.path.join("data", "db")
        self.config['extractPath'] = os.path.join("data", "books")
        self._index_recordname_mapping = \
            {0:'author', 1:'genre', 2:'title', 5:'bookid', 6:'size',\
            9:'format', 11:'language', 10:'date_added'}
    def _unpack_index_file(self):
        """method to unpack index file to a temporary path"""
        index_archive = zipfile.ZipFile(self.config['indexFile'])
        try:
            os.mkdir(self.config['tmpPath'])
        except:
            pass
        index_archive.extractall(self.config['tmpPath'])
    def _open_index_database(self):
        """ method to open index database file """
        if self.db_conn:
            return self.db_conn
        try:
            os.mkdir(self.config['dbPath'])
        except:
            pass
        db_conn = sqlite3.connect(
            os.path.join(self.config['dbPath'], 'fliShellIndex.db'))
        db_conn.text_factory = unicode
        db_conn.row_factory = sqlite3.Row
        self.db_conn = db_conn
    def _create_index_database(self):
        """ method to create index database file """
        try:
            os.mkdir(self.config['dbPath'])
        except:
            pass
        self._open_index_database()
        cursor = self._get_db_cursor()
        cursor.execute('''drop table if exists book_index''')
        cursor.execute('''drop table if exists book_search''')
        cursor.execute('''create table book_index(id int,\
            author text collate nocase, genre text,title text collate nocase,\
            archivefile text,format text, language text,\
            date_added text,size int)''')
        cursor.execute('''create table if not exists settings(name text,\
            value text, unique(name) on conflict replace )''')
        cursor.execute('''CREATE VIRTUAL TABLE if not exists book_search\
            USING fts4(id,author,title,language)''')
    def _get_index_files_list(self):
        """ get index files list iterator """
        index_files_iter = glob.iglob(
            os.path.join(self.config['tmpPath'], '*.inp'))
        return index_files_iter
    def _get_parsed_index_file_data(self, index_file):
        """  get parsed index file metadata """
        with open(os.path.join(self.config['tmpPath'], index_file)) as infile:
            while 1:
                line = unicode(infile.readline(),'utf-8')
                if not line:
                    break
                book_metadata = self._parse_file_metadata(line)
                yield book_metadata
    def _map_field_indexes_to_names(self, input_array):
        """ method to map metadata from parsed index file to python dict """
        output_record = {}
        for i in self._index_recordname_mapping.keys():
            output_record[self._index_recordname_mapping[i]] = input_array[i]
        return output_record
    def _parse_file_metadata(self, line):
        """ Method to parse input record for a book.
        Returns parsed fields as dict
        """
        fields = line.split('\x04')
        book_metadata = self._map_field_indexes_to_names(fields)
        return book_metadata
    def _db_fill_metadata(self, index_filename, book_metadata):
        """ Method to store book metadata into database"""
        mdt = book_metadata
        query = '''insert into book_index (id,author,genre,title,archivefile,
            format,language,date_added,size) values (?,?,?,?,?,?,?,?,?)'''
        query_data = (
            mdt['bookid'],
            mdt['author'].replace(',', ' ').replace(':', '').rstrip(),
            mdt['genre'].replace(':', ''),
            mdt['title'],
            os.path.basename(index_filename)[:-4]+".zip",
            mdt['format'],
            mdt['language'],
            mdt['date_added'],
            mdt['size'])
        cursor = self._get_db_cursor()
        try:
            cursor.execute(query, query_data)
        except Exception:
            sys.stderr.write(query+":".join(query_data)+"\n")
            raise
        query = '''insert into book_search (id,author,title,language)\
            values (?,?,?,?)'''
        query_data = (
            mdt['bookid'],
                mdt['author'].replace(',', ' ').replace(':', '').rstrip().upper(),
		mdt['title'].upper(), mdt['language'])
        try:
            cursor.execute(query, query_data)
        except Exception:
            print query, ":".join(query_data)
            raise
    def _create_index(self):
        """ method to create complete books index """
        self._create_index_database()
        self._unpack_index_file()
        try:
            for idx_file in self._get_index_files_list():
                print "Processing %s" % (idx_file)
                book_metadata = self._get_parsed_index_file_data(idx_file)
                for data in book_metadata:
                    self._db_fill_metadata(idx_file, data)
        finally:
            shutil.rmtree(self.config['tmpPath'])
        self._set_indexfile_checksum_db()
        self.db_conn.commit()
    def _create_fulltext_index(self):
        """ method to create full-text search index """
        cursor = self._get_db_cursor()
        cursor.execute('''CREATE VIRTUAL TABLE book_search \
            USING fts4(id,author,title,language)''')
        cursor.execute('''insert into book_search \
            select id,title,author,language from book_index''')
    def _get_indexfile_cheksum_db(self):
        """ Return checksum of the main index file """
        self._index_file_checksum = hashlib.md5(
            self.config['indexFile']
            ).hexdigest()
        return self._index_file_checksum
    def _get_index_file_cheksum_db(self):
        """ Return checksum of the main index file stored in database  """
        cursor = self._get_db_cursor()
        try:
            cursor.execute('select value from settings \
                where name="index_file_checksum"')
            return cursor.fetchone()[0]
        except:
            return None
    def _set_indexfile_checksum_db(self):
        """ Updates checksum of the main index file stored in database  """
        cursor = self._get_db_cursor()
        checksum = self._get_indexfile_cheksum_db()
        cursor.execute('''insert into settings (name,value) \
            values ('index_file_checksum',?)''', [checksum])
        self.db_conn.commit()
    def _check_index_valid(self):
        """ Checks of the main index file is the same from the previous run """
        if not os.path.exists(self.config['dbPath']):
            return False
        if self._get_indexfile_cheksum_db() != \
                self._get_index_file_cheksum_db():
            return False
        return True
    def _build_query(self, search_args):
        """ method to build search query from commandline args """
        query_cond = []
        for part in search_args.keys():
            query_part = "%s LIKE '%%%s%%'" % (part, search_args[part])
            query_cond.append(query_part)
        query = " AND ".join(query_cond)
        return query
    def _build_fulltext_query(self, search_args):
        """ method to build FTS search query from commandline args """
        query_cond = []
        for part in search_args.keys():
            query_part = "%s:%s" % (part, search_args[part])
            query_cond.append(query_part)
        query = " ".join(query_cond)
        return query
    def query_index(self, args, response_type=str):
        """ method to query database index """
        query = "select * from book_index where %s" % self._build_query(args)
        cursor = self._get_db_cursor()
        cursor.execute(query)
        for row in cursor.fetchall():
            if response_type == str:
                yield "%d [%s]: %s - %s" % ((row[0], row[6], row[1], row[3]))
            elif response_type == dict:
                yield dict(row)
    def query_fulltext_index(self, args, response_type=str):
        """ method to query database FTS index """
        query = "select id from book_search where book_search match '%s'\
            limit 1000" % self._build_fulltext_query(args)
        cursor = self._get_db_cursor()
        cursor.execute(query)
        ids = []
        for row in cursor.fetchall():
            ids.append(row[0])
        query = "select * from book_index where id in (%s)" % ",".join(ids)
        cursor.execute(query)
        for row in cursor.fetchall():
            if response_type == str:
                yield "%d [%s]: %s - %s" % ((row[0], row[6], row[1], row[3]))
            elif response_type == dict:
                yield dict(row)
    def _get_db_cursor(self):
        """ method to get db cursor """
        if not self.db_conn:
            self._open_index_database()
        if not self.db_cursor:
            self.db_cursor = self.db_conn.cursor()
        return self.db_cursor
    def extract_book(self, book_id):
        """ public method to extract book with user specified id"""
        cursor = self._get_db_cursor()
        cursor.execute(
            '''select id,format,archivefile,author,title,language \
             from book_index where id=%s'''%(book_id))
        query_result = cursor.fetchone()
        if not query_result:
            return None
        (book_id, book_format, archive_file, author, title, lang) = query_result
        book_archive = \
            zipfile.ZipFile(os.path.join(self.config['dataDir'], archive_file))
        book_archive.extract(
            str(book_id)+"."+book_format,
            self.config['extractPath'])
        src_book_name = str(book_id)+"."+book_format
        dst_book_name = ("%s - %s" % (author, title))\
            .replace(' ', '_')+"."+book_format
        os.rename(
            os.path.join(self.config['extractPath'], src_book_name),
            os.path.join(self.config['extractPath'], dst_book_name)
            )
        return dst_book_name

