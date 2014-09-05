#!/usr/bin/python
""" module implementing class to create and query books index """
# -*- coding: utf-8 -*-
import os, zipfile, sqlite3, glob, hashlib, tempfile, shutil
class BookIndex(object):
    """ Main class for storing and accessing books database index"""
    def __init__(self):
        self.config = {}
        self.db_cursor = None
        self.db_conn = None
        self._index_recordname_mapping = None
        self._load_settings()
        if self._check_index_valid():
            print "Re-using existing index"
            self._open_index_database()
        else:
            print "Creating index"
            self._create_index()
    def _load_settings(self):
        """ use configuration file """
        self.config['dataDir'] = os.path.join("data","fb2.Flibusta.Net")
        self.config['indexFile'] = os.path.join(self.config['dataDir'],\
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
        #TODO: cleanup TMPPATH
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
        db_conn = sqlite3.connect(os.path.join(self.config['dbPath'],\
            'fliShellIndex.db'))
        db_conn.text_factory = str
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
        """ _get_index_files_list """
        index_files_iter =\
            glob.iglob(os.path.join(self.config['tmpPath'],'*.inp'))
        return index_files_iter
    def _get_parsed_index_file_data(self, indexFile):
        """ _get_parsed_index_file_data """
        with open(os.path.join(self.config['tmpPath'],indexFile)) as infile:
            while 1:
                line = infile.readline()
                if not line: break
                book_metadata = self._parse_file_metadata(line)
                yield book_metadata
    def _map_field_indexes_to_names(self,inputArray):
        output_record = {}
        for i in self._index_recordname_mapping.keys():
            output_record[self._index_recordname_mapping[i]] = inputArray[i]
        return output_record
    def _parse_file_metadata(self,line):
        fields = line.split('\x04')
        book_metadata = self._map_field_indexes_to_names(fields)
        return book_metadata
    def _db_fill_metadata(self,indexFilename,book_metadata):
        md = book_metadata
        query = '''insert into book_index (id,author,genre,title,archivefile,
            format,language,date_added,size) values (?,?,?,?,?,?,?,?,?)'''
        query_data = (md['bookid'],\
            unicode(md['author'].replace(',', ' ').replace(':', '').rstrip(), 'utf-8'),
            md['genre'].replace(':', ''), unicode(md['title'], 'utf-8'),\
            os.path.basename(indexFilename)[:-4]+".zip", md['format'],\
            md['language'], md['date_added'], md['size'])
        cursor = self._get_db_cursor()
        try:
            cursor.execute(query,query_data)
        except Exception:
            print query,":".join(query_data)
            raise 
        query = '''insert into book_search (id,author,title,language)\
            values (?,?,?,?)'''
        query_data =\
            (md['bookid'],\
            unicode(md['author'].replace(',',' ').replace(':','').rstrip(),'utf-8').upper(),\
            unicode(md['title'],'utf-8').upper(), md['language'])
        try:
            cursor.execute(query, query_data)
        except Exception:
            print query, ":".join(query_data)
            raise 
    def _create_index(self):
        self._create_index_database()
        self._unpack_index_file()
        try:
            for idx_file in self._get_index_files_list():
                print "Processing %s" % (idx_file)
                book_metadata = self._get_parsed_index_file_data(idx_file)
                for data in book_metadata:
                    self._db_fill_metadata(idx_file,data)
        finally:
            shutil.rmtree(self.config['tmpPath'])
        self._set_indexfile_checksum_db()
        self.db_conn.commit()
    def _create_fulltext_index(self):
        self._get_db_cursor()
        cursor.execute('''CREATE VIRTUAL TABLE book_search USING fts4(id,author,title,language)''')
        cursor.execute('''insert into book_search select id,title,author,language from book_index''')
    def _get_indexfile_cheksum_db(self):
        self._indexFileChecksum = hashlib.md5(self.config['indexFile']).hexdigest()
        return self._indexFileChecksum
    def _get_index_file_cheksum_db(self):
        cursor = self._get_db_cursor()
        try:
            cursor.execute('select value from settings where name="index_file_checksum"')
            return cursor.fetchone()[0]
        except:
            return None
    def _set_indexfile_checksum_db(self):
        cursor = self._get_db_cursor()
        checksum = self._get_indexfile_cheksum_db()
        cursor.execute('''insert into settings (name,value) values ('index_file_checksum',?)''' , [checksum])
        self.db_conn.commit()
    def _check_index_valid(self):
        if not os.path.exists(self.config['dbPath']): return False
        if self._get_indexfile_cheksum_db() != self._get_index_file_cheksum_db(): return False
        return True
    def _build_query(self, searchArgs):
        query_cond = []
        for p in searchArgs.keys(): 
            query_part = "%s LIKE '%%%s%%'" % (p, searchArgs[p])
            query_cond.append(query_part)
        query = " AND ".join(query_cond)    
        return query
    def _build_fulltext_query(self,searchArgs):
        query_cond = []
        for p in searchArgs.keys(): 
            query_part = "%s:%s" % (p, searchArgs[p])
            query_cond.append(query_part)
        query = " ".join(query_cond)    
        return query
    def query_index(self,args):
        query = "select * from book_index where %s" % self._build_query(args)
        cursor = self._get_db_cursor()
        cursor.execute(query)
        for row in cursor.fetchall():
            print "%d [%s]: %s- %s" % ((row[0],row[6],row[1],row[3]))
    def query_fulltext_index(self,args):
        query = "select id from book_search where book_search match '%s' limit 1000" % self._build_fulltext_query(args)
        cursor = self._get_db_cursor()
        cursor.execute(query)
        ids = []
        for row in cursor.fetchall():
            ids.append(row[0])
        query = "select * from book_index where id in (%s)" % ",".join(ids)
        cursor.execute(query)
        for row in cursor.fetchall():
            print "%d [%s]: %s - %s" % ((row[0],row[6],row[1],row[3]))
    def _get_db_cursor(self):
        if not self.db_conn:
            self._open_index_database()
        if not self.db_cursor:
            self.db_cursor = self.db_conn.cursor()
        return self.db_cursor    
    def extract_book(self, book_id):
        """ public method to extract book with user specified id"""
        cursor = self._get_db_cursor()
        cursor.execute(\
            '''select id,format,archivefile,author,title,language \
             from book_index where id=%s'''%(book_id))
        query_result = cursor.fetchone()
        if not query_result:
            return None
        (book_id, bookFormat, archiveFile, author, title, lang) = query_result
        book_archive = zipfile.ZipFile(os.path.join(self.config['dataDir'], archiveFile))
        book_archive.extract(str(book_id)+"."+bookFormat, self.config['extractPath'])
        os.rename(os.path.join(self.config['extractPath'], str(book_id)+"."+bookFormat), os.path.join(self.config['extractPath'], ("%s- %s" % (author, title)).replace(' ', '_')+"."+bookFormat))
