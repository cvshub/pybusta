#!/usr/bin/python
# -*- coding: utf-8 -*-
import os, zipfile, sqlite3, glob, traceback, hashlib,tempfile,shutil
class BookIndex:
    """ Main class for storing and accessing books database index"""
    def __init__(self):
        self.config={}
        self.dbCursor=None
        self.dbConn=None
        self.load_settings()
        if self.check_index_valid():
            print "Re-using existing index"
            self.open_index_database()
        else:    
            print "Creating index"
            self.create_index()
    def load_settings(self):
        #TODO use configuration file
        self.config['dataDir']=os.path.join("data","fb2.Flibusta.Net")
        self.config['indexFile']=os.path.join(self.config['dataDir'],"flibusta_fb2_local.inpx")
        self.config['tmpPath']=os.path.join(tempfile.gettempdir(),'flishell')
        self.config['dbPath']=os.path.join("data","db")
        self.config['extractPath']=os.path.join("data","books")
        self.IndexRecordNameMapping={0:'author',1:'genre',2:'title',5:'bookid',6:'size',9:'format',11:'language',10:'date_added'}
    def unpack_index_file(self):
        indexArchive=zipfile.ZipFile(self.config['indexFile'])
        #TODO: cleanup TMPPATH
        try: os.mkdir(self.config['tmpPath'])
        except: pass
        indexArchive.extractall(self.config['tmpPath'])
    def open_index_database(self):
        if self.dbConn: return self.dbConn
        try: os.mkdir(self.config['dbPath'])
        except: pass
        dbConn=sqlite3.connect(os.path.join(self.config['dbPath'],'fliShellIndex.db'))
        dbConn.text_factory=str
        self.dbConn=dbConn
    def create_index_database(self):
        try: os.mkdir(self.config['dbPath'])
        except: pass
        self.open_index_database()
        c=self.get_db_cursor()
        c.execute('''drop table if exists book_index''')
        c.execute('''drop table if exists book_search''')
        c.execute('''create table book_index(id int,author text collate nocase,genre text,title text collate nocase,archivefile text,format text,language text,date_added text,size int)''')
        c.execute('''create table if not exists settings(name text,value text, unique(name) on conflict replace )''')
        c.execute('''CREATE VIRTUAL TABLE if not exists book_search USING fts4(id,author,title,language)''')
    def get_index_files_list(self):
        indexfilesIter=glob.iglob(os.path.join(self.config['tmpPath'],'*.inp'))
        return indexfilesIter
    def get_parsed_index_file_data(self,indexFile):        
        with open(os.path.join(self.config['tmpPath'],indexFile)) as infile:
            while 1:
                line=infile.readline()
                if not line: break
                bookMetadata=self.parse_file_metadata(line)
                yield bookMetadata
    def map_field_indexes_to_names(self,inputArray):
        outputRecord={}
        for i in self.IndexRecordNameMapping.keys():
            outputRecord[self.IndexRecordNameMapping[i]]=inputArray[i]
        return outputRecord    
            
    def parse_file_metadata(self,line):
        fields=line.split('\x04')
        bookMetadata=self.map_field_indexes_to_names(fields)
        return bookMetadata
    def db_fill_metadata(self,indexFilename,bookMetadata):
        md=bookMetadata
        query='''insert into book_index (id,author,genre,title,archivefile,format,language,date_added,size) values (?,?,?,?,?,?,?,?,?)''' 
        query_data=(md['bookid'],unicode(md['author'].replace(',',' ').replace(':','').rstrip(),'utf-8'),md['genre'].replace(':',''),unicode(md['title'],'utf-8'),os.path.basename(indexFilename)[:-4]+".zip",md['format'],md['language'],md['date_added'],md['size'])
        c=self.get_db_cursor()
        try:
            c.execute(query,query_data)
        except Exception:
            print query,":".join(query_data)
            raise 
        query='''insert into book_search (id,author,title,language) values (?,?,?,?)''' 
        query_data=(md['bookid'],unicode(md['author'].replace(',',' ').replace(':','').rstrip(),'utf-8').upper(),unicode(md['title'],'utf-8').upper(),md['language'])
        try:
            c.execute(query,query_data)
        except Exception:
            print query,":".join(query_data)
            raise 
    def create_index(self):
        self.create_index_database()
        self.unpack_index_file()
        try:
            for file in self.get_index_files_list():
                print "Processing %s" % (file)
                bookMetadata=self.get_parsed_index_file_data(file)
                for data in bookMetadata:
                    self.db_fill_metadata(file,data)
        finally:
            shutil.rmtree(self.config['tmpPath'])
        self.set_indexfile_checksum_db()
        self.dbConn.commit()        
    def create_fulltext_index(self):
        self.get_db_cursor()
        c.execute('''CREATE VIRTUAL TABLE book_search USING fts4(id,author,title,language)''')
        c.execute('''insert into book_search select id,title,author,language from book_index''')
    def get_indexfile_cheksum_db(self):
        self.indexFileChecksum=hashlib.md5(self.config['indexFile']).hexdigest()
        return self.indexFileChecksum
    def get_index_file_cheksum_db(self):
        c=self.get_db_cursor()
        try:
            c.execute('select value from settings where name="index_file_checksum"')
            return c.fetchone()[0]
        except:
            return None
    def set_indexfile_checksum_db(self):
        c=self.get_db_cursor()
        checksum=self.get_indexfile_cheksum_db()
        c.execute('''insert into settings (name,value) values ('index_file_checksum',?)''' , [checksum])
        self.dbConn.commit()
    def check_index_valid(self):
        if not os.path.exists(self.config['dbPath']): return False
        if self.get_indexfile_cheksum_db()!=self.get_index_file_cheksum_db(): return False
        return True
    def build_query(self,searchArgs):
        queryCond=[]
        for p in searchArgs.keys(): 
            queryPart="%s LIKE '%%%s%%'" % (p, searchArgs[p])
            queryCond.append(queryPart)
        query=" AND ".join(queryCond)    
        return query
    def build_fulltext_query(self,searchArgs):
        queryCond=[]
        for p in searchArgs.keys(): 
            queryPart="%s:%s" % (p, searchArgs[p])
            queryCond.append(queryPart)
        query=" ".join(queryCond)    
        return query
    def query_index(self,args):
        query="select * from book_index where %s" % self.build_query(args)
        c=self.get_db_cursor()
        c.execute(query)
        for row in c.fetchall():
            print "%d [%s]: %s- %s" % ((row[0],row[6],row[1],row[3]))
    def query_fulltext_index(self,args):
        query="select id from book_search where book_search match '%s' limit 1000" % self.build_fulltext_query(args)
        c=self.get_db_cursor()
        c.execute(query)
        ids=[]
        for row in c.fetchall():
            ids.append(row[0])
        query="select * from book_index where id in (%s)" % ",".join(ids)
        c.execute(query)
        for row in c.fetchall():
            print "%d [%s]: %s - %s" % ((row[0],row[6],row[1],row[3]))
    def get_db_cursor(self):
        if not self.dbConn:
            self.open_index_database()
        if not self.dbCursor:
            self.dbCursor=self.dbConn.cursor()
        return self.dbCursor    
    def extract_book(self,bookId):
        c=self.get_db_cursor()
        c.execute('''select id,format,archivefile,author,title,language from book_index where id=%s'''%(bookId))
        qRes=c.fetchone()
        if not qRes: return None
        (bookId,bookFormat,archiveFile,author,title,lang)=qRes
        bookArchive=zipfile.ZipFile(os.path.join(self.config['dataDir'],archiveFile))
        bookArchive.extract(str(bookId)+"."+bookFormat,self.config['extractPath'])
        os.rename(os.path.join(self.config['extractPath'],str(bookId)+"."+bookFormat),os.path.join(self.config['extractPath'],("%s- %s" % (author,title)).replace(' ','_')+"."+bookFormat))
