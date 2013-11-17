#!/usr/bin/python
# -*- coding: utf-8 -*-
import os, zipfile, sqlite3, glob, traceback, hashlib,tempfile,shutil
class BookIndex:
	def __init__(self):
		self.config={}
		self.dbCursor=None
		self.dbConn=None
		self.LoadSettings()
		if self.IsIndexValid():
			print "Re-using existing index"
			self.OpenIndexDatabase()
		else:	
			print "Creating index"
			self.CreateIndex()
	def LoadSettings(self):
		#TODO use configuration file
		self.config['dataDir']=os.path.join("data","fb2.Flibusta.Net")
		self.config['indexFile']=os.path.join(self.config['dataDir'],"flibusta_fb2_local.inpx")
		self.config['tmpPath']=os.path.join(tempfile.gettempdir(),'flishell')
		self.config['dbPath']=os.path.join("data","db")
		self.config['extractPath']=os.path.join("data","books")
		self.IndexRecordNameMapping={0:'author',1:'genre',2:'title',5:'bookid',6:'size',9:'format',11:'language',10:'date_added'}
	def UnpackIndexFile(self):
		indexArchive=zipfile.ZipFile(self.config['indexFile'])
		#TODO: cleanup TMPPATH
		try: os.mkdir(self.config['tmpPath'])
		except: pass
		indexArchive.extractall(self.config['tmpPath'])
	def OpenIndexDatabase(self):
		if self.dbConn: return self.dbConn
		try: os.mkdir(self.config['dbPath'])
		except: pass
		dbConn=sqlite3.connect(os.path.join(self.config['dbPath'],'fliShellIndex.db'))
		dbConn.text_factory=str
		self.dbConn=dbConn
	def CreateIndexDatabase(self):
		try: os.mkdir(self.config['dbPath'])
		except: pass
		self.OpenIndexDatabase()
		c=self.GetDbCursor()
		c.execute('''drop table if exists book_index''')
		c.execute('''drop table if exists book_search''')
		c.execute('''create table book_index(id int,author text collate nocase,genre text,title text collate nocase,archivefile text,format text,language text,date_added text,size int)''')
		c.execute('''create table if not exists settings(name text,value text, unique(name) on conflict replace )''')
		c.execute('''CREATE VIRTUAL TABLE if not exists book_search USING fts4(id,author,title,language)''')
	def GetIndexFilesList(self):
		indexfilesIter=glob.iglob(os.path.join(self.config['tmpPath'],'*.inp'))
		return indexfilesIter
	def GetParsedIndexFileData(self,indexFile):		
		with open(os.path.join(self.config['tmpPath'],indexFile)) as infile:
			while 1:
				line=infile.readline()
				if not line: break
				bookMetadata=self.ParseFileMetadata(line)
				yield bookMetadata
	def MapFieldIndexesToNames(self,inputArray):
		outputRecord={}
		for i in self.IndexRecordNameMapping.keys():
			outputRecord[self.IndexRecordNameMapping[i]]=inputArray[i]
		return outputRecord	
			
	def ParseFileMetadata(self,line):
		fields=line.split('\x04')
		bookMetadata=self.MapFieldIndexesToNames(fields)
		return bookMetadata
	def DbFillMetadata(self,indexFilename,bookMetadata):
		md=bookMetadata
		query='''insert into book_index (id,author,genre,title,archivefile,format,language,date_added,size) values (?,?,?,?,?,?,?,?,?)''' 
		query_data=(md['bookid'],unicode(md['author'].replace(',',' ').replace(':','').rstrip(),'utf-8'),md['genre'].replace(':',''),unicode(md['title'],'utf-8'),os.path.basename(indexFilename)[:-4]+".zip",md['format'],md['language'],md['date_added'],md['size'])
		c=self.GetDbCursor()
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
	def CreateIndex(self):
		self.CreateIndexDatabase()
		self.UnpackIndexFile()
		try:
			for file in self.GetIndexFilesList():
				print "Processing %s" % (file)
				bookMetadata=self.GetParsedIndexFileData(file)
				for data in bookMetadata:
					self.DbFillMetadata(file,data)
		finally:
			shutil.rmtree(self.config['tmpPath'])
		self.SetIndexFileChecksumDb()
		self.dbConn.commit()		
	def CreateFTSIndex(self):
		self.GetDbCursor()
		c.execute('''CREATE VIRTUAL TABLE book_search USING fts4(id,author,title,language)''')
		c.execute('''insert into book_search select id,title,author,language from book_index''')
	def GetIndexFileChecksum(self):
		self.indexFileChecksum=hashlib.md5(self.config['indexFile']).hexdigest()
		return self.indexFileChecksum
	def GetIndexFileChecksumDb(self):
		c=self.GetDbCursor()
		try:
			c.execute('select value from settings where name="index_file_checksum"')
			return c.fetchone()[0]
		except:
			return None
	def SetIndexFileChecksumDb(self):
		c=self.GetDbCursor()
		checksum=self.GetIndexFileChecksum()
		c.execute('''insert into settings (name,value) values ('index_file_checksum',?)''' , [checksum])
		self.dbConn.commit()
	def IsIndexValid(self):
		if not os.path.exists(self.config['dbPath']): return False
		if self.GetIndexFileChecksum()!=self.GetIndexFileChecksumDb(): return False
		return True
	def BuildQuery(self,searchArgs):
		queryCond=[]
		for p in searchArgs.keys(): 
			queryPart="%s LIKE '%%%s%%'" % (p, searchArgs[p])
			queryCond.append(queryPart)
		query=" AND ".join(queryCond)	
		return query
	def BuildFTQuery(self,searchArgs):
		queryCond=[]
		for p in searchArgs.keys(): 
			queryPart="%s:%s" % (p, searchArgs[p])
			queryCond.append(queryPart)
		query=" ".join(queryCond)	
		return query
	def QueryIndex(self,args):
		query="select * from book_index where %s" % self.BuildQuery(args)
		c=self.GetDbCursor()
		c.execute(query)
		for row in c.fetchall():
			print "%d [%s]: %s- %s" % ((row[0],row[6],row[1],row[3]))
	def QueryFTIndex(self,args):
		query="select id from book_search where book_search match '%s' limit 1000" % self.BuildFTQuery(args)
		c=self.GetDbCursor()
		c.execute(query)
		ids=[]
		for row in c.fetchall():
			ids.append(row[0])
		query="select * from book_index where id in (%s)" % ",".join(ids)
		c.execute(query)
		for row in c.fetchall():
			print "%d [%s]: %s - %s" % ((row[0],row[6],row[1],row[3]))
	def GetDbCursor(self):
		if not self.dbConn:
			self.OpenIndexDatabase()
		if not self.dbCursor:
			self.dbCursor=self.dbConn.cursor()
		return self.dbCursor	
	def ExtractBook(self,bookId):
		c=self.GetDbCursor()
		c.execute('''select id,format,archivefile,author,title,language from book_index where id=%s'''%(bookId))
		qRes=c.fetchone()
		if not qRes: return None
		(bookId,bookFormat,archiveFile,author,title,lang)=qRes
		bookArchive=zipfile.ZipFile(os.path.join(self.config['dataDir'],archiveFile))
		bookArchive.extract(str(bookId)+"."+bookFormat,self.config['extractPath'])
		os.rename(os.path.join(self.config['extractPath'],str(bookId)+"."+bookFormat),os.path.join(self.config['extractPath'],("%s- %s" % (author,title)).replace(' ','_')+"."+bookFormat))
