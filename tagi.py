#!/bin/python3


""" Obsluguje baze danych
    
    """
import os
import sys
import hashlib
import time
import sqlite3
from datetime import datetime
from os import path, getcwd

#db = path.join(getcwd(), 'mydatabase.db')
class Database:

    def __init__(self,db):
        db = path.join(getcwd(), db)  # baza w current dir jak odpale z /xbin to tam a jak z /data/data/.... to tam
        self.connection = sqlite3.connect(db)
        self.op_mode='debug'  # operatiin mode moze byc debug albo cos innego cokol2iek
        
        #import pdb;pdb.set_trace()
        cursor = self.connection.cursor()
        #result = cursor.execute('CREATE TABLE '+table+'(id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, value TEXT);')
        result = 'CREATE TABLE IF NOT EXISTS table_tags (hash TEXT PRIMARY KEY ON CONFLICT REPLACE, path TEXT, tags TEXT );'
        #self.debug(result)
        cursor.execute(result)
        self.connection.commit()
        cursor.close()

    # Show debug messeges
    def debug(self,*xxx):
        if self.op_mode=='debug':
            print(xxx)    

    # Check if table exists in database 
    # returns True or False
    def check_table(self,table):
        #import pdb;pdb.set_trace()
        if table is not 'sqlite_master':
            cursor = self.connection.cursor()
            cursor.execute('SELECT name FROM sqlite_master WHERE name=:name;', {'name':table})
            result = cursor.fetchone() # (users,)
            cursor.close()
            if not result:
                return False
        return True

    # Returns all columns from given table
    def get_cols(self,table):
        if self.check_table(table):
            cursor = self.connection.cursor()
            cursor.row_factory = sqlite3.Row
            query='SELECT * FROM '+table+';'
            self.debug(query,{'table':table})
            cursor.execute(query)
            results = cursor.fetchone()
            cursor.close()
            if results:  # table exists and has records i moge pobrac nazwy kolumn. Eazy
               return results.keys()
            else:  # table exists but has no recordsa trzeba wziasc nazwy z tabeli master
                cursor = self.connection.cursor()
                result='SELECT name, sql  FROM sqlite_master WHERE name=:table;'
                self.debug(result,{'table':table})
                #cursor.execute('SELECT name, sql  FROM sqlite_master WHERE name=:table;',{'table':table})
                cursor.execute(result,{'table':table})
                results = cursor.fetchone()
                cursor.close()
                results = results[1]  # tam powinien bys string i    CREATE TABLE zzz(is INT.. col TEXT)
                results = results[results.index('(')+1:results.index(')')]
                results = results.split()  # lista ['id', 'INTEGER', 'PRIMARY', 'KEY', 'col', 'TEXT', 'nazwa', 'TEXT']
                xcol = []
                for col in results:
                    if col.islower():
                        xcol.append(col)
                #import pdb;pdb.set_trace()
                return xcol
        else:
            return ('no such table',)
    
    # Query all columns and rows from given table
    def query_all(self,table):
        #import pdb;pdb.set_trace()
        if self.check_table(table):
            cursor = self.connection.cursor()
            result = 'SELECT * FROM '+table+';'
            self.debug(result)
            cursor.execute(result)
            results = cursor.fetchall()
            cursor.close()
            return results
        else:
            return ('no such table',)

    # Returns only rows matching criteria
    # {col:val, ...} AND
    def query_one(self, table, arg=()):
        if self.check_table(table):
            cols = self.get_cols(table)
            keys = arg.keys()
            xval = []
            for col in cols:
                if col in keys:
                    xval.append(col+'=:'+col)
            xval = ' AND '.join(xval)
            if xval:
                cursor = self.connection.cursor()
                result = 'SELECT * FROM '+table+' WHERE '+xval+';'
                self.debug(result,arg)
                #import pdb;pdb.set_trace()
                result = cursor.execute(result, arg)
                result = cursor.fetchall()
                cursor.close()
                return result
            return []
        else:
            return ('no such table',)
    
    # Returns first row matching criteria
    # {col:val, ...} AND
    def query_first(self, table, arg=()):
        if self.check_table(table):
            #import pdb;pdb.set_trace()
            cols = self.get_cols(table)
            keys = arg.keys()
            xval = []
            for col in cols:
                if col in keys:
                    xval.append(col+'=:'+col)
            xval = ' AND '.join(xval)
            if xval:
                cursor = self.connection.cursor()
                result = 'SELECT * FROM '+table+' WHERE '+xval+';'
                self.debug(result,arg)
                #print(result,arg)
                cursor.row_factory = sqlite3.Row
                result = cursor.execute(result, arg)
                result = cursor.fetchone()
                cursor.close()
                #import pdb;pdb.set_trace()
                return result
            return []
        else:
            return ('no such table',)

    # Find column containing text
    def find_like(self, table, arg=()):
        if self.check_table(table):
            cols = self.get_cols(table)
            keys = arg.keys()
            xval = {} 
            #import pdb;pdb.set_trace()
            tags = arg['tags']
            result = ""
            for tag in tags:
                if tag == "OR":
                    result = result+" OR "
                elif tag == "AND":
                    result = result+" AND "
                else:
                    printerr("Search:",tag)
                    result = result + 'tags LIKE :'+tag.replace(" ","_")
                    xval[tag.replace(" ","_")] = "%"+tag+"%"
            result = result+";"
            import pdb;pdb.set_trace()
            #ala = "%" + "%".join(arg['tags']) + "%"
            #xval['tags'] = '%' +arg[col] +'%'
            #xval['tags'] = "%plik%"
            #xval['dwa'] = "%siema%"
            #result  = 'tags LIKE :tags OR tags LIKE :dwa;'
            cursor = self.connection.cursor()
            result = 'SELECT * FROM '+table+' WHERE '+result
            #result = 'SELECT * FROM '+table+' WHERE title LIKE :title;'
            self.debug(result,xval)
            #import pdb;pdb.set_trace()
            result = cursor.execute(result, xval)
            result = cursor.fetchall()
            cursor.close()
            return result
        else:
            return ('no such table',)
    
    # Show count of all rows in given table
    def count_all(self,table):
        if self.check_table(table):
            cursor = self.connection.cursor()
            result = 'SELECT COUNT(*) FROM '+table+';' # WHERE status=:status;'
            self.debug(result)
            cursor.execute(result,{'status':'completed'})
            results = cursor.fetchone()
            cursor.close()
            #import pdb;pdb.set_trace()
            return results[0]
        else:
            return ('no such table',)

    # Count only those rows matching columns
    def count_only(self, table, arg=()):
        if self.check_table(table):
            cols = self.get_cols(table)
            keys = arg.keys()
            xval = []
            for col in cols:
                if col in keys:
                    xval.append(col+'=:'+col)
            xval = ' AND '.join(xval)
            if xval:
                cursor = self.connection.cursor()
                result = 'SELECT COUNT(*) FROM '+table+' WHERE '+xval+';'
                #result = 'SELECT COUNT(*) FROM '+table+';' # WHERE status=:status;'
                self.debug(result,arg)
                #import pdb;pdb.set_trace()
                result = cursor.execute(result, arg)
                result = cursor.fetchone()
                cursor.close()
                return result[0]
            return []
        else:
            return ('no such table',)
    
    # Insert new row to given table
    # table, {col:value, col2:value, ...}
    # returns id if successed
    def insert_row(self, table, arg=()):
        if self.check_table(table):
            cursor = self.connection.cursor()
            cols = self.get_cols(table)
            keys = arg.keys()
            xcol = []
            xval = []
            for col in cols:
                if col in keys:
                    xcol.append(col)
                    xval.append(':'+col)
            #import pdb;pdb.set_trace()
            if xcol == []: # nie ma takich column w tabeli
                return
            cols = ', '.join(xcol)
            xval = ', '.join(xval)
            cols = '('+cols+')'
            xval = '('+xval+')'
            #(       'INSERT INTO   table(name, created) values(:name, :created)')
            result = 'INSERT INTO '+table+ cols+ ' values'+xval+';'
            self.debug(result,arg)
            cursor.execute(result, arg)
            self.connection.commit()
            result = cursor.lastrowid
            cursor.close()
            return result
        else:
            return ('no such table',)

    # Update columns in given table by id 
    # table, {id:id, col1:new_value, col2:new_value, ...}
    def update_by_row(self,table,arg):
        if self.check_table(table):
            cols = self.get_cols(table)
            keys = arg.keys()
            xval = []
            for col in cols:
                if col in keys:
                    xval.append(col+'=:'+col)
            xval = xval[1:]   # wyrzucil :id
            xval = ', '.join(xval)
            if xval == '': #nje ma takich kolumn
                return ('no such column',)
            # UPDATE table SET name=:name, created=:created WHERE id = :id
            result = 'UPDATE '+table+' SET '+xval+' WHERE id=:id;'
            self.debug(result,arg)
            cursor = self.connection.cursor()
            cursor.execute(result, arg)
            self.connection.commit()
            cursor.close()
        else:
            return ('no such table',)

    # Update columns in given table by hash (id)
    # table, {id:id, col1:new_value, col2:new_value, ...}
    def update_by_hash(self,table,arg):
        if self.check_table(table):
            cols = self.get_cols(table)
            keys = arg.keys()
            xval = []
            for col in cols:
                if col in keys:
                    xval.append(col+'=:'+col)
            xval = xval[1:]   # wyrzucil :id
            xval = ', '.join(xval)
            if xval == '': #nje ma takich kolumn
                return ('no such column',)
            # UPDATE table SET name=:name, created=:created WHERE id = :id
            result = 'UPDATE '+table+' SET '+xval+' WHERE hash=:hash;'
            self.debug(result,arg)
            cursor = self.connection.cursor()
            cursor.execute(result, arg)
            self.connection.commit()
            cursor.close()
        else:
            return ('no such table',)

    # Delete row from table where hash matches
    # {col:val, col2:val, ...} AND
    def del_by_hash(self, table, arg):
        #import pdb;pdb.set_trace()
        if self.check_table(table):
            cols = self.get_cols(table)
            keys = arg.keys()
            xval = []
            for col in cols:
                if col in keys:
                    xval.append(col+'=:'+col)
            #if xval == []:  # nie ma takich kolumn
            #    return
            #xval = ' AND '.join(xval)
            result = 'DELETE FROM '+table+' WHERE hash=:hash;'
            self.debug(result,arg)
            self.debug(result,arg)
            cursor = self.connection.cursor()
            cursor.execute(result,arg)
            self.connection.commit()
            cursor.close()
        else:
            return ('no such table',)
    
    # Delete row from table where matches
    # {col:val, col2:val, ...} AND
    def del_row(self, table, arg):
        #import pdb;pdb.set_trace()
        if self.check_table(table):
            cols = self.get_cols(table)
            keys = arg.keys()
            xval = []
            for col in cols:
                if col in keys:
                    xval.append(col+'=:'+col)
            if xval == []:  # nie ma takich kolumn
                return
            xval = ' AND '.join(xval)
            result = 'DELETE FROM '+table+' WHERE '+xval+';'
            self.debug(result,arg)
            cursor = self.connection.cursor()
            cursor.execute(result,arg)
            self.connection.commit()
            cursor.close()
        else:
            return ('no such table',)
    
    # create html file with full table
    # in /sdcard/baza.html
    def make_html(self,table):
        res = self.get_cols(table)
        tag_row = '<table border=1>\n<tr>'
        for col in res:
            tag_row = tag_row +'<td><b>'+str(col)+'</b></td>'
        tag_row = tag_row +'</tr>\n'
        res = self.query_all(table)
        for row in res:
            tag_row = tag_row +'<tr>'
            for col in row:
                tag_row = tag_row +'<td>'+str(col)+'</td>'
            tag_row = tag_row +'</tr>\n'
        tag_row = tag_row +'</table>'
        self.debug(tag_row)
        plik = '/sdcard/baza.html'
        plik = open(plik,'w')
        plik.write(tag_row)
        plik.close()
    
    # Delete table from database
    def del_table(self, table): 
        if self.check_table(table):
            cursor = self.connection.cursor()
            result = 'DROP TABLE '+table
            self.debug(result)
            cursor.execute(result)
            self.connection.commit()
            cursor.close()
        return ('no such table',)

    # Returns all tables in database
    def get_tables(self):
        #import pdb;pdb.set_trace()
        cursor = self.connection.cursor()
        result = 'SELECT * FROM sqlite_master;'
        self.debug(result)
        cursor.execute(result)
        result = cursor.fetchall() 
        cursor.close()
        return result

    # Create new table if not exists with given name
    # and columns as string 'id TEXT, name, value INTEGER'
    def create_table(self, table, cols='id'):
        cursor = self.connection.cursor()
        #result = cursor.execute('CREATE TABLE '+table+'(id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, value TEXT);')
        result = 'CREATE TABLE IF NOT EXISTS table_tags (hash PRIMARY KEY TEXT, path TEXT, tags TEXT );'
        self.debug(result)
        cursor.execute(result)
        self.connection.commit()
        cursor.close()
    
    # Add column to given table
    # col_name TEXT or INTEGER or REAL or BLOB
    # PRIMARY KEY, AUTOINCREMENT
    def add_column(self,table,col_name):
        if self.check_table(table):
            cursor = self.connection.cursor()
            if ' ' in col_name:  # jesli jest spacja i cos jeszcze
                result = 'ALTER TABLE '+table+' ADD COLUMN '+col_name+';'
# powinno byc tak ALTER TABLE tablica ADD COLUMN 'id INTEGER PRIMARY KEY AUTOINCREMENT'
            else:
                result = 'ALTER TABLE '+table+' ADD COLUMN '+col_name+' TEXT;'
            self.debug(result)
            cursor.execute(result)
            self.connection.commit()
            cursor.close()
        else:
            return ('no such table',)

    # Execute meny SQL commands divided by ; and \n
    # CREATE VIEW new_table AS SELECT col3, col2 FROM table;
    # DROP VIEW new_view
    def executescript(self, script):
        #import pdb;pdb.set_trace()
        #self.executeSQL('SELECT * FROM linki WHERE title LIKE %pussy%;')
        cursor = self.connection.cursor()
        self.debug(script)
        cursor.executescript(script)
        #self.connection.commit()
        result = cursor.fetchall()
        cursor.close()
        print(result)
        return result

    # execute SQL command
    def executeSQL(self, command, args):
        #import pdb;pdb.set_trace)
        #command = 'SELECT * FROM linki WHERE title LIKE :title;'
        #args={'title':'%wife%'}
        cursor = self.connection.cursor()
        self.debug(command,args)
        #import pdb;pdb.set_trace()
        result = cursor.execute(command,args)
        result = cursor.fetchall()
        self.connection.commit()
        cursor.close()
        #print(result)
        return result

# end of Class




if  __name__ == '__main__':

    if (len(sys.argv)<3):
        print("""\tUsage:
                  'update' '/path/to/file' 'tags'
                  'insert' '/path/to/file' 'tags'
                  'search' tag OR tag2 OR 'tag 3' AND tag4
                  'get_tags' 'file'
                  'update_path' '/path/to/file'
                  'remove' '/path/to/file'
                  'show_all' 'cos'
              """)
        exit()

    start = time.time()
    # podlancza sie do wybranej bazy i wybiera table
    db = Database('tagi.db')
    db.op_mode='xxdebugxx'
    table = 'table_tags'
    #print(help(db))

    def printerr(*args, **kwargs):
        print(*args, file=sys.stderr, **kwargs)
    
    def show_result(result):
        for row in result:
            print(row)
    
    def get_md5sum(file):
        def file_as_bytes(file):
            with file:
                return file.read()
        if not os.path.isfile(file):
            printerr("File not exists. Exit")
            exit()
        return hashlib.md5(file_as_bytes(open(file, 'rb'))).hexdigest()

    command = sys.argv[1]
    #filepath = os.path.realpath(sys.argv[2])
    #tags = sys.argv[3:]
    #hash = get_md5sum(filepath)
    #print("plik", filepath)
    #print("tagi", tags)
    #print("hash", hash)
    #print("MODE:",command)
    printerr()

    def update_path():
        filepath = os.path.realpath(sys.argv[2])
        hash = get_md5sum(filepath)
        result = db.query_one(table,{'hash':hash})
        if result:
            row = {}
            row["hash"] = hash
            row["path"] = filepath
            db.update_by_hash(table, row)
            printerr("Updated path", filepath)
        else:
            printerr("Record not exists. No need to update path", filepath)

    def insert_or_update():
        filepath = os.path.realpath(sys.argv[2])
        tags = sys.argv[3:]
        hash = get_md5sum(filepath)
        row = {}
        row["hash"] = hash
        row["path"] = filepath
        row["tags"] = ",".join(tags)
        result = db.query_one(table,{'hash':hash})
        if result:
            printerr("Record exists. Do update", filepath)
            db.update_by_hash(table, row)
        else:
            printerr("Record not exists. Do insert")
            record_id = db.insert_row(table, row)
            printerr("New record added #", record_id, filepath)

    """ moge uzywac inseert za kazdym razem jako insert i jako update bodidalem ON CONFLICT REPLACE
        ale za kazdym razem jak zupdatuje w taki sposob to record id sie zmieni na wyszczy czyli jak bede
        mial tylko jeden record i zuodetuje go 3 razy komendom insert to jego id bedzie 3 zamiast 1 i
        kolejny rekord bedzie 4 chociaz to jest 2 rekord

        moge uniknac tego i uzywac insert do nowych rekordow i update do istniejacych ale bede musial sprawdzac za 
        kazdym razem czy istnieje rekord, chyba w taki sposob ze jak ppobiore tagi do edycji to wyszuka rekordu
        i jak znajdzie rekord i tagi to wtedy odpale z opcja update a jka nie znajdzie nic to insert

    """

    #get tags of given file
    def get_tags():
        filepath = os.path.realpath(sys.argv[2])
        hash = get_md5sum(filepath)
        #import pdb;pdb.set_trace()
        result = db.query_one(table,{'hash':hash})
        if result:
            print("hash:",result[0][0])
            print("file:",result[0][1])
            print("tags:",result[0][2])

    #search files with given tag
    def search():
        tags = sys.argv[2:]
        #import pdb;pdb.set_trace()
        where = {}
        where["tags"] = tags #[0]
        result = db.find_like(table,where)
        show_result(result)
        
    def remove():
        filepath = os.path.realpath(sys.argv[2])
        hash = get_md5sum(filepath)
        db.del_by_hash(table,{'hash':hash})
        # nie wazne czy rekord istnieje nie bedzie errora ani rezultatu

    if (command == "update"):
        insert_or_update()
    elif (command == "insert"):
        insert_or_update()
    elif (command == "get_tags"):
        get_tags()
    elif (command == "search"):
        search()
    elif (command == "update_path"):
        update_path()
    elif (command == "remove"):
        remove()
    elif (command == "show_all"):
        result = db.query_all(table)
        show_result(result)
    
    printerr("\noperation started at:  ",datetime.fromtimestamp(start))
    finish = time.time()
    printerr("operation finished at: ",datetime.fromtimestamp(finish))
    printerr("operation took: ",finish - start)
    exit()





    def add_record(table):
        created = int(time.time())
        row = {}
        print(db.get_cols(table))
        while True:
            n = input('column: ')
            if not n:
                break
            v = input('value: ')
            row[n] = v
        #row['created'] = created
        #import pdb;pdb.set_trace()
        user_id = db.insert_row(table, row)
        print(user_id)


    def del_record(table):
        row = {}
        print(db.get_cols(table))
        while True:
            n = input('column: ')
            if not n:
                break
            v = input('value: ')
            row[n] = v
        cols = db.del_row(table,row)
        print(cols)

    def show_all(table):
        #import pdb; pdb.set_trace()
        result = db.query_all(table)
        for row in result:
            print(row)
        return result

    def show_one(table):
        print(db.get_cols(table))
        where = {}
        while True:
            n = input('column: ')
            if not n:
                break
            v = input('value: ')
            where[n] = v
        result = db.query_one(table, where)
        for row in result:
            print(row)
    
    def show_only(table):
        print(db.get_cols(table))
        where = {}
        while True:
            n = input('column: ')
            if not n:
                break
            v = input('value: ')
            where[n] = v
        result = db.count_only(table, where)
        print(result)
   
    def find_something(table):
        print('Find column with given text')
        print(db.get_cols(table))
        where = {}
        n = input('column: ')
        v = input('value: ')
        where[n] = v
        result = db.find_like(table,where)
        for row in result:
            print(row)
        return result

    def getFirst(table):
        print(db.get_cols(table))
        where = {}
        print('get only one row with given criteria')
        while True:
            n = input('column: ')
            if not n:
                break
            v = input('value: ')
            where[n] = v
        result = db.query_first(table, where)
        print(dict(result))

    def add_col(table):
        col_name = input('Name: ')
        result = db.add_column(table ,col_name)
        print(result)

    def add_table():
        show_all('sqlite_master')
        table = input('Table name: ')
        db.create_table(table)

    def del_table():
        show_all('sqlite_master')
        table = input('Table name: ')
        db.del_table(table)

    def update_by_row(table):
        row = input('Row: ')
        row = {'id':row}
        print(db.get_cols(table))
        while True:
            n = input('column: ')
            if not n:
                break
            v = input('value: ')
            row[n] = v
        #row['created'] = int(time.time())
        #print(row)
        return db.update_by_row(table,row)

    def check_table():
        table = input('Name: ')
        print(db.check_table(table))

    def run_script():
        print('CREATE VIEW new_table AS SELECT col3, col2 FROM table;')
        print('DROP VIEW view')
        print('CREATE TABLE new_one(id,name, value);')
        print('TEXT, BLOB, INTEGER REAL, PRIMARY KEY, AUTOINCREMENT, UNIQUE ON CONFLICT REPLACE')
        print('ALTER TABLE xxx ADD TEXT col;')
        print('ALTER TABLE xxx RENAME TO ccc;')
        print('CREATE INDEX IF NOT EXISTS nazwa ON linki(url);')
        table = input('SQL Script: ')
        #table = 'create table xxx(id INTEGER PRIMARY KEY AUTOINCREMENT, nazwa TEXT UNIQUE ON CONFLICT REPLACE, inna TEXT);'
        print(table)
        print(db.executescript(table))

    def blob():
        import pickle
        cc = input('Wtite  Read: ')
        if cc == 'w':
            plik = '/sdcard/file.png'
            plik=open(plik,'rb')
            data=plik.read()
            plik.close()
            print(db.insert_row('cookies',{'dane':data}))
            #import pdb; pdb.set_trace()

        if cc == 'r':
            dane = db.query_one('cookies',{'id':'3'})
            import pdb;pdb.set_trace()
            dane = dane[0][1]
            plik = '/sdcard/file2.png'
            plik=open(plik,'wb')
            data=plik.write(dane)
            plik.close()
            all_cookies = pickle.loads(dane)  

    while True:
        print('\nTable:',table)
        print('1_Insert  2_Delete  3_show_All  4_show_One  First  5_show_Tables  6_change_Table  7_Add_column  8_html  9_add_table  0_Del_table  col  Update  v  fiNd  Script  Blobi  countall')
        cc = input(':')
        if cc == '1':
            add_record(table)
        if cc == '2':
            del_record(table)
        if cc == '3':
            show_all(table)
        if cc == '4':
            show_one(table)
        if cc == '5':
            show_all('sqlite_master')
        if cc == '6':
            show_all('sqlite_master')
            table = input('Table name: ')
        if cc == '7':
            add_col(table)
        if cc == '8':
            db.make_html(table)
        if cc == '9':
            add_table()
        if cc == '0':
            del_table()
        if cc == 'f':
            getFirst(table)
        if cc == 'n':
            find_something(table)
        if cc == 'u':
            print(update_by_row(table))
        if cc == 'countall':
            dd=db.count_all(table)
            print(dd)
        if cc == 'countonly':
            show_only(table)
        if cc == 'col':
            cols = db.get_cols(table)
            print(cols)
        if cc == 'v':
            print('Check if table exists')
            check_table()
        if cc == 's':
            run_script()
        if cc == 'b':
            blob()
        if cc == 'x' or cc == 'q':
            break





