#!/bin/python3


""" taging system
    
    """

database_here = "/root/sources/Taging/tagi.db"
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
    
    # Selact all tags from table and show uniqe only
    def get_all_tags(self, table):
        if self.check_table(table):
            #import pdb;pdb.set_trace()
            cols = self.get_cols(table)
            cursor = self.connection.cursor()
            result = 'SELECT * FROM '+table+';'
            
            #to tylko do testu zeby buylo szybko
            #xval = {} 
            #xval['dog'] = "%dog%"
            #xval['fpv'] = "%fpv%"
            #result = 'SELECT * FROM table_tags WHERE tags LIKE :doggy AND tags LIKE :fpv;'
            #import pdb;pdb.set_trace()
            result = cursor.execute(result) #,xval)   #xval nie potrzebne
            global_tags = []
            while (result):
                result = cursor.fetchmany(30)
                for row in result:
                    row_tags = row[2].split(',')
                    global_tags += row_tags
            cursor.close()
            
            all_tags = len(global_tags)
            numbers = {}
            while global_tags:
                tag = global_tags[0]
                if tag not in numbers.keys():
                    numbers[tag] = global_tags.count(tag)
                global_tags.pop(0)

            result = {key: value for key, value in sorted(numbers.items())}
            #import pdb;pdb.set_trace()
            print(result)
            print("--------------------- STATUS ---------------------")
            print("ALL tags:",all_tags)
            print("Unique tags:",len(result))
        else:
            return ('no such table',)

    def find_in_path(self, table, arg=()):
        if self.check_table(table):
            cols = self.get_cols(table)
            keys = arg.keys()
            xval = {} 
            #import pdb;pdb.set_trace()
            path = arg['file']#[0]
            result = 'path LIKE :path'
            #for tag in path:
            #    if tag == "OR":
            #        result = result+" OR "
            #    elif tag == "AND":
            #        result = result+" AND "
            #    else:
            printerr("Search in:", path)
            #        result = result + 'tags LIKE :'+tag.replace(" ","_")
            xval["path"] = "%"+path+"%"
            result = result+";"
            #import pdb;pdb.set_trace()
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
                elif tag == "NOT":
                    result = result+" AND NOT "
                else:
                    printerr("Search:",tag)
                    result = result + 'tags LIKE :'+tag.replace(" ","_").replace(",","_")
                    xval[tag.replace(" ","_").replace(",","_")] = "%"+tag+"%"
            result = result+";"
            #import pdb;pdb.set_trace()
            #ala = "%" + "%".join(arg['tags']) + "%"
            #xval['tags'] = '%' +arg[col] +'%'
            #xval['tags'] = "%plik%"
            #xval['dwa'] = "%siema%"
            #result  = 'tags LIKE :tags OR tags LIKE :dwa;'
            cursor = self.connection.cursor()
            result = 'SELECT * FROM '+table+' WHERE '+result
            #self.debug(result,xval)
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
    def del_by_path(self, table, arg):
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
            result = 'DELETE FROM '+table+' WHERE path=:path;'
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

    def print_usage():
        #import pdb;pdb.set_trace()
        print("""\tUsage:
                  'update'  '/path/to/file' tag1 tag2 'tag 3'
                  'insert'  '/path/to/file' tag1 tag2 'tag 3'
                  'add'     '/path/to/file' tag1 tag2 'tag 3'
                  'search'  tag OR tag2 OR 'tag 3' AND tag4
                  'in_path' keyword
                  'stat_dir' '/path/to/dir'
                  'get_tags' '/path/to/file'
                  'update_path' '/path/to/file'
                  'remove'  '/path/to/file'
                  'show_all'
                  'status'
              """)
        exit()

    if (len(sys.argv) <= 1 ): print_usage()
    
    start = time.time()
    # podlancza sie do wybranej bazy i wybiera table
    db = Database(database_here)
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
            #print(file)
            with file:
                return file.read()
        if not os.path.isfile(file):
            import pdb;pdb.set_trace()
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
    
    def clean_tags(tags): 
        #import pdb;pdb.set_trace()
        tags = tags.replace(".",",")    # remove spaces and
        tags = tags.replace(" ,",",")   # double ,, from string
        tags = tags.replace(", ",",")
        tags = tags.replace(",,",",")
        while tags.startswith(" "):     # remove space from begining
            tags = tags[1:]
        while tags.endswith(","):       # and from end of line
            tags = tags[:-1]
        # remove same tags
        tags = tags.split(",")
        clean_tags = []
        while tags:
            tag = tags.pop(0)
            if tag not in clean_tags:
                clean_tags.append(tag)
        tags = ",".join(clean_tags)
        return tags

    def update_path():
        if (len(sys.argv) <= 2 ): print_usage()
        files = sys.argv[2:]
        files = files[0].split("\n")
        for plik in files:
            filepath = os.path.realpath(plik)
            hash = get_md5sum(filepath)
            #printerr("-->",hash,filepath)
            result = db.query_one(table,{'hash':hash})
            #import pdb;pdb.set_trace()
            if result:
                if result[0][1] != filepath:
                    #import pdb;pdb.set_trace()
                    row = {}
                    row["hash"] = hash          # potrzeby do znalezienia rekordu
                    row["path"] = filepath      # nowa sciezka
                    db.update_by_hash(table, row)   # tagi zostana nie zmienione
                    printerr("Old path", result[0][1])
                    printerr("Updated path", filepath)

    def insert_update_add():
        #import pdb;pdb.set_trace()
        if (len(sys.argv) <= 3 ): print_usage()
        filepath = os.path.realpath(sys.argv[2])
        tags = sys.argv[3:]
        hash = get_md5sum(filepath)
        #import pdb;pdb.set_trace()
        tags = ",".join(tags)
        row = {}
        row["hash"] = hash
        row["path"] = filepath
        row["tags"] = tags
        result = db.query_one(table,{'hash':hash})
        if result:
            printerr("Record exists", filepath, result[0][2]) #.split(','))
            if command == "add":
                printerr("Adding tags", tags)
                row["tags"] = result[0][2]+","+row["tags"]
                row['tags'] = clean_tags(row['tags'])
            else:
                printerr("Do update", tags)
            row['tags'] = clean_tags(row['tags'])
            db.update_by_hash(table, row)
        else:
            printerr("Record not exists. Do insert")
            row['tags'] = clean_tags(row['tags'])
            record_id = db.insert_row(table, row)
            printerr("New record added #", record_id, filepath)
            printerr("Tags", tags)

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
        if (len(sys.argv) <= 2 ): print_usage()
        filepath = os.path.realpath(sys.argv[2])
        hash = get_md5sum(filepath)
        #import pdb;pdb.set_trace()
        result = db.query_one(table,{'hash':hash})
        if result:
            #print("hash:",result[0][0])
            #print("file:",result[0][1])
            #print("tags:",result[0][2])
            print(result[0][2])

    def get_by_path(mode):
        if (len(sys.argv) <= 2 ): print_usage()
        path = sys.argv[2:]
        where = {}
        #import pdb;pdb.set_trace()
        if ( mode == "stat_dir"):
            path = os.path.realpath(path[0])
            where["file"] = path
            result = db.find_in_path(table,where)   # all files in all subdirectories
            all_files_in_dir = os.listdir(path)
            print("\n All taged files in: ", path)
            for row in result:
                name = os.path.basename(row[1])
                if (name in all_files_in_dir):
                    print(name,"\t",row[2])
                    all_files_in_dir.remove(name)
            print("\n Not listed files:")
            for plik in all_files_in_dir:
                filepath = os.path.realpath(path+plik)
                if (filepath):
                    plik+="/"
                print(plik)

        else:    # search for keyword in path
            where["file"] = path[0]
            result = db.find_in_path(table,where)   # all files in all subdirectories
            for row in result:
                print(row[1])

    #search files with given tag
    def search():
        if (len(sys.argv) <= 2 ): print_usage()
        tags = sys.argv[2:]
        #import pdb;pdb.set_trace()
        where = {}
        where["tags"] = tags #[0]
        result = db.find_like(table,where)
        #show_result(result)
        for row in result:
            print(row[1])
        
    def remove():
        if (len(sys.argv) <= 2 ): print_usage()
        filepath = os.path.realpath(sys.argv[2])
        db.del_by_path(table,{'path':filepath})
        # nie wazne czy rekord istnieje nie bedzie errora ani rezultatu

    if (command == "update"):
        insert_update_add()
    elif (command == "insert"):
        insert_update_add()
    elif (command == "add"):
        insert_update_add()
    elif (command == "get_tags"):
        get_tags()
    elif (command == "search"):
        search()
    elif (command == "update_path"):
        update_path()
    elif (command == "remove"):
        remove()
    elif (command == "in_path"):
        get_by_path("in_path")
    elif (command == "stat_dir"):
        get_by_path("stat_dir")
    elif (command == "status"):
        db.get_all_tags(table)
        print("Records in db:",db.count_all(table))
    elif (command == "show_all"):
        result = db.query_all(table)
        show_result(result)

    else:
        print("No.Such command", sys.argv[1])
        print_usage()
    
    printerr("\noperation started at:  ",datetime.fromtimestamp(start))
    finish = time.time()
    printerr("operation finished at: ",datetime.fromtimestamp(finish))
    printerr("operation took: ",finish - start)


