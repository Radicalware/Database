#!/usr/bin/env python3

import MySQLdb
import re

# Copyright [2018] [Joel Leagues email: Scourge @ protomail.com]

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

class mysql_controller:
    
    def __init__(self,config):

        self._accessor = None#''#MySQLdb.connections.Connection
        self._config = {
            'username' : '',
            'password' : '',
            'server'   : '',
            'database' : '',
            'table'    : '',
            'columns'  : [''],   # columns = *array
            'rows'     : [['']]  # rows    = **array
            }
        # closest it gets to an enum in python
        # this is faster than comparing strings
        #----------------    
        self._action = 0 
        #   ------    
        self._query  = 0
        self._raw    = 1
        self._insert = 2
        self._alter  = 3
        self._update = 4
        self._create = 5
        self._max_aciton = 6
        #----------------

        self._query_results = (())
        self._query_item = ()
        self._saved_query = ""

        # make sure that the dictionary has all the default rows
        keys = list(self._config.keys())
        for k in keys:
            if k not in config:
                config[k] = self._config[k]
            
        self._config = config # now the user could omit keys and still get it working
        self.normalize_columns()
        self.normalize_rows()
    
    def connect_database(self):
        self._accessor = MySQLdb.connect(host=self._config['server'], user=self._config['username'], \
            password = self._config['password'],db=self._config['database'])
        
        return self
    
    # -------------------------------------------------------
    
    def row_count(self):
        cursor = (self._accessor.cursor())
        cursor.execute("SELECT COUNT(*) FROM {}.{}".format(self._config['database'],self._config['table'] )) 
        return int(cursor.fetchone()[0])
    
    def count(self, table=None):
        if table == None:
            table = self._config['table']
        else:
            self._config['table'] = table
        return self.row_count()
   
    def database(self,database):
        self._config['database'] = database; return self

    def table(self,table):
        self._config['table'] = table; return self

    def columns(self, *column_names): # give as *args or list
        self._config['columns'] = column_names
        self.normalize_columns(); return self

    def rows(self, *row_names): # send as *args, list or 2D list
        self._config['rows'] = row_names
        self.normalize_rows(); return self

    # -------------------------------------------------------

    #  query, raw, insert, alter, update, create, max_action

    def query(self):
        self._action = self._query; return self

    def raw(self):
        self._action = self._raw; return self
    
    def insert(self):
        self._action = self._insert; return self    
    def add_rows(self): # insert alias
        self._action = self._insert; return self

    def alter(self):
        self._action = self._alter; return self
    def modify_columns(self): # alter alias
        self._action = self._alter; return self
        
    def update(self):
        self._action = self._update; return self
    def modify_rows(self): # update alias
        self._action = self._update; return self

    def create(self): # TODO remove if not used
        self._action = self._create; return self

    # -------------------------------------------------------

    
    def action(self, columns=None, table=None, rows=None, mods='', raw=""):

        self.defaults(columns=columns, table=table, rows=rows)
        columns = self._config['columns']
        table = self._config['table']
        rows = self._config['rows']
        
        self.normalize_columns()
        self.normalize_rows()

        if len(columns) == 1: # means one word or '*'
            columns = columns[0]
        else:
            columns = ','.join(columns)

        cursor = self._accessor.cursor()
        def exe_qurey(cursor, query):
            cursor.execute(query)
            self._saved_query = query
            self._query_results = cursor.fetchall()
            self._query_item = cursor.fetchone()

        
        if self._action == self._query and raw == "":
            query = ("SELECT {} FROM {} {}".format(columns, table, mods))
            exe_qurey(cursor,query)
        elif self._action == self._raw:
            if raw != "":
                exe_qurey(cursor,raw)
            else:
                exe_qurey(cursor,self._saved_query)
        elif self._action == self._insert:
            for value_list in self.list_rows():
                if bool(re.search(r'[A-Za-z0-9]',str(value_list))) == False:
                    continue
                else:
                    value_list = "'"+re.sub(r'(?<=[^\\]),',"','",','.join(value_list))+"'"
                    command = "INSERT INTO {0} ({1}) VALUES({2})".format(table,columns,value_list)
                cursor.execute(command)
        elif self._action == self._update and mods != '':
            # update car set uuid = 'var1', image = 'var2' where pry_id = 1
            self.normalize_columns(); self.normalize_rows()
            for i in range(0,len(self._config['rows'])):
                set_rows = dict(zip(self._config['columns'],self._config['rows'][i]))
                set_rows_str = ''
                for k,v in set_rows.items():
                    set_rows_str += k + ' = "' + v + '", ' 
                command = "UPDATE {0} SET {1} {2}".format(table,set_rows_str[:-2:],mods) #; print(command)
                #print(command)
                cursor.execute(command)         
        elif self._action == self._alter:
            alter = "ALTER TABLE {} {}".format(table,mods)
            cursor.execute(alter)
            self._accessor.commit()
        
        self._accessor.commit()
        return self

    def normalize_columns(self): # *array
        modified_array = self._config['columns']
        if modified_array[0] == '' :
            return self


        if len(modified_array) == 1:
            if isinstance(modified_array, tuple):
                if len(modified_array) == 1:
                    modified_array = modified_array[0]
            if isinstance(modified_array, str):
                modified_array = (str(''.join(modified_array))).strip()
        
        if isinstance(modified_array, str):
            modified_array = (modified_array.split(','))

        if isinstance(modified_array[0], list): 
            # normalize if passed by *arg and not list
            modified_array = (modified_array[0])
        
        self._config['columns'] = modified_array


    def list_columns(self):
        for i in self._config['columns']:
            yield i
    
    
    def normalize_rows(self): # **array
        rows = self._config['rows']

        tuple(rows)
        if len(rows) == 1: # anti (**array,)
            rows = rows[0]

        if not isinstance(rows[0], list): # anti *array
            rows = [rows]


        if rows[-1::][0][0] == '': # last **array, first *array, first element
            tmp = rows[:-1:]
            rows = tmp


        #print('norm rows >> ' + str(rows))
        self._config['rows'] = rows

    def list_rows(self):
        for i in self._config['rows']:
            yield i

    
    def defaults(self, columns=columns, table=table, rows=rows):
        
        if columns == None:
            columns = self._config['columns']
        else:
            self.normalize_columns()
        
        if rows == None:
            rows = self._config['rows']
        else:
            self.normalize_rows()

        if table == None or table == '':
            table = self._config['table']        

    
    def saved_query(self):
        return self._saved_query
    
    def query_results(self):
        return self._query_results
    
    def query_item(self):
        return self._query_item
   
    def desc(self):
        cursor = (self._accessor.cursor())
        cursor.execute("DESC {}".format(self._config['table'])) 
        return cursor.fetchall()        

    
    def ret_columns(self):
        cursor = (self._accessor.cursor())
        cursor.execute("SHOW COLUMNS FROM {}".format(self._config['table']))
        return [x[0] for x in cursor.fetchall()]

    
    def yield_columns(self):
        cursor = (self._accessor.cursor())
        cursor.execute("SHOW COLUMNS FROM {}".format(self._config['table'])) 
        for i in [x [0] for x in cursor.fetchall()]:
            yield i

    
    def __call__(self,command):
        cursor = (self._accessor.cursor())
        cursor.execute(command)
        self._accessor.commit()
        self._query_results = cursor.fetchall()
        self._query_item = cursor.fetchone()
        return self

    
    def __str__(self):
        return (self._saved_query)

    
    def __eq__(self, other) -> bool:
        row_count = self.row_count()
        if row_count == other:
            return True
        else:
            False

    def __gt__(self, other) -> bool:
        if self.row_count() > other:
            return True
        else:
            False

    def __ge__(self, other) -> bool:
        if self.row_count() >= other:
            return True
        else:
            False
    
    def __lt__(self, other) -> bool:
        if self.row_count() < other:
            return True
        else:
            False

    def __le__(self, other) -> bool:
        if self.row_count() <= other:
            return True
        else:
            False
