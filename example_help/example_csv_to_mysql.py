#!/usr/bin/env python3

# The csv data was taken from Michael's How-To on Python's Pyramid Tutorials
# He converted the csv data to SQLite and then used SQLAlchemy
# One of the reasons I didn't like the course is that you won't see SQLite very much on heavy servers
# I made this example on my two libs
# One allows you to easily convert CSV data to Python Arrays
# the Second allows you to modify that data into your MySQL Database.
# I found this lib much easier to understand and use than SQLAlchemy

import uuid
import os, sys
from random import randint


from csv_parser import csv_parser
from mysql_controller import mysql_controller
# import my Libraries

pwd = os.popen('pwd').read().strip()
csv = csv_parser(pwd+"/car_stock.csv").parse_csv_file()
# Open the csv file and create the csv object for later ues with mysql

images = [
    'https://s1.cdn.autoevolution.com/images/models/OPEL_Vectra-GTS-2005_main.jpg',
    'http://www.opel.com/content/dam/Opel/OpelCorporate/corporate/nscwebsite/en/00_Home/252x142/'
    'Teaser_opel_group_252x142.jpg',
    'http://www.opel.com/content/dam/Opel/Europe/master/hq/en/15_ExperienceSection/02_AboutOpel/History_Heritage/'
    '1946_1970/Opel_Experience_History_Heritage_1953_Opel_Olympia_304x171_21754.jpg',
    'http://www.opel.co.za/content/dam/Opel/Europe/master/hq/en/15_ExperienceSection/11_Multimedia/02_Wallpaper/'
    'Opel_GT_992x425_16900.jpg',
    'https://pictures.topspeed.com/IMG/crop/201510/2016-opel-astra-tcr-16_600x0w.jpg',
    'https://1.bp.blogspot.com/-9l6kQpiC4nM/TnILNXWfUgI/AAAAAAAFEK4/70PPQSGCnmk/s1600/Opel-Rak-e-14.jpg',
    'http://www.opel.co.za/content/dam/Opel/Europe/master/hq/en/01_Vehicles/Concept_Cars/GT/Embargo/'
    'Opel_Concept_Cars_GT_1_1024x576_A298968.jpg',
]

# mysql_login = {
#     'username'  : 'scourge',
#     'password'  : 'NdneNVnewXne45DS',
#     'server'    : 'localhost',
#     'database'  : '',
#     'table'     : '',
#     'columns'   : [''],
#     'values'    : [['']] # a matrix of values
#     }

mysql_login = {
    'username' : 'scourge',
    'password' : 'NdneNVnewXne45DS',
    'server'   : 'localhost',
}
# create your login
# Any non-added keys will be atomatically added in the __init__
mysql = mysql_controller(mysql_login).connect_database()
# We __init__ with teh mysql_login and then connect to mysql


# drop the database named rest_pyramid if you have one already created
try:
    mysql.database('rest_pyramid').connect_database() # connect to new database
    mysql("DROP TABLE car") # delete car table
except:
    print("You didn't have the table car table and/or 'rest_pyramid' database")
    try:
        mysql('CREATE DATABASE rest_pyramid').database('rest_pyramid').connect_database()
    except:
        print('You have the rest_pyramid database')



# use __call__ for misc functions, here it is used to create a database
# we then select that database and then connect to it
# the first time we used connect_database() was to log into Mysql
# we had to use it again becaues we changed from having no database selected to now having one selected


create_table = """
create table car (
    pry_id int(11) unsigned auto_increment primary key not null,
    brand varchar(25) not null,
    name varchar(300) not null,
    price int(11) not null,
    year int(11) not null,
    damage varchar(300),
    last_seen varchar(25) not null
)
"""

mysql.table('car')(create_table)
# here we selected a new table 'car' and we use __call__ to create the table


mysql.add_rows().columns(csv.headers()).rows(csv.rows()).action()
# we have several ways we can modify our database easily (asside from __call__)
# 1. add_rows()
# 2. modify_columns()
# 3. modify_rows()
# we can then use query() to view the results
# here we chose add_rows() and used our *headers and **rows from csv object
# we then finalize it with action to execute the command which will 
# build all of our table rows from our csv object


print("table row count = " + str(mysql.query().row_count()))
# we are going to switch to query() mode from add_rows() mode and find the size
# of our table via row_count(). We use operator overloading as shorthand to
# avoid needing to use the .row_count()

print (mysql.action(columns="brand,name",mods="WHERE pry_id = 15").query_results())
# with query() still in used, we will see what the brand and the name of 
# the car is where the column 'pry_id' is equal to 15.
# note, we can use **kwargs to also easily change the columns/rows, note that
# mods (usually a where clause) will not be kept. We use query_results() to see
# everything that is returned to us. The alternative is query_item() which would
# be the first item in the list

print (mysql.saved_query()) # show last used query
print(str(mysql)) # use the __str__ as a shortcut

print (mysql.raw().action(raw=str(mysql)).query_item()) 
# raw is used for using a raw query (without the help of the mysql_parser)
# __call__ is used for database modification, .raw() is used for making queries
# we pass action() the **kwargs key of raw and the raw string 
# for sake of ease, we will just use our prviously used string from __str__
# then we will print ou the query results  but only the first item sense
# we only have one primary key per row.

print(mysql.ret_columns()) # lets see what our columns look like

# lets add a couple of columns if they are not already there
# we can do this with modify_columns() selected and passing the modifier to action()
if 'uuid' not in mysql.ret_columns(): 
    mysql.modify_columns().action(mods="ADD COLUMN uuid VARCHAR(36) AFTER pry_id;")

if 'image' not in mysql.ret_columns():
    mysql.action(mods="ADD COLUMN image VARCHAR(256) AFTER last_seen")

print(mysql.ret_columns()) # lets view our new columns


mysql.modify_rows().table('car').columns("uuid,image")
# now lets modify_rows() to give new values to the new columsn we just made

# note: columns/rows can be given via an array or csv format
count = 0
while mysql > count-1:
    # operator overloading always makes comparisons with the "row_count()"
    # for added speed use row_count to calculate the num of rows and send to a variable
    # then use "while row_count_var > count-1" so you don't need to keep making the "COUNT" query
    mysql.rows([str(uuid.uuid4()),images[int(randint(0,6))]]).action(mods="WHERE pry_id = {0}".format(count))
    count += 1
    # within each loop we set each column value to a random uuid and a random image
    # then we set the where clause to reflect our count
    # by the end of this loop, every row will have a random uuid and a random image added to their values


print(mysql.query().action(columns='*',mods=' WHERE pry_id = 5').query_item())
# here I decided to just add in the columns I wanted in the action **kwargs
# and you will see here on eof the items you updated with the two new columns
# and the new values placed into those
