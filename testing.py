import sqlite3 #imprort sqlite3 library to import sql3 file
import pandas #import pandas library for sql queries
import numpy as np #import numpy library
conn = sqlite3.connect('database.sqlite') #Connact to soccer database

tables = pandas.read_sql("""SELECT *
                        FROM sqlite_master
                        WHERE type='table';""", conn) #running tested queries
tables