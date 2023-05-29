import sqlite3
import pandas 
conn = sqlite3.connect('database.sqlite')

tables = pandas.read_sql("""SELECT *
                        FROM sqlite_master
                        WHERE type='table';""", conn)
tables
