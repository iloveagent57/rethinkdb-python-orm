rethinkdb-python-orm
====================

A super-light RethinkdDB object-mapper for python.

Usage
=====

from models import Model, Attribute
import rethinkdb

connection = rethinkdb.connect()
rethinkdb.table_create('myobjects').run(connection)

class MyObject(Model):
  name = Attribute()
  color = Attribute()
    
with Model.connection(connection):
  my_obj = MyObject(name="Charlie", color="Brown")
  my_obj.save()
  print my_obj
