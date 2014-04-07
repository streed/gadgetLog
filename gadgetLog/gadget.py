from mmap import mmap
from collections import namedtuple

Gadget = namedtuple( "Gadget", [ "offset", "size" ] )

class MemoryStore( object ):
  def __init__( self, name, size=1024 ):
    self.size = size
    self.memory = mmap( -1, size )

  def put( self, data ):
    if( len( data ) + self.memory.tell() < self.size ):
      self.memory.write( data )
      return self.memory.tell() - len( data ) 
    else:
      return -1

  def get( self, offset, size ):
    if( size >= self.size or ( offset + size ) >= self.size ):
      return None
    else:
      old = self.memory.tell()
      self.memory.seek( offset )
      data = self.memory.read( size )
      self.memory.seek( old )

      return data

class GadgetTable( object ):
  def __init__( self, name, bufferSize=1024, output=MemoryStore ):
    self.store = output(  name, size=bufferSize )
    self.gadgets = {}

  def put( self, key, data ):
    offset = self.store.put( data )

    self.gadgets[key] = Gadget( offset, len( data ) )
    
  def get( self, key ):
    if( not key in self.gadgets ):
      return None
    else:
      g = self.gadgets[key]
      data = self.store.get( g.offset, g.size )

      return data
