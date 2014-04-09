import glob
import os
import pickle
import threading
import time
from collections import namedtuple
from mmap import mmap
from pybloom import ScalableBloomFilter

Gadget = namedtuple( "Gadget", [ "offset", "size" ] )

class MemoryStore( object ):
  def __init__( self, name, size=1024, dataDir="" ):
    self.dataDir = dataDir
    self.size = size
    self.f = None
    if( size != None ):
      self.f = open( os.path.join( dataDir, "%s.data" % name ), "w+b" )
      f = self.f
      f.seek( size - 1 )
      f.write( "\0" )
      f.flush()
      self.memory = mmap( f.fileno(), size )
    else:
      self.memory = None

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
      return self.memory[offset:offset + size]

  def close( self ):
    self.memory.close()
    if( not self.f == None ):
      self.f.close()

  @classmethod
  def fromfile( cls, name, size=1024, dataDir="" ):
    ret = cls( name, size=None, dataDir=dataDir )
    f = open( os.path.join( dataDir, "%s.data" % name ), "r+b" )
    ret.memory = mmap( f.fileno(), size )
    ret.size = size

    return ret

class GadgetBox( object ):
  def __init__( self, name, size, dataDir="" ):
    self.dataDir = dataDir
    self.filter = ScalableBloomFilter( mode=ScalableBloomFilter.SMALL_SET_GROWTH )
    self.memory = None
    self.keys = None
    self.name = name
    self.size = size
    self.loadGadgets()

  def persist( self, keys, memory ):
    with open( os.path.join( self.dataDir, "%s.bloom" % self.name ), "wb" ) as f:
      self.filter.tofile( f )

    with open( os.path.join( self.dataDir, "%s.meta" % self.name ), "wb" ) as f:
      pickle.dump( keys, f )

    return self

  def get( self, key ):
    if( self.memory == None ):
      self.memory = MemoryStore.fromfile( self.name, size=self.size, dataDir=self.dataDir )
    ret = self.memory.get( key.offset, key.size )

    return ret

  def putGadget( self, key, gadget ):
    self.keys[key] = gadget

  def put( self, data ):
    if( self.memory == None ):
      self.memory = MemoryStore( self.name, size=self.size, dataDir=self.dataDir )
    return self.memory.put( data )

  def close( self ):
    if( self.memory != None ):
      self.memory.close()
    self.memory = None

  def loadGadgets( self ):
    if( self.keys == None ):
      try:
        with open( os.path.join( self.dataDir, "%s.meta" % self.name ), "rb" ) as f:
          self.keys = pickle.load( f )
          self.keys = dict( self.keys )
      except IOError:
        self.keys = {}

    return self.keys
      
class GadgetBoxFactory( object ):
  def __init__( self, name, bufferSize=1024, dataDir="" ):
    self.boxes = []
    self.dataDir = dataDir
    self.name = name
    self.size = bufferSize

  def next( self ):
    self.boxes.append( GadgetBox( "%s-%d" % ( self.name, len( self.boxes ) ), self.size, dataDir=self.dataDir ) )
    return self.boxes[-1] 

  @classmethod
  def fromfiles( cls, name, bufferSize=1024, dataDir="" ):
    bloomFiles = glob.glob( os.path.join( dataDir, "%s-*.bloom" % name ) )
    dataFiles = glob.glob( os.path.join( dataDir, "%s-*.data" % name ) )

    if( len( bloomFiles ) == 0 ):
      return None

    factory = cls( name, bufferSize=bufferSize, dataDir=dataDir )

    for i in range( len( dataFiles ) ): 
      b, d = bloomFiles[i], dataFiles[i]
      box = GadgetBox( b.split( "." )[0].split( os.sep )[-1], bufferSize, dataDir=dataDir )
      with open( os.path.join( dataDir, b ), "rb" ) as f:
        box.filter = ScalableBloomFilter.fromfile( f )

      factory.boxes.append( box )

    return factory

class GadgetTable( object ):
  def __init__( self, name, bufferSize=1024, dataDir="" ):
    self.boxes = GadgetBoxFactory( name, bufferSize=bufferSize )
    self.bufferSize = bufferSize
    self.currentBox = self.boxes.next()
    self.dataDir = dataDir
    self.name = name

  def put( self, key, data ):
    offset = self.currentBox.put( data )
    if( offset < 0 ):
      self.persist()
      self.currentBox.close()
      self.currentBox = self.boxes.next()
      offset = self.currentBox.put( data )

    self.currentBox.filter.add( key )

    self.currentBox.putGadget( key, Gadget( offset, len( data ) ) )
    
  def get( self, key ):
    if( not key in self.currentBox.loadGadgets() ):
      return self._find_in_boxes( key )
    else:
      g = self.currentBox.loadGadgets()[key]
      data = self.currentBox.get( g )
      return data

  def _find_in_boxes( self, key ):
    ret = None
    for b in self.boxes.boxes:
      if( key in b.filter ):
        keys = b.loadGadgets() 
        if( key in keys ):
          ret = b.get( keys[key] )
          b.close()
          break
        b.close()

    return ret

  def persist( self ):
    self._flush()

  def close( self ):
    self.persist()
    self.currentBox.close()

  @classmethod
  def fromfiles( cls, name, bufferSize=1024, dataDir="" ):
    self = cls( name, bufferSize=bufferSize, dataDir=dataDir )
    self.boxes = GadgetBoxFactory.fromfiles( name, bufferSize=bufferSize, dataDir=dataDir )
    self.currentBox = self.boxes.boxes[-1]
    self.gadgets = self.currentBox.loadGadgets()
    self.memory = self.currentBox.memory
    return self

  def _flush( self ):
    keys = self.currentBox.loadGadgets()
    self.currentBox.persist( [ ( key, self.currentBox.loadGadgets()[key] ) for key in keys ], self.currentBox.memory )
 
class GadgetTableCollection( object ):
  def __init__( self, dataDir="", interval=120 ):
    self._persistEvent = threading.Event()
    self._running = threading.Event()
    self.dataDir = dataDir
    self.interval = interval

    self.tables = {}

    #class GadgetPersistThread( threading.Thread ):
    #  def __init__( self, collection ):
    #    super( GadgetPersistThread, self ).__init__()
    #    self.collection = collection
    #    self.daemon = True
    #
    #  def run( self ):
    #    self.collection._persist()
    #    self.collection.close()
    #
    #self.persistThread = GadgetPersistThread( self )
    #self.persistThread.start()

  def create( self, name, size ):
    try:
      table = GadgetTable.fromfiles( name, bufferSize=size, dataDir=self.dataDir )
      if( not table == None ):
        self.tables[name] =  table
    except Exception:
      self.tables[name] = GadgetTable( os.path.join( self.dataDir, name ), bufferSize=size, dataDir=self.dataDir )

  def put( self, table, key, value ):
    self.tables[table].put( key, value )

  def get( self, table, key ):
    return self.tables[table].get( key )

  def persist( self, table ):
    self.tables[table].persist()

  def close( self ):
    #self._running.set()

    for table in self.tables.values():
      table.close()

  def _persist( self ):
    while not self._running.is_set():
      time.sleep( self.interval )
      self._persistEvent.set()
 
      for table in self.tables.values():
        table.persist()

      self._persistEvent.clear()

