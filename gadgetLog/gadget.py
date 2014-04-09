import os
import pickle
import glob
from mmap import mmap
from collections import namedtuple
from pybloom import ScalableBloomFilter

Gadget = namedtuple( "Gadget", [ "offset", "size" ] )

class MemoryStore( object ):
  def __init__( self, name, size=1024, dataDir="" ):
    self.dataDir = dataDir
    self.size = size
    if( size != None ):
      self.memory = mmap( -1, size )
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

  @classmethod
  def fromfile( cls, name, size=1024, dataDir="" ):
    ret = cls( name, size=None, dataDir=dataDir )
    f = open( os.path.join( dataDir, "%s.data" % name ), "r+b" )
    data = f.read( size )
    ret.memory = mmap( -1, size )
    ret.memory.write( data )
    ret.size = size

    return ret

class GadgetBox( object ):
  def __init__( self, name, size, dataDir="" ):
    self.dataDir = dataDir
    self.size = size
    self.name = name
    self.filter = ScalableBloomFilter( mode=ScalableBloomFilter.SMALL_SET_GROWTH )
    self.handle = None
    self.keys = None

  def persist( self, keys, memory ):
    self.filter.tofile( open( os.path.join( self.dataDir, "%s.bloom" % self.name ), "wb" ) )
    keys2 = []
    with open( os.path.join( self.dataDir, "%s.data" % self.name ), "wb" ) as f:
      for k in keys:
        f.write( memory.get( k[1].offset, k[1].size ) )
        newOffset = f.tell()
        keys2.append( ( k[0], Gadget( newOffset - k[1].size, k[1].size ) ) )

    pickle.dump( keys2, open( os.path.join( self.dataDir, "%s.meta" % self.name ), "wb" ) )

    return self

  def get( self, key, size ):
    if( self.handle == None ):
      self.handle = MemoryStore.fromfile( self.name, size=self.size, dataDir=self.dataDir )
    ret = self.handle.get( key.offset, key.size )

    return ret

  def loadGadgets( self ):
    if( self.keys == None ):
      self.keys = pickle.load( open( os.path.join( self.dataDir, "%s.meta" % self.name ), "rb" ) )
      self.keys = dict( self.keys )

    return self.keys
      
class GadgetBoxFactory( object ):
  def __init__( self, name, bufferSize=1024, dataDir="" ):
    self.boxes = []
    self.name = name
    self.size = bufferSize
    self.dataDir = dataDir

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
      box.filter = ScalableBloomFilter.fromfile( open( os.path.join( dataDir, b ), "rb" ) )

      factory.boxes.append( box )

    return factory

class GadgetTable( object ):
  def __init__( self, name, bufferSize=1024, dataDir="" ):
    self.dataDir = dataDir
    self.name = name
    self.bufferSize = bufferSize
    self.memory = MemoryStore( name, size=bufferSize )
    self.gadgets = {}
    self.boxes = GadgetBoxFactory( name, bufferSize=bufferSize )
    self.currentBox = self.boxes.next()

  def put( self, key, data ):
    offset = self.memory.put( data )
    if( offset < 0 ):
      self.persist()
      self.memory.close()
      self.memory = MemoryStore( self.name, size=self.bufferSize )
      self.gadgets = {}
      offset = self.memory.put( data )
      self.currentBox = self.boxes.next()
    else:
      self.currentBox.filter.add( key )

    self.gadgets[key] = Gadget( offset, len( data ) )
    
  def get( self, key ):
    if( not key in self.currentBox.loadGadgets() ):
      return self._find_in_boxes( key )
    else:
      g = self.currentBox.loadGadgets()[key]
      data = self.memory.get( g.offset, g.size )
      return data

  def _find_in_boxes( self, key ):
    ret = None
    for b in self.boxes.boxes:
      if( key in b.filter ):
        keys = b.loadGadgets() 
        if( key in keys ):
          ret = b.get( keys[key], self.bufferSize )
          break

    return ret

  def persist( self ):
    self._flush()

  @classmethod
  def fromfiles( cls, name, bufferSize=1024, dataDir="" ):
    self = cls( name, bufferSize=bufferSize, dataDir=dataDir )
    self.boxes = GadgetBoxFactory.fromfiles( name, bufferSize=bufferSize, dataDir=dataDir )
    self.currentBox = self.boxes.boxes[-1]
    self.gadgets = self.currentBox.loadGadgets()
    self.memory = MemoryStore.fromfile( self.currentBox.name, size=bufferSize, dataDir=dataDir )
    return self

  def _flush( self ):
    keys = self.gadgets.keys()
    keys.sort()
    self.currentBox.persist( [ ( key, self.gadgets[key] ) for key in keys ], self.memory )
 
class GadgetTableCollection( object ):
  def __init__( self, dataDir="", interval=120 ):
    self.dataDir = dataDir
    self.interval = interval

    self.tables = {}

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

