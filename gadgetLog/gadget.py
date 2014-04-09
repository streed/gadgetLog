import pickle
import glob
from mmap import mmap
from collections import namedtuple
from pybloom import ScalableBloomFilter

Gadget = namedtuple( "Gadget", [ "offset", "size" ] )

class MemoryStore( object ):
  def __init__( self, name, size=1024 ):
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
  def fromfile( cls, name, size=1024 ):
    ret = cls( name, size=None )
    f = open( "%s.data" % name, "r+b" )
    data = f.read( size )
    ret.memory = mmap( -1, size )
    ret.memory.write( data )
    ret.size = size

    return ret

class GadgetBox( object ):
  def __init__( self, name, size ):
    self.size = size
    self.name = name
    self.filter = ScalableBloomFilter( mode=ScalableBloomFilter.SMALL_SET_GROWTH )
    self.handle = None
    self.keys = None

  def persist( self, keys, memory ):
    self.filter.tofile( open( "%s.bloom" % self.name, "wb" ) )
    keys2 = []
    with open( "%s.data" % self.name, "wb" ) as f:
      for k in keys:
        f.write( memory.get( k[1].offset, k[1].size ) )
        newOffset = f.tell()
        keys2.append( ( k[0], Gadget( newOffset - k[1].size, k[1].size ) ) )

    pickle.dump( keys2, open( "%s.meta" % self.name, "wb" ) )

    return self

  def get( self, key, size ):
    if( self.handle == None ):
      self.handle = MemoryStore.fromfile( self.name, size=self.size )
    ret = self.handle.get( key.offset, key.size )

    return ret

  def loadGadgets( self ):
    if( self.keys == None ):
      self.keys = pickle.load( open( "%s.meta" % self.name, "rb" ) )
      self.keys = dict( self.keys )

    return self.keys
      
class GadgetBoxFactory( object ):
  def __init__( self, name, bufferSize=1024 ):
    self.boxes = []
    self.name = name
    self.size = bufferSize

  def next( self ):
    self.boxes.append( GadgetBox( "%s-%d" % ( self.name, len( self.boxes ) ), self.size ) )
    return self.boxes[-1] 

  @classmethod
  def fromfiles( cls, name, bufferSize=1024 ):
    bloomFiles = glob.glob( "%s-*.bloom" % name )
    dataFiles = glob.glob( "%s-*.data" % name )

    factory = cls( name )

    for i in range( len( dataFiles ) ): 
      b, d = bloomFiles[i], dataFiles[i]
      box = GadgetBox( b.split( "." )[0], bufferSize )
      box.filter = ScalableBloomFilter.fromfile( open( b, "rb" ) )

      factory.boxes.append( box )

    return factory

class GadgetMemoryTable( object ):
  def __init__( self, name, bufferSize=1024 ):
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
    if( not key in self.gadgets ):
      return self._find_in_boxes( key )
    else:
      g = self.gadgets[key]
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
  def fromfiles( cls, name, bufferSize=1024 ):
    self = cls( name, bufferSize=bufferSize )
    self.boxes = GadgetBoxFactory.fromfiles( name, bufferSize=bufferSize )
    self.currentBox = self.boxes.boxes[-1]
    self.gadgets = self.currentBox.loadGadgets()
    self.memory = MemoryStore.fromfile( self.currentBox.name, size=bufferSize )
    return self

  def _flush( self ):
    keys = self.gadgets.keys()
    keys.sort()
    self.currentBox.persist( [ ( key, self.gadgets[key] ) for key in keys ], self.memory )
 
