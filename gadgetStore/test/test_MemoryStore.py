import unittest

from ..gadget import MemoryStore

class TestMemoryStore( unittest.TestCase ):

  def test_MemoryStore_creation( self ):
    m = MemoryStore( "test", 128 )

    self.assertEquals( 0, m.memory.tell() )

  def test_MemoryStore_put_simple_data( self ):
    m = MemoryStore( "test", 128 )

    offset = m.put( "test" )

    self.assertEquals( 0, offset )

    offset = m.put( "test" )

    self.assertEquals( 4, offset )

  def test_MemoryStore_put_too_large( self ):
    m = MemoryStore( "test", 10 )

    offset = m.put( "testtesttest" )

    self.assertEquals( -1, offset )

  def test_MemoryStore_get( self ):
    m = MemoryStore( "test", 128 )

    for i in range( 10 ):
      m.put( "test" )

    data = m.get( 4, 4 )

    self.assertEquals( "test", data )

    data = m.get( 8, 8 )
    self.assertEquals( "testtest", data )

  def test_MemoryStore_read_past_size( self ):
    m = MemoryStore( "test", 10 )

    data = m.get( 5, 5 )

    self.assertEquals( None, data )

    data = m.get( 10, 10 )
    self.assertEquals( None, data )
