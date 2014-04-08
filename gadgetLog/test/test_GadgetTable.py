import unittest

from ..gadget import GadgetMemoryTable

class TestGadgetMemoryTable( unittest.TestCase ):
  def test_GadgetMemory_creates_memmap_correctly( self ):
    m = GadgetMemoryTable( "test" )

    self.assertTrue( m.memory != None )

  def test_GagetTable_put( self ):
    m = GadgetMemoryTable( "test" )

    m.put( "test", "test string" )

    self.assertTrue( "test" in m.gadgets )

  def test_GadgetMemoryTable_get( self ):
    m = GadgetMemoryTable( "test" )

    m.put( "test", "test string" )

    self.assertTrue( "test string", m.get( "test" ) )

