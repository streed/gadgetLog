import unittest

from ..gadget import GadgetTable

class TestGadgetTable( unittest.TestCase ):
  def test_Gadget_creates_memmap_correctly( self ):
    m = GadgetTable( "test" )

    self.assertTrue( m.store != None )

  def test_GagetTable_put( self ):
    m = GadgetTable( "test" )

    m.put( "test", "test string" )

    self.assertTrue( "test" in m.gadgets )

  def test_GadgetTable_get( self ):
    m = GadgetTable( "test" )

    m.put( "test", "test string" )

    self.assertTrue( "test string", m.get( "test" ) )

