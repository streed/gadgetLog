from gadgetLog.gadget import GadgetMemoryTable

"""g = GadgetMemoryTable( "test", bufferSize=50 )

g.put( "test", "this is a giant test" )
g.put( "pickles", "here are some pickles" )
g.put( "tickles", "There are a lot of tickles" )

g.persist()
"""
gg = GadgetMemoryTable.fromfiles( "test", bufferSize=64000 )

for i in range( 0, 10000 ):
  print gg.get( "%d" % i )

for i in range( 10000, 20000 ):
  print gg.get( "%d" % i )

gg.persist()

