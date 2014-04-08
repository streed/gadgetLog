from gadgetLog.gadget import GadgetMemoryTable

g = GadgetMemoryTable( "test", bufferSize=50 )

g.put( "test", "this is a giant test" )
g.put( "pickles", "here are some pickles" )
g.put( "tickles", "There are a lot of tickles" )

g.persist()

gg = GadgetMemoryTable.fromfiles( "test", bufferSize=50 )

print gg.get( "tickles" )
print gg.get( "pickles" )
print gg.get( "test" )
print gg.get( "fake" )
gg.put( "fake", "fakeness" )
gg.persist()
