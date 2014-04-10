from gadgetStore.gadget import GadgetTableCollection
import random

store = GadgetTableCollection( dataDir="/tmp/gadget", interval=120 )

#store.create( "test", 1024 * 1024 )

total = 20000

#for i in range( total ):
#  store.put( "test", i, i )

#print store.get( "test", "55123" )
#store.close()
store.create( "test", 1024 * 1024 )
for i in [random.randint(0, total ) for r in xrange(100)]:
  v = store.get( "test", "%d" % i )
  print i, v
