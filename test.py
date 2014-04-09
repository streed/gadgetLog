from gadgetStore.gadget import GadgetTableCollection
import random

store = GadgetTableCollection( dataDir="/tmp/gadget", interval=120 )

store.create( "test", 1024 * 100 )

for i in range( 100000 ):
  store.put( "test", "%d" % i, "%d" % i )


#for i in [random.randint(0, 100000) for r in xrange(100)]:
#  v = store.get( "test", "%d" % i )
#  print i, v
print store.get( "test", "55123" )
store.close()
