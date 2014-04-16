from gadgetStore.gadget import GadgetTableCollection
import random

store = GadgetTableCollection( dataDir="/tmp/gadget", interval=120 )

#store.create( "test", 1024 * 1024 * 16 )

total = 1000000

#for i in range( total ):
#  if i % 10000 == 0:
#    print i
#  store.put( "test", i, i )

#store.close()
store.create( "test", 1024 * 1024 * 16 )
for i in [random.randint(0, total ) for r in xrange(total)]:
  v = store.get( "test", "%d" % i )
  print i, v

#store.close()
