from gadgetStore.gadget import GadgetTableCollection
import random

def write():
  store = GadgetTableCollection( dataDir="/tmp/gadget", interval=120 )
  table = store.create( "test", 1024 * 1024 * 16 )
  total = 10000000
  for i in xrange( total ):
    table.put( i, i )

  store.close()

def read():
  store = GadgetTableCollection( dataDir="/tmp/gadget", interval=120 )
  table = store.create( "test", 1024 * 1024 * 16 )
  total = 10000000
  find = 1000000
  for i in (random.randint(0, total ) for r in xrange(find)):
    v = table.get( i )
    print i, v

  #store.close()

if __name__ == "__main__":
  import timeit
  print(timeit.timeit( "write()", setup="from __main__ import write", number=1))
  #print(timeit.timeit("read()", setup="from __main__ import read", number=1))
