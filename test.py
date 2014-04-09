from gadgetLog.gadget import GadgetTableCollection

store = GadgetTableCollection( dataDir="/tmp/gadget", interval=60 )

store.create( "test", 128 )
#store.put( "test", "test", "Test" )
print store.get( "test", "test" )
#store.put( "test", "test2", "This is a test" )
print store.get( "test", "test2" )

