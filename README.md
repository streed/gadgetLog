gadgetStore
=========

Simple Log Based Database

The system uses two stores. The first of which is a in memory block of memory a long with a dictionary. The block of memory is of variable size and stays constant. The dictionary
holds the offsets and sizes of values inside of this memory. Data is also stored inside of a bloom filter for quicker lookups. 

Data is persisted to disc as a raw binary file. 

Each of the files makes up a block and a database is made up of many blocks, each block is the same size.

The following process occurs when a key is looked up. It is first looked for within the in memory dictionary and if it is found the value is retreived from the in memory block.

If the value is not found in memory then the other blocks, if there are any, need to be searched. To speed this up blocks have their bloom filters cached in memory as well because they
are trivially small. These bloom filters are used to see whether a key is in that block or not. If the key is found to be in the block then the block is read into memory a long with its
associated dictionary. Then a better check is performed to see whether the block contains the key or not, if it is then the value is read and returned. This process happens through all
of the blocks.

```python

collection = GadgetTableCollection( dataDir="/tmp" )

table = collection.create( "test" )

table.put( "test", "This is some test data" )

table.persist()

collection.close()
```

The above code retreives the _test_ table and then puts a key _test_ into the table with the data _This is some test data_.

```python

collection = GadgetTableCollection( dataDir="/tmp" )

table = collection.create( "test" )

print table.get( "test" )

collection.close()
```

The above will open up the same _test_ table and then retreive the _test_ key which will then print out the _This is some test data_.

