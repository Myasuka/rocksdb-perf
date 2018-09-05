# rocksdb-perf
Performance benchmark for RocksDB differnet versions (4.2.0 vs 5.7.5)

# Usage
* Build the package. 
`mvn clean package`

* Run the jar package. 
For jar package at `rocksdb-4.2.0/target/rocksdb-4.2.0-1.0-SNAPSHOT.jar` and `rocksdb-5.7.5/target/rocksdb-5.7.5-1.0-SNAPSHOT.jar`,
run command:
~~~
    `java jar rocksdb-*-1.0-SNAPSHOT.jar [-dataPath <data path>] [-numKeys <num of keys>] [-queryTimes <times of query>]`
~~~
The default data path is current working dir, default num of keys is 10000000 and the default query times is 3.

# Some statics
At a linux machine of 64 cores with 256GB memory, running with following configuration and statics.
~~~
RocksDB 5.7.5
   configuration:
        rocksdb.pin-l0-index-filter: true
        rocksdb.thread.compactions: 2
        rocksdb.compaction.level1-file-target-size: 64 mb
        rocksdb.block.cache-size: 256 mb
        rocksdb.thread.flushes: 2
        rocksdb.compaction.level1-max-size: 512 mb
        rocksdb.block.cache-index-filter: true
        rocksdb.writebuffer.size: 32 mb
        rocksdb.compaction.levels: 4
        rocksdb.inplace-update: false
        rocksdb.compaction.level0-files-num-trigger: 4
        rocksdb.block.block-size: 4 kb
        rocksdb.writebuffer.number: 4
        rocksdb.compaction.type: snappy
        rocksdb.index-type: kBinarySearch
        rocksdb.ttl: 3 d
        rocksdb.optimize-filter-hits: true
  
  STATICS for 15000000 key-value pairs put into rocksDB, consumed 53.345 seconds    
        blockCache Hit: 20
        blockCache Miss: 215011
        blockCacheIndex Hit: 20
        blockCacheIndex Miss: 41
        blockCacheFilter Hit: 0
        blockCacheFilter Miss: 41
        blockCacheData Hit: 0
        blockCacheData Miss: 214929
        memTable Hit: 0
        memTable Miss: 0
        numKeys read: 0
        numKeys written: 15000000
        numKeys updated: 0
  
  STATICS for get 15000000 key-value pairs randomly from rocksdb, consumed 74.484 seconds
        blockCache Hit: 45699058
        blockCache Miss: 158385
        blockCacheIndex Hit: 14792248
        blockCacheIndex Miss: 5
        blockCacheFilter Hit: 15623094
        blockCacheFilter Miss: 5
        blockCacheData Hit: 15283716
        blockCacheData Miss: 158375
        memTable Hit: 0
        memTable Miss: 15000000
        numKeys read: 15000000
        numKeys written: 0
        numKeys updated: 0

RocksDB 4.2.0   
  configuration:
        rocksdb.thread.compactions: 2
        rocksdb.compaction.level1-file-target-size: 64 mb
        rocksdb.block.cache-size: 256 mb
        rocksdb.thread.flushes: 2
        rocksdb.compaction.level1-max-size: 512 mb
        rocksdb.block.cache-index-filter: true
        rocksdb.writebuffer.size: 32 mb
        rocksdb.compaction.levels: 4
        rocksdb.inplace-update: false
        rocksdb.compaction.level0-files-num-trigger: 4
        rocksdb.block.block-size: 4 kb
        rocksdb.writebuffer.number: 4
        rocksdb.compaction.type: snappy
        rocksdb.index-type: kBinarySearch
        rocksdb.ttl: 3 d
        rocksdb.optimize-filter-hits: false
  
  STATICS for 15000000 key-value pairs put into rocksDB, consumed 50.86 seconds    
        blockCache Hit: 89
        blockCache Miss: 294945
        blockCacheIndex Hit: 89
        blockCacheIndex Miss: 47
        blockCacheFilter Hit: 0
        blockCacheFilter Miss: 47
        blockCacheData Hit: 0
        blockCacheData Miss: 294851
        memTable Hit: 92260395
        memTable Miss: 0
        numKeys read: 0
        numKeys written: 0
        numKeys updated: 15000000
        
  STATICS for get 15000000 key-value pairs randomly from rocksdb, consumed 63.964 seconds:
        blockCache Hit: 96870851
        blockCache Miss: 170525
        blockCacheIndex Hit: 15597897
        blockCacheIndex Miss: 5
        blockCacheFilter Hit: 65775939
        blockCacheFilter Miss: 5
        blockCacheData Hit: 15497015
        blockCacheData Miss: 170515
        memTable Hit: 448881433
        memTable Miss: 50178050
        numKeys read: 0
        numKeys written: 0
        numKeys updated: 0
~~~
