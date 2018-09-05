/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.myasuka.rocksdb;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.Preconditions;

import com.myasuka.rocksdb.common.StateAccessException;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.Checkpoint;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompactionStyle;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.TickerType;
import org.rocksdb.TtlDB;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * A DB instance wrapper of {@link RocksDB}.
 */
public class RocksDBInstance implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(RocksDBInstance.class);

    /** The name of the merge operator in RocksDB. Do not change except you know exactly what you do. */
    static final String MERGE_OPERATOR_NAME = "stringappendtest";

    /** The DB options. */
    private final DBOptions dbOptions;

    /** The column family options. */
    private final ColumnFamilyOptions columnOptions;

    /**
     * Our RocksDB database, this is used to store state.
     * The different k/v states that we have don't each have their own RocksDB instance.
     */
    private RocksDB db;

    /** The write options to use in the states. We disable write ahead logging. */
    private final WriteOptions writeOptions;

    private final ColumnFamilyHandle defaultColumnFamily;

    private long lastStaticsTimeMills;

    private long blockCacheHit = 0;
    private long blockCacheMiss = 0;

    private long blockCacheIndexHit = 0;
    private long blockCacheIndexMiss = 0;

    private long blockCacheFilterHit = 0;
    private long blockCacheFilterMiss = 0;

    private long blockCacheDataHit = 0;
    private long blockCacheDataMiss = 0;

    private long memTableHit = 0;
    private long memTableMiss = 0;

    private long numKeysRead = 0;
    private long numKeysWritten = 0;
    private long numKeysUpdated = 0;

    /**
     * Creates a rocksDB instance with given options, ttlSeconds and the instance path for rocksDB.
     *
     * @param conf The configuration for RocksDB.
     * @param instanceRocksDBPath The DB path used to create the rocksDB.
     * @throws RocksDBException Throws when failing to create the rocksDB instance.
     */
    RocksDBInstance(RocksDBConfiguration conf, Path instanceRocksDBPath) throws RocksDBException {

        this.dbOptions = new DBOptions()
                .setUseFsync(false)
                .setMaxOpenFiles(-1)
                .setMaxBackgroundCompactions(conf.getMaxBackgroundCompactions())
                .setMaxBackgroundFlushes(conf.getMaxBackgroundFlushes())
                .createStatistics()
                .setStatsDumpPeriodSec(60)
                .setCreateIfMissing(true);
        dbOptions.setDbLogDir(instanceRocksDBPath.getParent().toString());

        this.columnOptions = new ColumnFamilyOptions()
                .setCompactionStyle(CompactionStyle.LEVEL)
                .setLevelCompactionDynamicLevelBytes(true)
                .setTargetFileSizeBase(conf.getLevel1FileTargetSize())
                .setMaxBytesForLevelBase(conf.getLevel1MaxSize())
                .setInplaceUpdateSupport(conf.getInplaceUpdate())
                .setWriteBufferSize(conf.getWriteBufferSize())
                .setMaxWriteBufferNumber(conf.getMaxWriteBufferNumber())
                .setOptimizeFiltersForHits(conf.getOptimizeFiltersForHits())
                .setLevelZeroFileNumCompactionTrigger(conf.getLevel0FileNumCompactionTrigger())
                .setMergeOperatorName(MERGE_OPERATOR_NAME)
                .setTableFormatConfig(
                        new BlockBasedTableConfig()
                                .setFilter(new BloomFilter(10, false))
                                .setIndexType(conf.getIndexType())
                                .setCacheIndexAndFilterBlocks(conf.getCacheIndexAndFilterBlocks())
                                .setBlockCacheSize(conf.getBlockCacheSize())
                                .setBlockSize(conf.getBlockSize())
                );

        this.writeOptions = new WriteOptions().setDisableWAL(true);
        List<ColumnFamilyDescriptor> columnFamilyDescriptors = Collections.singletonList(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, columnOptions));
        List<ColumnFamilyHandle> stateColumnFamilyHandles = new ArrayList<>(1);
        List<Integer> ttlValues = Collections.singletonList(conf.getTtl());

        this.db = TtlDB.open(dbOptions, instanceRocksDBPath.getPath(), columnFamilyDescriptors, stateColumnFamilyHandles, ttlValues, false);
        // create handle for default CF
        Preconditions.checkState(columnFamilyDescriptors.size() == stateColumnFamilyHandles.size(),
                "Not all requested column family handles have been created");
        this.defaultColumnFamily = stateColumnFamilyHandles.get(0);
        this.lastStaticsTimeMills = System.currentTimeMillis();
        LOG.info("Successfully create rocksDB-v4.2.0 instance.");
    }

    @Override
    public void close() {
        if (db != null) {

            defaultColumnFamily.dispose();
            db.dispose();
            writeOptions.dispose();
            columnOptions.dispose();
            dbOptions.dispose();

            // invalidate the reference
            db = null;
        }
    }

    byte[] get(byte[] keyBytes) {
        try {
            byte[] value = db.get(defaultColumnFamily, keyBytes);
            return value;
        } catch (RocksDBException e) {
            throw new StateAccessException(e);
        }
    }

    Map<byte[], byte[]> multiGet(List<byte[]> listKeyBytes) {
        try {
            Map<byte[], byte[]> value = db.multiGet(listKeyBytes);
            return value;
        } catch (RocksDBException e) {
            throw new StateAccessException(e);
        }
    }

    void put(byte[] keyBytes, byte[] valueBytes) {
        try {
            db.put(writeOptions, keyBytes, valueBytes);
        } catch (RocksDBException e) {
            throw new StateAccessException(e);
        }
    }


    void remove(byte[] keyBytes) {
        try {
            db.remove(writeOptions, keyBytes);
        } catch (RocksDBException e) {
            throw new StateAccessException(e);
        }
    }

    void merge(byte[] keyBytes, byte[] partialValueBytes) {
        try {
            db.merge(writeOptions, keyBytes, partialValueBytes);
        } catch (RocksDBException e) {
            throw new StateAccessException(e);
        }
    }

    RocksIterator iterator() {
        return db.newIterator();
    }

    void snapshot(Path localCheckpointPath) throws RocksDBException, IOException {
        long numBytesIndexAndFilter = db.getLongProperty("rocksdb.estimate-table-readers-mem");

        FileSystem localFileSystem = localCheckpointPath.getFileSystem();
        if (localFileSystem.exists(localCheckpointPath)) {
            LOG.warn("Found an existing local checkpoint directory. Attempt to delete it.");

            if (!localFileSystem.delete(localCheckpointPath, true)) {
                throw new IOException("Error while deleting the local checkpoint directory.");
            }
        }

        Checkpoint checkpoint = Checkpoint.create(db);
        checkpoint.createCheckpoint(localCheckpointPath.getPath());
    }

    void getDBStatistics() {
        LOG.info("Start to get DB statics.");
        long currentTimeMillis = System.currentTimeMillis();
        double passedTime = (currentTimeMillis - lastStaticsTimeMills) / 1000.0;
        this.lastStaticsTimeMills = currentTimeMillis;

        long curBlockCacheHit = dbOptions.statisticsPtr().getTickerCount(TickerType.BLOCK_CACHE_HIT);
        long curBlockCacheMiss = dbOptions.statisticsPtr().getTickerCount(TickerType.BLOCK_CACHE_MISS);

        long curBlockIndexHit = dbOptions.statisticsPtr().getTickerCount(TickerType.BLOCK_CACHE_INDEX_HIT);
        long curBlockIndexMiss = dbOptions.statisticsPtr().getTickerCount(TickerType.BLOCK_CACHE_INDEX_MISS);

        long curBlockFilterHit = dbOptions.statisticsPtr().getTickerCount(TickerType.BLOCK_CACHE_FILTER_HIT);
        long curBlockFilterMiss = dbOptions.statisticsPtr().getTickerCount(TickerType.BLOCK_CACHE_FILTER_MISS);

        long curBlockDataHit = dbOptions.statisticsPtr().getTickerCount(TickerType.BLOCK_CACHE_DATA_HIT);
        long curBlockDataMiss = dbOptions.statisticsPtr().getTickerCount(TickerType.BLOCK_CACHE_DATA_MISS);

        long curMemTableHit = dbOptions.statisticsPtr().getTickerCount(TickerType.MEMTABLE_HIT);
        long curMemTableMiss = dbOptions.statisticsPtr().getTickerCount(TickerType.MEMTABLE_MISS);

        long curNumKeysWritten = dbOptions.statisticsPtr().getTickerCount(TickerType.NUMBER_KEYS_WRITTEN);
        long curNumKeysRead = dbOptions.statisticsPtr().getTickerCount(TickerType.NUMBER_KEYS_READ);
        long curNumKeysUpdated = dbOptions.statisticsPtr().getTickerCount(TickerType.NUMBER_KEYS_UPDATED);

        LOG.info("STATICS in the last {} seconds: \n" +
                        "  blockCache Hit: {} \n" +
                        "  blockCache Miss: {} \n" +
                        "  blockCacheIndex Hit: {} \n" +
                        "  blockCacheIndex Miss: {} \n" +
                        "  blockCacheFilter Hit: {} \n" +
                        "  blockCacheFilter Miss: {} \n" +
                        "  blockCacheData Hit: {} \n" +
                        "  blockCacheData Miss: {} \n" +
                        "  memTable Hit: {} \n" +
                        "  memTable Miss: {} \n" +
                        "  numKeys read: {} \n" +
                        "  numKeys written: {} \n" +
                        "  numKeys updated: {} \n",
                passedTime,
                curBlockCacheHit - blockCacheHit,
                curBlockCacheMiss - blockCacheMiss,
                curBlockIndexHit - blockCacheIndexHit,
                curBlockIndexMiss - blockCacheIndexMiss,
                curBlockFilterHit - blockCacheFilterHit,
                curBlockFilterMiss - blockCacheFilterMiss,
                curBlockDataHit - blockCacheDataHit,
                curBlockDataMiss - blockCacheDataMiss,
                curMemTableHit - memTableHit,
                curMemTableMiss - memTableMiss,
                curNumKeysRead - numKeysRead,
                curNumKeysWritten - numKeysWritten,
                curNumKeysUpdated - numKeysUpdated);
        blockCacheHit = curBlockCacheHit;
        blockCacheMiss = curBlockCacheMiss;
        blockCacheIndexHit = curBlockIndexHit;
        blockCacheIndexMiss = curBlockIndexMiss;
        blockCacheFilterHit = curBlockFilterHit;
        blockCacheFilterMiss = curBlockFilterMiss;
        blockCacheDataHit = curBlockDataHit;
        blockCacheDataMiss = curBlockDataMiss;
        memTableHit = curMemTableHit;
        memTableMiss = curMemTableMiss;
        numKeysRead = curNumKeysRead;
        numKeysWritten = curNumKeysWritten;
        numKeysUpdated = curNumKeysUpdated;

    }
}

