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

import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

import com.myasuka.rocksdb.common.Utils;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

import static com.myasuka.rocksdb.common.ConfConstants.DEFAULT_NUM_KEYS;
import static com.myasuka.rocksdb.common.ConfConstants.DEFAULT_QUERY_TIMES;
import static com.myasuka.rocksdb.common.ConfConstants.VALUE_BYTE_LENGTH;

public class Runner {
    private static final Logger LOG = LoggerFactory.getLogger(Runner.class);

    private static final String DATA_PATH = "dataPath";
    private static final String NUM_KEYS = "numKeys";
    private static final String QUERY_TIMES = "queryTimes";

    public static void main(String[] args) throws RocksDBException, IOException, InterruptedException {
        LOG.info("Usage:\n java -jar <rocksdb-perf>.jar [-{} <data path>] [-{} <num of keys>] [-{} <times of query>]\n " +
                        "\t default data path is current working dir \n" +
                        "\t default num of keys is {} \n" +
                        "\t default query times is {}.",
                DATA_PATH, NUM_KEYS, QUERY_TIMES, DEFAULT_NUM_KEYS, DEFAULT_QUERY_TIMES);


        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        String cwd = parameterTool.get(DATA_PATH, System. getProperty("user.dir"));
        int numKeys = parameterTool.getInt(NUM_KEYS, DEFAULT_NUM_KEYS);
        int queryTimes = parameterTool.getInt(QUERY_TIMES, DEFAULT_QUERY_TIMES);

        Path path = new Path(cwd);

        FileSystem fileSystem = path.getFileSystem();
        fileSystem.mkdirs(path);

        RocksDB.loadLibrary();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        StringSerializer.INSTANCE.serialize("_long_value_state_1", new DataOutputViewStreamWrapper(out));
        byte[] stateNameBytes = out.toByteArray();

        Path dbPath = new Path(path, "db-data");
        RocksDBConfiguration configuration = new RocksDBConfiguration();
        try (RocksDBInstance dbInstance = new RocksDBInstance(configuration, dbPath)) {
            LOG.info("Create rocksDB instance {} at path {}. ", configuration, dbPath);
            byte[] value = new byte[VALUE_BYTE_LENGTH];
            LOG.info("Start to put {} key-value pairs into rocksdb.", numKeys);
            long startPut = System.currentTimeMillis();
            for (int i = 0; i < numKeys; i++) {
                ThreadLocalRandom.current().nextBytes(value);
                dbInstance.put(Utils.serializeBytes(i, stateNameBytes), value);
            }
            long endPut = System.currentTimeMillis();
            LOG.info("Finished put {} key-value pairs from rocksdb consumed {} seconds.", numKeys, (endPut - startPut) / 1000.0);
            dbInstance.getDBStatistics();
            snapshot(dbInstance, path);

            LOG.info("Start to get {} key-value pairs randomly from rocksdb.", numKeys);
            for (int i = 0; i < queryTimes; i++) {
                long start = System.currentTimeMillis();
                for (int j = 0; j < numKeys; j++) {
                    dbInstance.get(Utils.serializeBytes(j, stateNameBytes));
                }
                long end = System.currentTimeMillis();
                LOG.info("Finished get {} key-value pairs from rocksdb consumed {} seconds.", numKeys, (end - start) / 1000.0);
                dbInstance.getDBStatistics();

                snapshot(dbInstance, path);
                Thread.sleep(10 * 1000L);
            }
            LOG.info("Finish to get {} key-value pairs into rocksdb.", numKeys);
        } finally {
            fileSystem.delete(dbPath, true);
        }
    }

    private static void snapshot(RocksDBInstance dbInstance, Path basePath) throws IOException, RocksDBException {

        Path chkPath = new Path(basePath, "chk-" + ThreadLocalRandom.current().nextLong(100000));
        LOG.info("Start to execute snapshot on rocksdb at {}.", chkPath);

        dbInstance.snapshot(chkPath);
        LOG.info("Finish to execute snapshot on rocksdb at {}.", chkPath);
    }
}
