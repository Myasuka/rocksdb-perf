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

package com.myasuka.rocksdb.common;

import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

import java.io.IOException;

public class Utils {

    public static byte[] serializeBytes(int key, byte[] stateNameBytes) throws IOException {
        ByteArrayOutputStreamWithPos outputStream = new ByteArrayOutputStreamWithPos();
        DataOutputViewStreamWrapper outputView = new DataOutputViewStreamWrapper(outputStream);

        int group = HashPartitioner.INSTANCE.partition(key, 320);
        writeInt(outputStream, group);

        outputStream.write(stateNameBytes);

        outputView.writeBoolean(false);
        IntSerializer.INSTANCE.serialize(key, outputView);

        return outputStream.toByteArray();
    }

    static void writeInt(ByteArrayOutputStreamWithPos outputStream, int v) {
        outputStream.write((v >>> 16) & 0xFF);
        outputStream.write((v >>> 8) & 0xFF);
        outputStream.write((v >>> 0) & 0xFF);
    }
}
