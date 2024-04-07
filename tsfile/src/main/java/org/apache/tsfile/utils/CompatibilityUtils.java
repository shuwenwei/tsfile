/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.tsfile.utils;

import org.apache.tsfile.file.metadata.MetadataIndexNode;
import org.apache.tsfile.file.metadata.TsFileMetadata;

import java.nio.ByteBuffer;
import java.util.Collections;

public class CompatibilityUtils {
  public static TsFileMetadata deserializeTsFileMetadataFromV3(ByteBuffer buffer) {
    TsFileMetadata fileMetaData = new TsFileMetadata();

    // metadataIndex
    MetadataIndexNode metadataIndexNode = MetadataIndexNode.deserializeFrom(buffer, true);
    fileMetaData.setTableMetadataIndexNodeMap(Collections.singletonMap("", metadataIndexNode));

    // metaOffset
    long metaOffset = ReadWriteIOUtils.readLong(buffer);
    fileMetaData.setMetaOffset(metaOffset);

    // read bloom filter
    if (buffer.hasRemaining()) {
      byte[] bytes = ReadWriteIOUtils.readByteBufferWithSelfDescriptionLength(buffer);
      int filterSize = ReadWriteForEncodingUtils.readUnsignedVarInt(buffer);
      int hashFunctionSize = ReadWriteForEncodingUtils.readUnsignedVarInt(buffer);
      fileMetaData.setBloomFilter(
          BloomFilter.buildBloomFilter(bytes, filterSize, hashFunctionSize));
    }

    return fileMetaData;
  }
}
