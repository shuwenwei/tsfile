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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.tsfile.read;

import org.apache.tsfile.common.TsFileApi;
import org.apache.tsfile.read.controller.CachedChunkLoaderImpl;
import org.apache.tsfile.read.controller.IChunkLoader;
import org.apache.tsfile.read.controller.IMetadataQuerier;
import org.apache.tsfile.read.controller.MetadataQuerierByFileImpl;
import org.apache.tsfile.read.expression.QueryExpression;
import org.apache.tsfile.read.query.dataset.QueryDataSet;
import org.apache.tsfile.read.query.executor.TsFileExecutor;

import java.io.File;
import java.io.IOException;

@Deprecated
public class TsFileReader implements AutoCloseable {

  private TsFileSequenceReader fileReader;
  private IMetadataQuerier metadataQuerier;
  private IChunkLoader chunkLoader;
  private TsFileExecutor tsfileExecutor;

  @TsFileApi
  public TsFileReader(File file) throws IOException {
    this(new TsFileSequenceReader(file.getPath()));
  }

  /** Constructor, create ReadOnlyTsFile with {@link TsFileSequenceReader}. */
  public TsFileReader(TsFileSequenceReader fileReader) throws IOException {
    this.fileReader = fileReader;
    this.metadataQuerier = new MetadataQuerierByFileImpl(fileReader);
    this.chunkLoader = new CachedChunkLoaderImpl(fileReader);
    this.tsfileExecutor = new TsFileExecutor(metadataQuerier, chunkLoader);
  }

  @Deprecated
  public QueryDataSet query(QueryExpression queryExpression) throws IOException {
    return tsfileExecutor.execute(queryExpression);
  }

  @Deprecated
  public QueryDataSet query(
      QueryExpression queryExpression, long partitionStartOffset, long partitionEndOffset)
      throws IOException {
    return tsfileExecutor.execute(queryExpression, partitionStartOffset, partitionEndOffset);
  }

  @Override
  @TsFileApi
  public void close() throws IOException {
    fileReader.close();
  }
}
