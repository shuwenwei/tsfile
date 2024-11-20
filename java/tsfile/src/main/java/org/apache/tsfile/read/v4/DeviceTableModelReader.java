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

package org.apache.tsfile.read.v4;

import org.apache.tsfile.common.TsFileApi;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.read.ReadProcessException;
import org.apache.tsfile.exception.write.NoMeasurementException;
import org.apache.tsfile.exception.write.NoTableException;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.MetadataIndexNode;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.file.metadata.TsFileMetadata;
import org.apache.tsfile.read.expression.ExpressionTree;
import org.apache.tsfile.read.query.dataset.ResultSet;
import org.apache.tsfile.read.query.dataset.TableResultSet;
import org.apache.tsfile.read.query.executor.TableQueryExecutor;
import org.apache.tsfile.read.reader.block.TsBlockReader;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class DeviceTableModelReader extends AbstractModelReader {
  protected TableQueryExecutor queryExecutor;

  public DeviceTableModelReader(File file) throws IOException {
    super(file);
    this.queryExecutor =
        new TableQueryExecutor(
            metadataQuerier, chunkLoader, TableQueryExecutor.TableQueryOrdering.DEVICE);
  }

  @TsFileApi
  public List<String> getAllTables() throws IOException {
    Map<String, TableSchema> tableSchemaMap = fileReader.readFileMetadata().getTableSchemaMap();
    return new ArrayList<>(tableSchemaMap.keySet());
  }

  @TsFileApi
  public List<IDeviceID> getAllTableDevices(String tableName) throws IOException {
    MetadataIndexNode tableMetadataIndexNode =
        fileReader.readFileMetadata().getTableMetadataIndexNode(tableName);
    if (tableMetadataIndexNode == null) {
      return Collections.emptyList();
    }
    return fileReader.getAllDevices(tableMetadataIndexNode);
  }

  @TsFileApi
  public List<TableSchema> getTableSchema(List<String> tableNames) throws IOException {
    TsFileMetadata tsFileMetadata = fileReader.readFileMetadata();
    Map<String, TableSchema> tableSchemaMap = tsFileMetadata.getTableSchemaMap();
    List<TableSchema> result = new ArrayList<>(tableNames.size());
    for (String tableName : tableNames) {
      result.add(tableSchemaMap.get(tableName));
    }
    return result;
  }

  @TsFileApi
  public ResultSet queryTable(
      String tableName,
      List<String> columnNames,
      List<IDeviceID> deviceIds,
      long startTime,
      long endTime)
      throws ReadProcessException, IOException, NoTableException, NoMeasurementException {
    TsFileMetadata tsFileMetadata = fileReader.readFileMetadata();
    TableSchema tableSchema = tsFileMetadata.getTableSchemaMap().get(tableName);
    if (tableSchema == null) {
      throw new NoTableException(tableName);
    }
    List<TSDataType> dataTypeList = new ArrayList<>(columnNames.size());
    for (String columnName : columnNames) {
      Map<String, Integer> column2IndexMap = tableSchema.buildColumnPosIndex();
      Integer columnIndex = column2IndexMap.get(columnName);
      if (columnIndex == null) {
        throw new NoMeasurementException(columnName);
      }
      dataTypeList.add(tableSchema.getColumnSchemas().get(columnIndex).getType());
    }
    TsBlockReader tsBlockReader =
        queryExecutor.query(
            tableName,
            columnNames,
            new ExpressionTree.TimeBetweenAnd(startTime, endTime),
            new ExpressionTree.IdColumnMatch(deviceIds),
            null);
    return new TableResultSet(tsBlockReader, columnNames, dataTypeList);
  }
}
