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

package org.apache.tsfile.read.query.dataset;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.utils.Binary;

import java.io.IOException;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ResultSet {
  private QueryDataSet queryDataSet;
  private ResultSetMetadata resultSetMetadata;
  private RowRecord currentRow;
  private Map<String, Integer> columnNameToColumnIndexMap;

  public ResultSet(QueryDataSet queryDataSet) {
    this.queryDataSet = queryDataSet;
    this.resultSetMetadata =
        new ResultSetMetadata(queryDataSet.getPaths(), queryDataSet.getDataTypes());
    this.columnNameToColumnIndexMap = new HashMap<>(queryDataSet.getColumnNum());
    for (int columnIndex = 1; columnIndex <= queryDataSet.getColumnNum(); columnIndex++) {
      this.columnNameToColumnIndexMap.put(
          resultSetMetadata.getColumnName(columnIndex), columnIndex);
    }
  }

  public ResultSetMetadata getMetadata() {
    return this.resultSetMetadata;
  }

  public boolean next() throws IOException {
    if (!queryDataSet.hasNext()) {
      return false;
    }
    currentRow = queryDataSet.next();
    return true;
  }

  public int getInt(String columnName) {
    Integer columnIndex = columnNameToColumnIndexMap.get(columnName);
    return getInt(columnIndex);
  }

  public int getInt(int columnIndex) {
    return currentRow.getFields().get(columnIndex - 1).getIntV();
  }

  public long getLong(String columnName) {
    Integer columnIndex = columnNameToColumnIndexMap.get(columnName);
    return getLong(columnIndex);
  }

  public long getLong(int columnIndex) {
    return currentRow.getFields().get(columnIndex - 1).getLongV();
  }

  public float getFloat(String columnName) {
    Integer columnIndex = columnNameToColumnIndexMap.get(columnName);
    return getFloat(columnIndex);
  }

  public float getFloat(int columnIndex) {
    return currentRow.getFields().get(columnIndex - 1).getFloatV();
  }

  public double getDouble(String columnName) {
    Integer columnIndex = columnNameToColumnIndexMap.get(columnName);
    return getDouble(columnIndex);
  }

  public double getDouble(int columnIndex) {
    return currentRow.getFields().get(columnIndex - 1).getDoubleV();
  }

  public boolean getBoolean(String columnName) {
    Integer columnIndex = columnNameToColumnIndexMap.get(columnName);
    return getBoolean(columnIndex);
  }

  public boolean getBoolean(int columnIndex) {
    return currentRow.getFields().get(columnIndex - 1).getBoolV();
  }

  public String getString(String columnName) {
    Integer columnIndex = columnNameToColumnIndexMap.get(columnName);
    return getString(columnIndex);
  }

  public String getString(int columnIndex) {
    return currentRow.getFields().get(columnIndex - 1).getStringValue();
  }

  public LocalDate getDate(String columnName) {
    Integer columnIndex = columnNameToColumnIndexMap.get(columnName);
    return getDate(columnIndex);
  }

  public LocalDate getDate(int columnIndex) {
    return currentRow.getFields().get(columnIndex - 1).getDateV();
  }

  public Binary getBinary(String columnName) {
    Integer columnIndex = columnNameToColumnIndexMap.get(columnName);
    return getBinary(columnIndex);
  }

  public Binary getBinary(int columnIndex) {
    return currentRow.getFields().get(columnIndex - 1).getBinaryV();
  }

  public boolean isNull(String columnName) {
    Integer columnIndex = columnNameToColumnIndexMap.get(columnName);
    return isNull(columnIndex);
  }

  public boolean isNull(int columnIndex) {
    return currentRow.getFields().get(columnIndex - 1) == null;
  }

  public void close() {}

  public static class ResultSetMetadata {

    private List<Path> paths;
    private List<TSDataType> dataTypeList;

    public ResultSetMetadata(List<Path> paths, List<TSDataType> dataTypeList) {
      this.paths = paths;
      this.dataTypeList = dataTypeList;
    }

    // columnIndex starting from 1
    public String getColumnName(int columnIndex) {
      return paths.get(columnIndex - 1).getFullPath();
    }

    // columnIndex starting from 1
    public TSDataType getColumnType(int columnIndex) {
      return dataTypeList.get(columnIndex - 1);
    }
  }
}
