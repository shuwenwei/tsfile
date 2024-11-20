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

import org.apache.tsfile.common.TsFileApi;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.Field;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.utils.Binary;

import java.io.IOException;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class ResultSet {

  protected ResultSetMetadata resultSetMetadata;
  protected Map<String, Integer> columnNameToColumnIndexMap;
  protected RowRecord currentRow;

  protected ResultSet(List<String> columnNameList, List<TSDataType> tsDataTypeList) {
    // Add Time at first column
    this.resultSetMetadata = new ResultSetMetadata(columnNameList, tsDataTypeList);
    this.columnNameToColumnIndexMap = new HashMap<>(resultSetMetadata.getColumnNum());
    for (int columnIndex = 1; columnIndex <= resultSetMetadata.getColumnNum(); columnIndex++) {
      this.columnNameToColumnIndexMap.put(
          resultSetMetadata.getColumnName(columnIndex), columnIndex);
    }
  }

  @TsFileApi
  public ResultSetMetadata getMetadata() {
    return this.resultSetMetadata;
  }

  @TsFileApi
  public abstract boolean next() throws IOException;

  @TsFileApi
  public int getInt(String columnName) {
    Integer columnIndex = columnNameToColumnIndexMap.get(columnName);
    return getInt(columnIndex);
  }

  @TsFileApi
  public int getInt(int columnIndex) {
    return getField(columnIndex).getIntV();
  }

  @TsFileApi
  public long getLong(String columnName) {
    Integer columnIndex = columnNameToColumnIndexMap.get(columnName);
    return getLong(columnIndex);
  }

  @TsFileApi
  public long getLong(int columnIndex) {
    return getField(columnIndex).getLongV();
  }

  @TsFileApi
  public float getFloat(String columnName) {
    Integer columnIndex = columnNameToColumnIndexMap.get(columnName);
    return getFloat(columnIndex);
  }

  @TsFileApi
  public float getFloat(int columnIndex) {
    return getField(columnIndex).getFloatV();
  }

  @TsFileApi
  public double getDouble(String columnName) {
    Integer columnIndex = columnNameToColumnIndexMap.get(columnName);
    return getDouble(columnIndex);
  }

  @TsFileApi
  public double getDouble(int columnIndex) {
    return getField(columnIndex).getDoubleV();
  }

  @TsFileApi
  public boolean getBoolean(String columnName) {
    Integer columnIndex = columnNameToColumnIndexMap.get(columnName);
    return getBoolean(columnIndex);
  }

  @TsFileApi
  public boolean getBoolean(int columnIndex) {
    return getField(columnIndex).getBoolV();
  }

  @TsFileApi
  public String getString(String columnName) {
    Integer columnIndex = columnNameToColumnIndexMap.get(columnName);
    return getString(columnIndex);
  }

  @TsFileApi
  public String getString(int columnIndex) {
    return getField(columnIndex).getStringValue();
  }

  @TsFileApi
  public LocalDate getDate(String columnName) {
    Integer columnIndex = columnNameToColumnIndexMap.get(columnName);
    return getDate(columnIndex);
  }

  @TsFileApi
  public LocalDate getDate(int columnIndex) {
    return getField(columnIndex).getDateV();
  }

  @TsFileApi
  public Binary getBinary(String columnName) {
    Integer columnIndex = columnNameToColumnIndexMap.get(columnName);
    return getBinary(columnIndex);
  }

  @TsFileApi
  public Binary getBinary(int columnIndex) {
    return getField(columnIndex).getBinaryV();
  }

  @TsFileApi
  public boolean isNull(String columnName) {
    Integer columnIndex = columnNameToColumnIndexMap.get(columnName);
    return isNull(columnIndex);
  }

  @TsFileApi
  public boolean isNull(int columnIndex) {
    return getField(columnIndex) == null;
  }

  protected Field getField(int columnIndex) {
    Field field;
    if (columnIndex == 1) {
      field = new Field(TSDataType.INT64);
      field.setLongV(currentRow.getTimestamp());
    } else {
      field = currentRow.getField(columnIndex - 2);
    }
    return field;
  }

  @TsFileApi
  public abstract void close();
}
