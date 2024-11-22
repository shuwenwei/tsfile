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

package org.apache.tsfile.file.metadata;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.record.Tablet.ColumnCategory;

public class ColumnSchema {
  private String columnName;
  private TSDataType dataType;
  private ColumnCategory columnCategory;

  public ColumnSchema(String columnName, TSDataType dataType, ColumnCategory columnCategory) {
    this.columnName = columnName;
    this.dataType = dataType;
    this.columnCategory = columnCategory;
  }

  public String getColumnName() {
    return columnName;
  }

  public TSDataType getDataType() {
    return dataType;
  }

  public Tablet.ColumnCategory getColumnCategory() {
    return columnCategory;
  }
}
