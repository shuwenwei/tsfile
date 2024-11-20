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

package org.apache.tsfile.write.v4;

import org.apache.tsfile.common.TsFileApi;
import org.apache.tsfile.exception.write.ConflictDataTypeException;
import org.apache.tsfile.exception.write.NoMeasurementException;
import org.apache.tsfile.exception.write.NoTableException;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.WriteUtils;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DeviceTableModelWriter extends CommonModelWriter {

  private boolean isTableWriteAligned = true;

  public DeviceTableModelWriter(File file) throws IOException {
    super(file);
  }

  /**
   * Write the tablet in to the TsFile with the table-view. The method will try to split the tablet
   * by device. If you know the device association within the tablet, please use writeTable(Tablet
   * tablet, List<Pair<IDeviceID, Integer>> deviceIdEndIndexPairs). One typical case where the other
   * method should be used is that all rows in the tablet belong to the same device.
   *
   * @param table data to write
   * @return true if a flush is triggered after write, false otherwise
   * @throws IOException if the file cannot be written
   * @throws WriteProcessException if the schema is not registered first
   */
  @TsFileApi
  public boolean writeTable(Tablet table) throws IOException, WriteProcessException {
    return writeTable(table, null);
  }

  /**
   * Write the tablet in to the TsFile with the table-view.
   *
   * @param tablet data to write
   * @param deviceIdEndIndexPairs each deviceId and its end row number in row order. For example, if
   *     the first three rows belong to device ("table1", "d1"), the next five rows belong to device
   *     ("table1", "d2"), and the last two rows belong to device ("table1", "d3"), then the list
   *     will be [(("table1", "d1"), 3), (("table1", "d2"), 8), (("table1", "d3"), 10)]. If the list
   *     is not provided, the method will try to split the tablet.
   * @return true if a flush is triggered after write, false otherwise
   * @throws IOException if the file cannot be written
   * @throws WriteProcessException if the schema is not registered first
   */
  public boolean writeTable(Tablet tablet, List<Pair<IDeviceID, Integer>> deviceIdEndIndexPairs)
      throws IOException, WriteProcessException {
    // make sure the ChunkGroupWriter for this Tablet exist and there is no type conflict
    checkIsTableExistAndSetColumnCategoryList(tablet);
    // spilt the tablet by deviceId
    if (deviceIdEndIndexPairs == null) {
      deviceIdEndIndexPairs = WriteUtils.splitTabletByDevice(tablet);
    }

    int startIndex = 0;
    for (Pair<IDeviceID, Integer> pair : deviceIdEndIndexPairs) {
      // get corresponding ChunkGroupWriter and write this Tablet
      recordCount +=
          tryToInitialGroupWriter(pair.left, isTableWriteAligned)
              .write(tablet, startIndex, pair.right);
      startIndex = pair.right;
    }
    return checkMemorySizeAndMayFlushChunks();
  }

  private void checkIsTableExistAndSetColumnCategoryList(Tablet tablet)
      throws WriteProcessException {
    String tableName = tablet.getTableName();
    final TableSchema tableSchema = getSchema().getTableSchemaMap().get(tableName);
    if (tableSchema == null) {
      throw new NoTableException(tableName);
    }

    List<Tablet.ColumnCategory> columnCategoryListForTablet =
        new ArrayList<>(tablet.getSchemas().size());
    for (IMeasurementSchema writingColumnSchema : tablet.getSchemas()) {
      final int columnIndex = tableSchema.findColumnIndex(writingColumnSchema.getMeasurementName());
      if (columnIndex < 0) {
        throw new NoMeasurementException(writingColumnSchema.getMeasurementName());
      }
      final IMeasurementSchema registeredColumnSchema =
          tableSchema.getColumnSchemas().get(columnIndex);
      if (!writingColumnSchema.getType().equals(registeredColumnSchema.getType())) {
        throw new ConflictDataTypeException(
            writingColumnSchema.getType(), registeredColumnSchema.getType());
      }
      columnCategoryListForTablet.add(tableSchema.getColumnTypes().get(columnIndex));
    }
    tablet.setColumnCategories(columnCategoryListForTablet);
  }

  public boolean isTableWriteAligned() {
    return isTableWriteAligned;
  }

  public void setTableWriteAligned(boolean tableWriteAligned) {
    isTableWriteAligned = tableWriteAligned;
  }

  public void registerTableSchema(TableSchema tableSchema) {
    getSchema().registerTableSchema(tableSchema);
  }

  public void setGenerateTableSchema(boolean generateTableSchema) {
    this.getIOWriter().setGenerateTableSchema(generateTableSchema);
  }
}
