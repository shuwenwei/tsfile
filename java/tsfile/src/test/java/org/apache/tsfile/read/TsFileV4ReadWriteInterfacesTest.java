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

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.read.v4.DeviceTableModelReader;
import org.apache.tsfile.read.v4.PointTreeModelReader;
import org.apache.tsfile.utils.TsFileGeneratorForTest;
import org.apache.tsfile.utils.TsFileGeneratorUtils;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.v4.DeviceTableModelWriter;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

public class TsFileV4ReadWriteInterfacesTest {
  @Test
  public void testGetDeviceMethods() throws Exception {
    String filePath = TsFileGeneratorForTest.getTestTsFilePath("root.testsg", 0, 0, 0);
    try {
      File file = TsFileGeneratorUtils.generateAlignedTsFile(filePath, 5, 1, 10, 1, 1, 10, 100);
      try (PointTreeModelReader tsFileReader = new PointTreeModelReader(file)) {
        Assert.assertEquals(
            Arrays.asList(
                IDeviceID.Factory.DEFAULT_FACTORY.create("root.testsg.d10000"),
                IDeviceID.Factory.DEFAULT_FACTORY.create("root.testsg.d10001"),
                IDeviceID.Factory.DEFAULT_FACTORY.create("root.testsg.d10002"),
                IDeviceID.Factory.DEFAULT_FACTORY.create("root.testsg.d10003"),
                IDeviceID.Factory.DEFAULT_FACTORY.create("root.testsg.d10004")),
            tsFileReader.getAllDevices());
        List<IMeasurementSchema> timeseriesSchema =
            tsFileReader.getTimeseriesSchema(
                IDeviceID.Factory.DEFAULT_FACTORY.create("root.testsg.d10000"));
        Assert.assertEquals(2, timeseriesSchema.size());
        Assert.assertEquals("", timeseriesSchema.get(0).getMeasurementName());
        Assert.assertEquals("s0", timeseriesSchema.get(1).getMeasurementName());
      }
    } finally {
      Files.deleteIfExists(Paths.get(filePath));
    }
  }

  @Test
  public void testGetTableDeviceMethods() throws Exception {
    String filePath = TsFileGeneratorForTest.getTestTsFilePath("root.testsg", 0, 0, 0);
    try {
      File file = TsFileGeneratorUtils.generateAlignedTsFile(filePath, 5, 1, 10, 1, 1, 10, 100);
      List<IDeviceID> deviceIDList = new ArrayList<>();
      TableSchema tableSchema =
          new TableSchema(
              "t1",
              Arrays.asList(
                  new MeasurementSchema("id1", TSDataType.STRING),
                  new MeasurementSchema("id2", TSDataType.STRING),
                  new MeasurementSchema("id3", TSDataType.STRING),
                  new MeasurementSchema("s1", TSDataType.INT32)),
              Arrays.asList(
                  Tablet.ColumnCategory.ID,
                  Tablet.ColumnCategory.ID,
                  Tablet.ColumnCategory.ID,
                  Tablet.ColumnCategory.MEASUREMENT));
      try (DeviceTableModelWriter writer = new DeviceTableModelWriter(file, tableSchema)) {
        Tablet tablet =
            new Tablet(
                tableSchema.getTableName(),
                IMeasurementSchema.getMeasurementNameList(tableSchema.getColumnSchemas()),
                IMeasurementSchema.getDataTypeList(tableSchema.getColumnSchemas()),
                tableSchema.getColumnTypes());

        String[][] ids =
            new String[][] {
              {null, null, null},
              {null, null, "id3-4"},
              {null, "id2-1", "id3-1"},
              {null, "id2-5", null},
              {"id1-2", null, "id3-2"},
              {"id1-3", "id2-3", null},
              {"id1-6", null, null},
            };
        for (int i = 0; i < ids.length; i++) {
          tablet.addTimestamp(i, i);
          tablet.addValue("id1", i, ids[i][0]);
          tablet.addValue("id2", i, ids[i][1]);
          tablet.addValue("id3", i, ids[i][2]);
          deviceIDList.add(
              new StringArrayDeviceID(tableSchema.getTableName(), ids[i][0], ids[i][1], ids[i][2]));
          tablet.addValue("s1", i, i);
        }
        tablet.setRowSize(ids.length);
        writer.writeTable(tablet);
      }
      try (DeviceTableModelReader tsFileReader = new DeviceTableModelReader(file)) {
        Assert.assertEquals(
            new HashSet<>(deviceIDList), new HashSet<>(tsFileReader.getAllTableDevices("t1")));
        Assert.assertEquals("t1", tsFileReader.getAllTables().get(0));
        Assert.assertEquals(
            tableSchema, tsFileReader.getTableSchema(Collections.singletonList("t1")).get(0));
      }
    } finally {
      Files.deleteIfExists(Paths.get(filePath));
    }
  }
}
