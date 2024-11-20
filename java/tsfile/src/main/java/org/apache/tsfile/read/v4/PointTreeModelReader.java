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
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.common.TimeSeries;
import org.apache.tsfile.read.expression.QueryExpression;
import org.apache.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.tsfile.read.filter.operator.TimeFilterOperators;
import org.apache.tsfile.read.query.dataset.ResultSet;
import org.apache.tsfile.read.query.dataset.TreeResultSet;
import org.apache.tsfile.read.query.executor.TsFileExecutor;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

class PointTreeModelReader extends AbstractModelReader {

  protected TsFileExecutor queryExecutor;

  public PointTreeModelReader(File file) throws IOException {
    super(file);
    this.queryExecutor = new TsFileExecutor(metadataQuerier, chunkLoader);
  }

  @TsFileApi
  public List<IMeasurementSchema> getTimeseriesSchema(String deviceId) throws IOException {
    IDeviceID iDeviceID = IDeviceID.Factory.DEFAULT_FACTORY.create(deviceId);
    List<TimeseriesMetadata> deviceTimeseriesMetadata =
        fileReader.getDeviceTimeseriesMetadataWithoutChunkMetadata(iDeviceID);
    List<IMeasurementSchema> measurementSchemaList = new ArrayList<>();
    for (TimeseriesMetadata timeseriesMetadata : deviceTimeseriesMetadata) {
      measurementSchemaList.add(
          new MeasurementSchema(
              timeseriesMetadata.getMeasurementId(), timeseriesMetadata.getTsDataType()));
    }
    return measurementSchemaList;
  }

  @TsFileApi
  public ResultSet query(List<TimeSeries> pathList, long startTime, long endTime)
      throws IOException {
    QueryExpression queryExpression = QueryExpression.create();
    for (TimeSeries path : pathList) {
      queryExpression.addSelectedPath(
          new Path(path.getDeviceId(), path.getMeasurementName(), false));
    }
    queryExpression.setExpression(
        new GlobalTimeExpression(new TimeFilterOperators.TimeBetweenAnd(startTime, endTime)));
    return new TreeResultSet(queryExecutor.execute(queryExpression));
  }
}
