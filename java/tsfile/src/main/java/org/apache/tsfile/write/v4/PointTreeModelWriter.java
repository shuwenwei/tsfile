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
import org.apache.tsfile.exception.write.NoDeviceException;
import org.apache.tsfile.exception.write.NoMeasurementException;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.utils.MeasurementGroup;
import org.apache.tsfile.write.chunk.IChunkGroupWriter;
import org.apache.tsfile.write.record.TSRecord;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.record.datapoint.DataPoint;
import org.apache.tsfile.write.schema.IMeasurementSchema;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PointTreeModelWriter extends CommonModelWriter {

  @TsFileApi
  public PointTreeModelWriter(File file) throws IOException {
    super(file);
  }

  @TsFileApi
  public void registerTimeseries(String deviceId, IMeasurementSchema measurementSchema)
      throws WriteProcessException {
    registerTimeseries(IDeviceID.Factory.DEFAULT_FACTORY.create(deviceId), measurementSchema);
  }

  /** Register nonAligned timeseries by single. */
  @TsFileApi
  public void registerTimeseries(IDeviceID deviceID, IMeasurementSchema measurementSchema)
      throws WriteProcessException {
    MeasurementGroup measurementGroup;
    if (getSchema().containsDevice(deviceID)) {
      measurementGroup = getSchema().getSeriesSchema(deviceID);
      if (measurementGroup.isAligned()) {
        throw new WriteProcessException(
            "given device " + deviceID + " has been registered for aligned timeseries.");
      } else if (measurementGroup
          .getMeasurementSchemaMap()
          .containsKey(measurementSchema.getMeasurementName())) {
        throw new WriteProcessException(
            "given nonAligned timeseries "
                + (deviceID + "." + measurementSchema.getMeasurementName())
                + " has been registered.");
      }
    } else {
      measurementGroup = new MeasurementGroup(false);
    }
    measurementGroup
        .getMeasurementSchemaMap()
        .put(measurementSchema.getMeasurementName(), measurementSchema);
    getSchema().registerMeasurementGroup(deviceID, measurementGroup);
  }

  @TsFileApi
  public void registerAlignedTimeseries(
      String deviceId, List<IMeasurementSchema> measurementSchemas) throws WriteProcessException {
    registerAlignedTimeseries(
        IDeviceID.Factory.DEFAULT_FACTORY.create(deviceId), measurementSchemas);
  }

  /**
   * Register aligned timeseries. Once the device is registered for aligned timeseries, it cannot be
   * expanded.
   */
  @TsFileApi
  public void registerAlignedTimeseries(
      IDeviceID deviceID, List<IMeasurementSchema> measurementSchemas)
      throws WriteProcessException {
    if (getSchema().containsDevice(deviceID)) {
      if (getSchema().getSeriesSchema(deviceID).isAligned()) {
        throw new WriteProcessException(
            "given device "
                + deviceID
                + " has been registered for aligned timeseries and should not be expanded.");
      } else {
        throw new WriteProcessException(
            "given device " + deviceID + " has been registered for nonAligned timeseries.");
      }
    }
    MeasurementGroup measurementGroup = new MeasurementGroup(true);
    measurementSchemas.forEach(
        measurementSchema -> {
          measurementGroup
              .getMeasurementSchemaMap()
              .put(measurementSchema.getMeasurementName(), measurementSchema);
        });
    getSchema().registerMeasurementGroup(deviceID, measurementGroup);
  }

  private boolean checkIsTimeseriesExist(TSRecord record, boolean isAligned)
      throws WriteProcessException, IOException {
    // initial ChunkGroupWriter of this device in the TSRecord
    final IDeviceID deviceID = record.deviceId;
    IChunkGroupWriter groupWriter = tryToInitialGroupWriter(deviceID, isAligned);

    // initial all SeriesWriters of measurements in this TSRecord
    List<IMeasurementSchema> measurementSchemas;
    if (getSchema().containsDevice(deviceID)) {
      measurementSchemas =
          checkIsAllMeasurementsInGroup(
              record.dataPointList, getSchema().getSeriesSchema(deviceID), isAligned);
      if (isAligned) {
        for (IMeasurementSchema s : measurementSchemas) {
          if (flushedMeasurementsInDeviceMap.containsKey(deviceID)
              && !flushedMeasurementsInDeviceMap.get(deviceID).contains(s.getMeasurementName())) {
            throw new WriteProcessException(
                "TsFile has flushed chunk group and should not add new measurement "
                    + s.getMeasurementName()
                    + " in device "
                    + deviceID);
          }
        }
      }
      groupWriter.tryToAddSeriesWriter(measurementSchemas);
    } else if (getSchema().getSchemaTemplates() != null
        && getSchema().getSchemaTemplates().size() == 1) {
      // use the default template without needing to register device
      MeasurementGroup measurementGroup =
          getSchema().getSchemaTemplates().entrySet().iterator().next().getValue();
      measurementSchemas =
          checkIsAllMeasurementsInGroup(record.dataPointList, measurementGroup, isAligned);
      groupWriter.tryToAddSeriesWriter(measurementSchemas);
    } else {
      throw new NoDeviceException(deviceID.toString());
    }
    return true;
  }

  private void checkIsTimeseriesExist(Tablet tablet, boolean isAligned)
      throws WriteProcessException, IOException {
    final IDeviceID deviceID = IDeviceID.Factory.DEFAULT_FACTORY.create(tablet.getDeviceId());
    IChunkGroupWriter groupWriter = tryToInitialGroupWriter(deviceID, isAligned);

    List<IMeasurementSchema> schemas = tablet.getSchemas();
    if (getSchema().containsDevice(deviceID)) {
      checkIsAllMeasurementsInGroup(getSchema().getSeriesSchema(deviceID), schemas, isAligned);
      if (isAligned) {
        for (IMeasurementSchema s : schemas) {
          if (flushedMeasurementsInDeviceMap.containsKey(deviceID)
              && !flushedMeasurementsInDeviceMap.get(deviceID).contains(s.getMeasurementName())) {
            throw new WriteProcessException(
                "TsFile has flushed chunk group and should not add new measurement "
                    + s.getMeasurementName()
                    + " in device "
                    + deviceID);
          }
        }
      }
      groupWriter.tryToAddSeriesWriter(schemas);
    } else if (getSchema().getSchemaTemplates() != null
        && getSchema().getSchemaTemplates().size() == 1) {
      MeasurementGroup measurementGroup =
          getSchema().getSchemaTemplates().entrySet().iterator().next().getValue();
      checkIsAllMeasurementsInGroup(measurementGroup, schemas, isAligned);
      groupWriter.tryToAddSeriesWriter(schemas);
    } else {
      throw new NoDeviceException(deviceID.toString());
    }
  }

  /**
   * write a record in type of T.
   *
   * @param record - record responding a data line
   * @return true -size of tsfile or metadata reaches the threshold. false - otherwise
   * @throws IOException exception in IO
   * @throws WriteProcessException exception in write process
   */
  @TsFileApi
  public boolean writeRecord(TSRecord record) throws IOException, WriteProcessException {
    MeasurementGroup measurementGroup = getSchema().getSeriesSchema(record.deviceId);
    if (measurementGroup == null) {
      throw new NoDeviceException(record.deviceId.toString());
    }
    checkIsTimeseriesExist(record, measurementGroup.isAligned());
    recordCount += groupWriters.get(record.deviceId).write(record.time, record.dataPointList);
    return checkMemorySizeAndMayFlushChunks();
  }

  /**
   * write a tablet
   *
   * @param tablet - multiple time series of one device that share a time column
   * @throws IOException exception in IO
   * @throws WriteProcessException exception in write process
   */
  @TsFileApi
  public boolean writeTree(Tablet tablet) throws IOException, WriteProcessException {
    IDeviceID deviceID = IDeviceID.Factory.DEFAULT_FACTORY.create(tablet.getDeviceId());
    MeasurementGroup measurementGroup = getSchema().getSeriesSchema(deviceID);
    if (measurementGroup == null) {
      throw new NoDeviceException(deviceID.toString());
    }
    // make sure the ChunkGroupWriter for this Tablet exist
    checkIsTimeseriesExist(tablet, measurementGroup.isAligned());
    // get corresponding ChunkGroupWriter and write this Tablet
    recordCount += groupWriters.get(deviceID).write(tablet);
    return checkMemorySizeAndMayFlushChunks();
  }

  /**
   * If it's aligned, then all measurementSchemas should be contained in the measurementGroup, or it
   * will throw exception. If it's nonAligned, then remove the measurementSchema that is not
   * contained in the measurementGroup.
   */
  private void checkIsAllMeasurementsInGroup(
      MeasurementGroup measurementGroup,
      List<IMeasurementSchema> measurementSchemas,
      boolean isAligned)
      throws NoMeasurementException {
    if (isAligned && !measurementGroup.isAligned()) {
      throw new NoMeasurementException("aligned");
    } else if (!isAligned && measurementGroup.isAligned()) {
      throw new NoMeasurementException("nonAligned");
    }
    for (IMeasurementSchema measurementSchema : measurementSchemas) {
      if (!measurementGroup
          .getMeasurementSchemaMap()
          .containsKey(measurementSchema.getMeasurementName())) {
        if (isAligned) {
          throw new NoMeasurementException(measurementSchema.getMeasurementName());
        } else {
          measurementSchemas.remove(measurementSchema);
        }
      }
    }
  }

  /** Check whether all measurements of dataPoints list are in the measurementGroup. */
  private List<IMeasurementSchema> checkIsAllMeasurementsInGroup(
      List<DataPoint> dataPoints, MeasurementGroup measurementGroup, boolean isAligned)
      throws NoMeasurementException {
    if (isAligned && !measurementGroup.isAligned()) {
      throw new NoMeasurementException("aligned");
    } else if (!isAligned && measurementGroup.isAligned()) {
      throw new NoMeasurementException("nonAligned");
    }
    List<IMeasurementSchema> schemas = new ArrayList<>();
    for (DataPoint dataPoint : dataPoints) {
      if (!measurementGroup.getMeasurementSchemaMap().containsKey(dataPoint.getMeasurementId())) {
        if (isAligned) {
          throw new NoMeasurementException(dataPoint.getMeasurementId());
        } else {
          LOG.warn(
              "Ignore nonAligned measurement "
                  + dataPoint.getMeasurementId()
                  + " , because it is not registered or in the default template");
        }
      } else {
        schemas.add(measurementGroup.getMeasurementSchemaMap().get(dataPoint.getMeasurementId()));
      }
    }
    return schemas;
  }

  public boolean isGenerateTableSchemaForTree() {
    return getIOWriter().isGenerateTableSchema();
  }
}
