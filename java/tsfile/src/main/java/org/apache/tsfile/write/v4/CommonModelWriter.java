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
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.encrypt.EncryptParameter;
import org.apache.tsfile.encrypt.IEncryptor;
import org.apache.tsfile.exception.encrypt.EncryptException;
import org.apache.tsfile.exception.write.NoMeasurementException;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.utils.MeasurementGroup;
import org.apache.tsfile.write.chunk.AlignedChunkGroupWriterImpl;
import org.apache.tsfile.write.chunk.IChunkGroupWriter;
import org.apache.tsfile.write.chunk.NonAlignedChunkGroupWriterImpl;
import org.apache.tsfile.write.record.datapoint.DataPoint;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.Schema;
import org.apache.tsfile.write.writer.RestorableTsFileIOWriter;
import org.apache.tsfile.write.writer.TsFileIOWriter;
import org.apache.tsfile.write.writer.TsFileOutput;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

abstract class CommonModelWriter implements AutoCloseable {

  protected static final TSFileConfig config = TSFileDescriptor.getInstance().getConfig();
  private static final Logger LOG = LoggerFactory.getLogger(CommonModelWriter.class);

  /** IO writer of this TsFile. */
  protected final TsFileIOWriter fileWriter;

  protected EncryptParameter encryptParam;

  protected final int pageSize;
  protected long recordCount = 0;

  // deviceId -> measurementIdList
  protected Map<IDeviceID, List<String>> flushedMeasurementsInDeviceMap = new HashMap<>();

  // DeviceId -> LastTime
  protected Map<IDeviceID, Long> alignedDeviceLastTimeMap = new HashMap<>();

  // TimeseriesId -> LastTime
  protected Map<IDeviceID, Map<String, Long>> nonAlignedTimeseriesLastTimeMap = new HashMap<>();

  /**
   * if true, this tsfile allow unsequential data when writing; Otherwise, it limits the user to
   * write only sequential data
   */
  protected boolean isUnseq = false;

  protected Map<IDeviceID, IChunkGroupWriter> groupWriters = new TreeMap<>();

  /** min value of threshold of data points num check. */
  protected long recordCountForNextMemCheck = 100;

  protected long chunkGroupSizeThreshold;

  /**
   * init this Writer.
   *
   * @param file the File to be written by this TsFileWriter
   */
  @TsFileApi
  public CommonModelWriter(File file) throws IOException {
    this(new TsFileIOWriter(file), new Schema(), TSFileDescriptor.getInstance().getConfig());
  }

  /**
   * init this Writer.
   *
   * @param fileWriter the io writer of this TsFile
   */
  public CommonModelWriter(TsFileIOWriter fileWriter) throws IOException {
    this(fileWriter, new Schema(), TSFileDescriptor.getInstance().getConfig());
  }

  /**
   * init this Writer.
   *
   * @param file the File to be written by this TsFileWriter
   * @param schema the schema of this TsFile
   */
  public CommonModelWriter(File file, Schema schema) throws IOException {
    this(new TsFileIOWriter(file), schema, TSFileDescriptor.getInstance().getConfig());
  }

  /**
   * init this Writer.
   *
   * @param output the TsFileOutput of the file to be written by this TsFileWriter
   * @param schema the schema of this TsFile
   */
  public CommonModelWriter(TsFileOutput output, Schema schema) throws IOException {
    this(new TsFileIOWriter(output), schema, TSFileDescriptor.getInstance().getConfig());
  }

  /**
   * init this Writer.
   *
   * @param file the File to be written by this PointTreeModelWriter
   * @param schema the schema of this TsFile
   * @param conf the configuration of this TsFile
   */
  public CommonModelWriter(File file, Schema schema, TSFileConfig conf) throws IOException {
    this(new TsFileIOWriter(file), schema, conf);
  }

  /**
   * init this Writer.
   *
   * @param fileWriter the io writer of this TsFile
   * @param schema the schema of this TsFile
   * @param conf the configuration of this TsFile
   */
  protected CommonModelWriter(TsFileIOWriter fileWriter, Schema schema, TSFileConfig conf)
      throws IOException {
    if (!fileWriter.canWrite()) {
      throw new IOException(
          "the given file Writer does not support writing any more. Maybe it is an complete TsFile");
    }
    this.fileWriter = fileWriter;

    if (fileWriter instanceof RestorableTsFileIOWriter) {
      schema = ((RestorableTsFileIOWriter) fileWriter).getKnownSchema();
    }
    fileWriter.setSchema(schema);

    this.pageSize = conf.getPageSizeInByte();
    this.chunkGroupSizeThreshold = conf.getGroupSizeInByte();
    config.setTSFileStorageFs(conf.getTSFileStorageFs());
    if (this.pageSize >= chunkGroupSizeThreshold) {
      LOG.warn(
          "TsFile's page size {} is greater than chunk group size {}, please enlarge the chunk group"
              + " size or decrease page size. ",
          pageSize,
          chunkGroupSizeThreshold);
    }

    String encryptLevel;
    byte[] encryptKey;
    byte[] dataEncryptKey;
    String encryptType;
    if (config.getEncryptFlag()) {
      encryptLevel = "2";
      encryptType = config.getEncryptType();
      try {
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        md.update("IoTDB is the best".getBytes());
        md.update(config.getEncryptKey().getBytes());
        dataEncryptKey = Arrays.copyOfRange(md.digest(), 0, 16);
        encryptKey =
            IEncryptor.getEncryptor(config.getEncryptType(), config.getEncryptKey().getBytes())
                .encrypt(dataEncryptKey);
      } catch (Exception e) {
        throw new EncryptException(
            "SHA-256 function not found while using SHA-256 to generate data key");
      }
    } else {
      encryptLevel = "0";
      encryptType = "org.apache.tsfile.encrypt.UNENCRYPTED";
      encryptKey = null;
      dataEncryptKey = null;
    }
    this.encryptParam = new EncryptParameter(encryptType, dataEncryptKey);
    if (encryptKey != null) {
      StringBuilder valueStr = new StringBuilder();

      for (byte b : encryptKey) {
        valueStr.append(b).append(",");
      }

      valueStr.deleteCharAt(valueStr.length() - 1);
      String str = valueStr.toString();

      fileWriter.setEncryptParam(encryptLevel, encryptType, str);
    } else {
      fileWriter.setEncryptParam(encryptLevel, encryptType, "");
    }
  }

  /**
   * If it's aligned, then all measurementSchemas should be contained in the measurementGroup, or it
   * will throw exception. If it's nonAligned, then remove the measurementSchema that is not
   * contained in the measurementGroup.
   */
  protected void checkIsAllMeasurementsInGroup(
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
  protected List<IMeasurementSchema> checkIsAllMeasurementsInGroup(
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

  protected IChunkGroupWriter tryToInitialGroupWriter(IDeviceID deviceId, boolean isAligned) {
    IChunkGroupWriter groupWriter = groupWriters.get(deviceId);
    if (groupWriter == null) {
      if (isAligned) {
        groupWriter = new AlignedChunkGroupWriterImpl(deviceId, encryptParam);
        if (!isUnseq) { // Sequence File
          ((AlignedChunkGroupWriterImpl) groupWriter)
              .setLastTime(alignedDeviceLastTimeMap.get(deviceId));
        }
      } else {
        groupWriter = new NonAlignedChunkGroupWriterImpl(deviceId, encryptParam);
        if (!isUnseq) { // Sequence File
          ((NonAlignedChunkGroupWriterImpl) groupWriter)
              .setLastTimeMap(
                  nonAlignedTimeseriesLastTimeMap.getOrDefault(deviceId, new HashMap<>()));
        }
      }
      groupWriters.put(deviceId, groupWriter);
    }
    return groupWriter;
  }

  /**
   * calculate total memory size occupied by allT ChunkGroupWriter instances currently.
   *
   * @return total memory size used
   */
  protected long calculateMemSizeForAllGroup() {
    long memTotalSize = 0;
    for (IChunkGroupWriter group : groupWriters.values()) {
      memTotalSize += group.updateMaxGroupMemSize();
    }
    return memTotalSize;
  }

  /**
   * check occupied memory size, if it exceeds the chunkGroupSize threshold, flush them to given
   * OutputStream.
   *
   * @return true - size of tsfile or metadata reaches the threshold. false - otherwise
   * @throws IOException exception in IO
   */
  protected boolean checkMemorySizeAndMayFlushChunks() throws IOException {
    if (recordCount >= recordCountForNextMemCheck) {
      long memSize = calculateMemSizeForAllGroup();
      assert memSize > 0;
      if (memSize > chunkGroupSizeThreshold) {
        LOG.debug("start to flush chunk groups, memory space occupy:{}", memSize);
        recordCountForNextMemCheck = recordCount * chunkGroupSizeThreshold / memSize;
        return flush();
      } else {
        recordCountForNextMemCheck = recordCount * chunkGroupSizeThreshold / memSize;
        return false;
      }
    }
    return false;
  }

  /**
   * flush the data in all series writers of all chunk group writers and their page writers to
   * outputStream.
   *
   * @return true - size of tsfile or metadata reaches the threshold. false - otherwise. But this
   *     function just return false, the Override of IoTDB may return true.
   * @throws IOException exception in IO
   */
  @TsFileApi
  public boolean flush() throws IOException {
    if (recordCount > 0) {
      for (Map.Entry<IDeviceID, IChunkGroupWriter> entry : groupWriters.entrySet()) {
        IDeviceID deviceId = entry.getKey();
        IChunkGroupWriter groupWriter = entry.getValue();
        fileWriter.startChunkGroup(deviceId);
        long pos = fileWriter.getPos();
        long dataSize = groupWriter.flushToFileWriter(fileWriter);
        if (fileWriter.getPos() - pos != dataSize) {
          throw new IOException(
              String.format(
                  "Flushed data size is inconsistent with computation! Estimated: %d, Actual: %d",
                  dataSize, fileWriter.getPos() - pos));
        }
        fileWriter.endChunkGroup();
        if (groupWriter instanceof AlignedChunkGroupWriterImpl) {
          // add flushed measurements
          List<String> measurementList =
              flushedMeasurementsInDeviceMap.computeIfAbsent(deviceId, p -> new ArrayList<>());
          ((AlignedChunkGroupWriterImpl) groupWriter)
              .getMeasurements()
              .forEach(
                  measurementId -> {
                    if (!measurementList.contains(measurementId)) {
                      measurementList.add(measurementId);
                    }
                  });
          // add lastTime
          if (!isUnseq) { // Sequence TsFile
            this.alignedDeviceLastTimeMap.put(
                deviceId, ((AlignedChunkGroupWriterImpl) groupWriter).getLastTime());
          }
        } else {
          // add lastTime
          if (!isUnseq) { // Sequence TsFile
            this.nonAlignedTimeseriesLastTimeMap.put(
                deviceId, ((NonAlignedChunkGroupWriterImpl) groupWriter).getLastTimeMap());
          }
        }
      }
      reset();
    }
    return false;
  }

  protected void reset() {
    groupWriters.clear();
    recordCount = 0;
  }

  /**
   * this function is only for Test.
   *
   * @return TsFileIOWriter
   */
  protected TsFileIOWriter getIOWriter() {
    return this.fileWriter;
  }

  protected Schema getSchema() {
    return fileWriter.getSchema();
  }

  /**
   * calling this method to write the last data remaining in memory and close the normal and error
   * OutputStream.
   *
   * @throws IOException exception in IO
   */
  @Override
  @TsFileApi
  public void close() throws IOException {
    LOG.info("start close file");
    flush();
    fileWriter.endFile();
  }
}
