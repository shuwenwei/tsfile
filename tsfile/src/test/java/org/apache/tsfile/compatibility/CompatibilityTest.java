package org.apache.tsfile.compatibility;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import org.apache.tsfile.read.TsFileReader;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.read.expression.QueryExpression;
import org.apache.tsfile.read.query.dataset.QueryDataSet;
import org.junit.Test;

public class CompatibilityTest {

  private String fileName = "target" + File.separator + "test-classes" + File.separator +
      "v3TsFile";

  /**
   * The file is generated by the TsFileWriter in version 3.
   */
  @Test
  public void testReadV3() {
    readOneRow();
  }

  private void readOneRow() {
    readOneRow(5);
  }

  private void readOneRow(int s2Value) {
    try {
      TsFileReader tsFileReader = new TsFileReader(new TsFileSequenceReader(fileName));
      QueryDataSet dataSet =
          tsFileReader.query(
              QueryExpression.create()
                  .addSelectedPath(new Path("d1", "s1", true))
                  .addSelectedPath(new Path("d1", "s2", true))
                  .addSelectedPath(new Path("d1", "s3", true)));
      while (dataSet.hasNext()) {
        RowRecord result = dataSet.next();
        assertEquals(2, result.getFields().size());
        assertEquals(10000, result.getTimestamp());
        assertEquals(5.0f, result.getFields().get(0).getFloatV(), 0.00001);
        assertEquals(s2Value, result.getFields().get(1).getIntV());
      }
      tsFileReader.close();
    } catch (IOException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
