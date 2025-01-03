<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at
    
        http://www.apache.org/licenses/LICENSE-2.0
    
    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->
# Quick Start

## Java
### Installation Method
Add the following content to the `dependencies` in `pom.xml`

```xml
<dependency>
    <groupId>org.apache.tsfile</groupId>
    <artifactId>tsfile</artifactId>
    <version>2.0.0</version>
</dependency>
```

### Writing Process

#### Construct TableSchema

```java
TableSchema tableSchema =
        new TableSchema(
            tableName,
            Arrays.asList(
                new ColumnSchemaBuilder()
                    .name("tag1")
                    .dataType(TSDataType.STRING)
                    .category(Tablet.ColumnCategory.TAG)
                    .build(),
                new ColumnSchemaBuilder()
                    .name("tag2")
                    .dataType(TSDataType.STRING)
                    .category(Tablet.ColumnCategory.TAG)
                    .build(),
                new ColumnSchemaBuilder()
                    .name("field1")
                    .dataType(TSDataType.INT32)
                    .category(Tablet.ColumnCategory.FIELD)
                    .build(),
                new ColumnSchemaBuilder()
                    .name("field2")
                    .dataType(TSDataType.BOOLEAN)
                    .category(Tablet.ColumnCategory.FIELD)
                    .build()));
```

#### Constrcut ITsFileWriter
```java
File f = new File("test.tsfile");
ITsFileWriter writer =
        new TsFileWriterBuilder()
            .file(f)
            .tableSchema(tableSchema)
            .memoryThreshold(memoryThreshold)
            .build();
```

#### Build Tablet
```java
Tablet tablet =
    new Tablet(
        Arrays.asList("tag1", "tag2", "field1", "field2"),
        Arrays.asList(
            TSDataType.STRING, TSDataType.STRING, TSDataType.INT32, TSDataType.BOOLEAN));
      for (int row = 0; row < 5; row++) {
        long timestamp = row;
        tablet.addTimestamp(row, timestamp);
        tablet.addValue(row, "tag1", "tag1_value_1");
        tablet.addValue(row, "tag2", "tag2_value_1");
        tablet.addValue(row, "field1", row);
        tablet.addValue(row, "field2", true);
      }
```
#### Write Data

```shell
writer.write(tablet);
```

#### Close File

```java
writer.close();
```

#### Sample Code

<https://github.com/apache/tsfile/blob/develop/java/examples/src/main/java/org/apache/tsfile/v4/WriteTabletWithITsFileWriter.java>

### Query Process

#### Construct ITsFileReader

```java
File f = new File("test.tsfile");
ITsFileReader reader = new TsFileReaderBuilder().file(f).build();
```

#### Query Data
Query tag1, tag2, field1, field2 with a time range of [1,4]
```java
ResultSet resultSet = reader.query(tableName, Arrays.asList("tag1", "tag2", "field1", "field2"), 1, 4);
```

#### Check query result

```shell
while (resultSet.next()) {
  // columnIndex starts from 1
  // first column is Time
  // Time tag1 tag2 field1 field2
  Long timeField = resultSet.getLong("Time");
  String tag1 = resultSet.isNull("tag1") ? null : resultSet.getString("tag1");
  String tag2 = resultSet.isNull("tag2") ? null : resultSet.getString("tag2");
  Integer s1Field = resultSet.isNull("field1") ? null : resultSet.getInt(4);
  Boolean s2Field = resultSet.isNull("field2") ? null : resultSet.getBoolean(5);
}  
 ```

#### Close ResultSet
```java
resultSet.close();
```

#### Close File

```shell
tsFileReader.close();
```

#### Sample Code
<https://github.com/apache/tsfile/blob/develop/java/examples/src/main/java/org/apache/tsfile/v4/ITsFileReaderAndITsFileWriter.java>

