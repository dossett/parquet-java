/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.hadoop;

import static org.apache.parquet.schema.Types.primitive;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type.Repetition;
import org.junit.jupiter.api.Test;

class ParquetReadOptionsWithoutMapReduceTest {

  @Test
  void buildsReadOptionsWithoutMapReduce() {
    assertThatThrownBy(() -> Class.forName("org.apache.hadoop.mapreduce.lib.input.FileInputFormat"))
        .isInstanceOf(ClassNotFoundException.class);

    assertThatCode(() -> ParquetReadOptions.builder().build()).doesNotThrowAnyException();
  }
  
  @Test
  void canWriteAndReadFiles() throws IOException {
      Path path = Files.createTempDirectory("sample").resolve(java.util.UUID.randomUUID() + ".parquet");
      Configuration conf = new Configuration();

      PrimitiveType id = primitive(PrimitiveTypeName.INT32, Repetition.OPTIONAL).named("id");
      MessageType schema = new MessageType("schema", id);
      GroupWriteSupport.setSchema(schema, conf);

      SimpleGroupFactory f = new SimpleGroupFactory(schema);
      LocalOutputFile localOutputFile = new LocalOutputFile(path);
      try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(localOutputFile).withConf(conf).build()) {
          writer.write(f.newGroup().append("id", 1));
          writer.write(f.newGroup().append("id", 2));
      }

      LocalInputFile readFile = new LocalInputFile(path);
      try (ParquetReader<Group> reader = new GroupReaderBuilder().withFile(readFile).build()) {
          Group group1 = reader.read();
          assertEquals(1, group1.getInteger("id", 0));
          Group group2 = reader.read();
          assertEquals(2, group2.getInteger("id", 0));
      }
  }

  public static class GroupReaderBuilder extends ParquetReader.Builder<Group> {

      public GroupReaderBuilder() {
          super();
      }

      @Override
      public ReadSupport<Group> getReadSupport() {
          return new GroupReadSupport();
      }

  }
}
