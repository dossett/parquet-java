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
package org.apache.parquet.column.values.bitpacking;

import static org.junit.Assert.assertArrayEquals;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.List;
import org.junit.Assume;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestByteBitPacking512VectorLE {
  private static final Logger LOG = LoggerFactory.getLogger(TestByteBitPacking512VectorLE.class);
  private static final int MIN_TEST_VALUES = 64;
  private static final int MAX_TEST_VALUES = 1 << 20;

  @Test
  public void unpackValuesUsingVector() {
    Assume.assumeTrue(ParquetReadRouter.getSupportVectorFromCPUFlags() == VectorSupport.VECTOR_512);
    for (int i = 1; i <= 32; i++) {
      unpackValuesUsingVectorBitWidth(i);
    }
  }

  private void unpackValuesUsingVectorBitWidth(int bitWidth) {
    List<int[]> intInputs = getRangeData(bitWidth);

    for (int[] intInput : intInputs) {
      int pack8Count = intInput.length / 8;
      int byteOutputSize = pack8Count * bitWidth;
      byte[] byteOutput = new byte[byteOutputSize];
      int[] output1 = new int[intInput.length];
      int[] output2 = new int[intInput.length];
      int[] output3 = new int[intInput.length];

      BytePacker bytePacker = Packer.LITTLE_ENDIAN.newBytePacker(bitWidth);
      for (int i = 0; i < pack8Count; i++) {
        bytePacker.pack8Values(intInput, 8 * i, byteOutput, bitWidth * i);
      }

      unpack8Values(bitWidth, byteOutput, output1);
      unpackValuesUsingVectorArray(bitWidth, byteOutput, output2);

      ByteBuffer byteBuffer = ByteBuffer.wrap(byteOutput);
      unpackValuesUsingVectorByteBuffer(bitWidth, byteBuffer, output3);

      assertArrayEquals(intInput, output1);
      assertArrayEquals(intInput, output2);
      assertArrayEquals(intInput, output3);
    }
  }

  public void unpack8Values(int bitWidth, byte[] input, int[] output) {
    BytePacker bytePacker = Packer.LITTLE_ENDIAN.newBytePacker(bitWidth);
    int len = input.length;
    int i = 0, j = 0;
    for (; i < len; i += bitWidth, j += 8) {
      bytePacker.unpack8Values(input, i, output, j);
    }
  }

  public void unpackValuesUsingVectorArray(int bitWidth, byte[] input, int[] output) {
    BytePacker bytePacker = Packer.LITTLE_ENDIAN.newBytePacker(bitWidth);
    BytePacker bytePackerVector = Packer.LITTLE_ENDIAN.newBytePackerVector(bitWidth);

    int byteIndex = 0;
    int valueIndex = 0;
    int totalByteCount = input.length;
    int outCountPerVector = bytePackerVector.getUnpackCount();
    int inputByteCountPerVector = outCountPerVector / 8 * bitWidth;
    int totalByteCountVector = totalByteCount - inputByteCountPerVector;

    for (;
        byteIndex < totalByteCountVector;
        byteIndex += inputByteCountPerVector, valueIndex += outCountPerVector) {
      bytePackerVector.unpackValuesUsingVector(input, byteIndex, output, valueIndex);
    }
    for (; byteIndex < totalByteCount; byteIndex += bitWidth, valueIndex += 8) {
      bytePacker.unpack8Values(input, byteIndex, output, valueIndex);
    }
  }

  public void unpackValuesUsingVectorByteBuffer(int bitWidth, ByteBuffer input, int[] output) {
    BytePacker bytePacker = Packer.LITTLE_ENDIAN.newBytePacker(bitWidth);
    BytePacker bytePackerVector = Packer.LITTLE_ENDIAN.newBytePackerVector(bitWidth);

    int byteIndex = 0;
    int valueIndex = 0;
    int totalByteCount = input.capacity();
    int outCountPerVector = bytePackerVector.getUnpackCount();
    int inputByteCountPerVector = outCountPerVector / 8 * bitWidth;
    int totalByteCountVector = totalByteCount - inputByteCountPerVector;

    for (;
        byteIndex < totalByteCountVector;
        byteIndex += inputByteCountPerVector, valueIndex += outCountPerVector) {
      bytePackerVector.unpackValuesUsingVector(input, byteIndex, output, valueIndex);
    }
    for (; byteIndex < totalByteCount; byteIndex += bitWidth, valueIndex += 8) {
      bytePacker.unpack8Values(input, byteIndex, output, valueIndex);
    }
  }

  private List<int[]> getRangeData(int bitWidth) {
    long maxValue = getMaxValue(bitWidth);
    long maxValueFilled = maxValue + 1;
    int len = (int) Math.min(Math.max(MIN_TEST_VALUES, maxValueFilled), MAX_TEST_VALUES);
    int[] array = new int[len];
    if (bitWidth == 32) {
      long min = Integer.MIN_VALUE;
      long max = Integer.MAX_VALUE;
      long span = max - min;
      for (int i = 0; i < len; i++) {
        long value = min + (span * i) / (len - 1);
        array[i] = (int) value;
      }
    } else {
      for (int i = 0; i < len; i++) {
        long value = (maxValue * i) / (len - 1);
        array[i] = (int) value;
      }
    }
    return List.of(array);
  }

  private long getMaxValue(int bitWidth) {
    return BigDecimal.valueOf(Math.pow(2, bitWidth)).longValue() - 1;
  }
}
