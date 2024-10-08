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

package org.apache.druid.segment.data;

import org.apache.druid.guice.annotations.ExtensionPoint;
import org.apache.druid.segment.column.TypeStrategy;
import org.apache.druid.segment.writeout.WriteOutBytes;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Comparator;

@ExtensionPoint
public interface ObjectStrategy<T> extends Comparator<T>
{
  Class<? extends T> getClazz();

  /**
   * Convert values from their underlying byte representation.
   *
   * Implementations of this method <i>may</i> change the given buffer's mark, or limit, and position.
   *
   * Implementations of this method <i>may not</i> store the given buffer in a field of the "deserialized" object,
   * need to use {@link ByteBuffer#slice()}, {@link ByteBuffer#asReadOnlyBuffer()} or {@link ByteBuffer#duplicate()} in
   * this case.
   *
   * @param buffer buffer to read value from
   * @param numBytes number of bytes used to store the value, starting at buffer.position()
   * @return an object created from the given byte buffer representation
   */
  @Nullable
  T fromByteBuffer(ByteBuffer buffer, int numBytes);

  @Nullable
  byte[] toBytes(@Nullable T val);

  /**
   * Whether {@link #compare} is valid or not.
   */
  default boolean canCompare()
  {
    return true;
  }

  /**
   * Whether the {@link #fromByteBuffer(ByteBuffer, int)}, {@link #fromByteBufferWithSize(ByteBuffer)}, and
   * {@link #fromByteBufferSafe(ByteBuffer, int)} methods return an object that may retain a reference to the underlying
   * memory provided by a {@link ByteBuffer}. If a reference is sometimes retained, this method returns true. It
   * returns false if, and only if, a reference is *never* retained.
   * <p>
   * If this method returns true, and the caller does not control the lifecycle of the underlying memory or cannot
   * ensure that it will not change over the lifetime of the returned object, callers should copy the memory to a new
   * location that they do control the lifecycle of and will be available for the duration of the returned object.
   *
   * @see TypeStrategy#readRetainsBufferReference()
   */
  default boolean readRetainsBufferReference()
  {
    return true;
  }

  /**
   * Reads 4-bytes numBytes from the given buffer, and then delegates to {@link #fromByteBuffer(ByteBuffer, int)}.
   */
  default T fromByteBufferWithSize(ByteBuffer buffer)
  {
    int size = buffer.getInt();
    ByteBuffer bufferToUse = buffer.asReadOnlyBuffer();
    bufferToUse.limit(bufferToUse.position() + size);
    buffer.position(bufferToUse.limit());

    return fromByteBuffer(bufferToUse, size);
  }

  default void writeTo(T val, WriteOutBytes out) throws IOException
  {
    byte[] bytes = toBytes(val);
    if (bytes != null) {
      out.write(bytes);
    }
  }

  /**
   * Convert values from their underlying byte representation, when the underlying bytes might be corrupted or
   * maliciously constructed
   *
   * Implementations of this method <i>absolutely must never</i> perform any sun.misc.Unsafe based memory read or write
   * operations from instructions contained in the data read from this buffer without first validating the data. If the
   * data cannot be validated, all read and write operations from instructions in this data must be done directly with
   * the {@link ByteBuffer} methods, or using {@link SafeWritableMemory} if
   * {@link org.apache.datasketches.memory.Memory} is employed to materialize the value.
   *
   * Implementations of this method <i>may</i> change the given buffer's mark, or limit, and position.
   *
   * Implementations of this method <i>may not</i> store the given buffer in a field of the "deserialized" object,
   * need to use {@link ByteBuffer#slice()}, {@link ByteBuffer#asReadOnlyBuffer()} or {@link ByteBuffer#duplicate()} in
   * this case.
   *
   *
   * @param buffer buffer to read value from
   * @param numBytes number of bytes used to store the value, starting at buffer.position()
   * @return an object created from the given byte buffer representation
   */
  @Nullable
  default T fromByteBufferSafe(ByteBuffer buffer, int numBytes)
  {
    return fromByteBuffer(buffer, numBytes);
  }
}
