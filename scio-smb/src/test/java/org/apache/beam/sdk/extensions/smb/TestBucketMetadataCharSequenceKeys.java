/*
 * Copyright 2024 Spotify AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.beam.sdk.extensions.smb;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;
import java.util.Objects;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;

/**
 * Test metadata that uses CharSequence keys and extracts variable-length secondary keys.
 *
 * <p>Format: Input strings like "A:Alpha" where:
 * - Primary key: first character ("A")
 * - Secondary key: everything after the colon ("Alpha")
 *
 * <p>This allows testing with variable-length secondary keys to expose the VarInt prefix bug.
 */
public class TestBucketMetadataCharSequenceKeys
    extends BucketMetadata<CharSequence, CharSequence, String> {

  static TestBucketMetadataCharSequenceKeys of(int numBuckets, int numShards) {
    return of(numBuckets, numShards, SortedBucketIO.DEFAULT_FILENAME_PREFIX);
  }

  static TestBucketMetadataCharSequenceKeys of(
      int numBuckets, int numShards, String filenamePrefix) {
    try {
      return new TestBucketMetadataCharSequenceKeys(
          numBuckets, numShards, BucketMetadata.HashType.MURMUR3_32, filenamePrefix);
    } catch (CannotProvideCoderException | Coder.NonDeterministicException e) {
      throw new RuntimeException(e);
    }
  }

  TestBucketMetadataCharSequenceKeys(
      @JsonProperty("numBuckets") int numBuckets,
      @JsonProperty("numShards") int numShards,
      @JsonProperty("hashType") BucketMetadata.HashType hashType,
      @JsonProperty("filenamePrefix") String filenamePrefix)
      throws CannotProvideCoderException, Coder.NonDeterministicException {
    this(BucketMetadata.CURRENT_VERSION, numBuckets, numShards, hashType, filenamePrefix);
  }

  @JsonCreator
  TestBucketMetadataCharSequenceKeys(
      @JsonProperty("version") int version,
      @JsonProperty("numBuckets") int numBuckets,
      @JsonProperty("numShards") int numShards,
      @JsonProperty("hashType") BucketMetadata.HashType hashType,
      @JsonProperty("filenamePrefix") String filenamePrefix)
      throws CannotProvideCoderException, Coder.NonDeterministicException {
    super(
        version,
        numBuckets,
        numShards,
        CharSequence.class,
        CharSequence.class,
        hashType,
        filenamePrefix);
  }

  @Override
  public Map<Class<?>, Coder<?>> coderOverrides() {
    return AvroUtils.coderOverrides();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    TestBucketMetadataCharSequenceKeys metadata = (TestBucketMetadataCharSequenceKeys) o;
    return this.getNumBuckets() == metadata.getNumBuckets()
        && this.getNumShards() == metadata.getNumShards()
        && this.getHashType() == metadata.getHashType();
  }

  @Override
  public int hashCode() {
    return Objects.hash(getNumBuckets(), getNumShards(), getHashType());
  }

  /**
   * Extract primary key: first character before the colon.
   *
   * <p>Example: "A:Alpha" → "A"
   */
  @Override
  public CharSequence extractKeyPrimary(final String value) {
    if (value == null || value.isEmpty()) {
      return null;
    }
    int colonIdx = value.indexOf(':');
    if (colonIdx < 0) {
      return null;
    }
    return value.substring(0, colonIdx);
  }

  /**
   * Extract secondary key: everything after the colon.
   *
   * <p>Example: "A:Alpha" → "Alpha"
   *
   * <p>This produces variable-length strings which will expose the VarInt prefix bug if present.
   */
  @Override
  public CharSequence extractKeySecondary(final String value) {
    if (value == null || value.isEmpty()) {
      return null;
    }
    int colonIdx = value.indexOf(':');
    if (colonIdx < 0 || colonIdx == value.length() - 1) {
      return null;
    }
    return value.substring(colonIdx + 1);
  }

  @Override
  int hashPrimaryKeyMetadata() {
    return Objects.hash("primaryKey");
  }

  @Override
  int hashSecondaryKeyMetadata() {
    return Objects.hash("secondaryKey");
  }
}
