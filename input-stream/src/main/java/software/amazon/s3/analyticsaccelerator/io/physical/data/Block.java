/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package software.amazon.s3.analyticsaccelerator.io.physical.data;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import lombok.Getter;
import lombok.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.s3.analyticsaccelerator.S3SdkObjectClient;
import software.amazon.s3.analyticsaccelerator.common.Preconditions;
import software.amazon.s3.analyticsaccelerator.common.telemetry.Operation;
import software.amazon.s3.analyticsaccelerator.common.telemetry.Telemetry;
import software.amazon.s3.analyticsaccelerator.request.GetRequest;
import software.amazon.s3.analyticsaccelerator.request.ObjectClient;
import software.amazon.s3.analyticsaccelerator.request.ObjectContent;
import software.amazon.s3.analyticsaccelerator.request.Range;
import software.amazon.s3.analyticsaccelerator.request.ReadMode;
import software.amazon.s3.analyticsaccelerator.request.Referrer;
import software.amazon.s3.analyticsaccelerator.request.StreamContext;
import software.amazon.s3.analyticsaccelerator.util.ObjectKey;
import software.amazon.s3.analyticsaccelerator.util.StreamAttributes;
import software.amazon.s3.analyticsaccelerator.util.StreamUtils;

/**
 * A Block holding part of an object's data and owning its own async process for fetching part of
 * the object.
 */
public class Block implements Closeable {
  private CompletableFuture<ObjectContent> source;
  private CompletableFuture<byte[]> data;
  private final ObjectKey objectKey;
  private final Range range;
  private final Telemetry telemetry;
  private final ObjectClient objectClient;
  private final StreamContext streamContext;
  private final ReadMode readMode;
  private final Referrer referrer;

  @Getter private final long start;
  @Getter private final long end;
  @Getter private final long generation;

  private static final String OPERATION_BLOCK_GET_ASYNC = "block.get.async";
  private static final String OPERATION_BLOCK_GET_JOIN = "block.get.join";

  private static final int MAX_RETRIES = 20;

  private static final Logger LOG = LoggerFactory.getLogger(Block.class);

  /**
   * Constructs a Block data.
   *
   * @param objectKey the etag and S3 URI of the object
   * @param objectClient the object client to use to interact with the object store
   * @param telemetry an instance of {@link Telemetry} to use
   * @param start start of the block
   * @param end end of the block
   * @param generation generation of the block in a sequential read pattern (should be 0 by default)
   * @param readMode read mode describing whether this is a sync or async fetch
   */
  public Block(
      @NonNull ObjectKey objectKey,
      @NonNull ObjectClient objectClient,
      @NonNull Telemetry telemetry,
      long start,
      long end,
      long generation,
      @NonNull ReadMode readMode) {

    this(objectKey, objectClient, telemetry, start, end, generation, readMode, null);
  }

  /**
   * Constructs a Block data.
   *
   * @param objectKey the etag and S3 URI of the object
   * @param objectClient the object client to use to interact with the object store
   * @param telemetry an instance of {@link Telemetry} to use
   * @param start start of the block
   * @param end end of the block
   * @param generation generation of the block in a sequential read pattern (should be 0 by default)
   * @param readMode read mode describing whether this is a sync or async fetch
   * @param streamContext contains audit headers to be attached in the request header
   */
  public Block(
      @NonNull ObjectKey objectKey,
      @NonNull ObjectClient objectClient,
      @NonNull Telemetry telemetry,
      long start,
      long end,
      long generation,
      @NonNull ReadMode readMode,
      StreamContext streamContext) {

    Preconditions.checkArgument(
        0 <= generation, "`generation` must be non-negative; was: %s", generation);
    Preconditions.checkArgument(0 <= start, "`start` must be non-negative; was: %s", start);
    Preconditions.checkArgument(0 <= end, "`end` must be non-negative; was: %s", end);
    Preconditions.checkArgument(
        start <= end, "`start` must be less than `end`; %s is not less than %s", start, end);

    this.start = start;
    this.end = end;
    this.generation = generation;
    this.telemetry = telemetry;
    this.objectKey = objectKey;
    this.range = new Range(start, end);
    this.objectClient = objectClient;
    this.streamContext = streamContext;
    this.readMode = readMode;
    this.referrer = new Referrer(range.toHttpString(), readMode);

    generateSourceAndData();
  }

  /** Method to help construct source and data */
  private void generateSourceAndData() {
    GetRequest.GetRequestBuilder getRequestBuilder =
        GetRequest.builder()
            .s3Uri(this.objectKey.getS3URI())
            .range(this.range)
            .etag(this.objectKey.getEtag())
            .referrer(referrer);

    GetRequest getRequest = getRequestBuilder.build();
    this.source =
        this.telemetry.measureCritical(
            () ->
                Operation.builder()
                    .name(OPERATION_BLOCK_GET_ASYNC)
                    .attribute(StreamAttributes.uri(this.objectKey.getS3URI()))
                    .attribute(StreamAttributes.etag(this.objectKey.getEtag()))
                    .attribute(StreamAttributes.range(this.range))
                    .attribute(StreamAttributes.generation(generation))
                    .build(),
            objectClient.getObject(getRequest, streamContext));

    this.data = this.source.thenApply(StreamUtils::toByteArray);
  }

  /**
   * Reads a byte from the underlying object
   *
   * @param pos The position to read
   * @return an unsigned int representing the byte that was read
   * @throws IOException if an I/O error occurs
   */
  public int read(long pos) throws IOException {
    Preconditions.checkArgument(0 <= pos, "`pos` must not be negative");

    byte[] content = this.getDataWithRetries();
    return Byte.toUnsignedInt(content[posToOffset(pos)]);
  }

  /**
   * Reads data into the provided buffer
   *
   * @param buf buffer to read data into
   * @param off start position in buffer at which data is written
   * @param len length of data to be read
   * @param pos the position to begin reading from
   * @return the total number of bytes read into the buffer
   * @throws IOException if an I/O error occurs
   */
  public int read(byte @NonNull [] buf, int off, int len, long pos) throws IOException {
    Preconditions.checkArgument(0 <= pos, "`pos` must not be negative");
    Preconditions.checkArgument(0 <= off, "`off` must not be negative");
    Preconditions.checkArgument(0 <= len, "`len` must not be negative");
    Preconditions.checkArgument(off < buf.length, "`off` must be less than size of buffer");

    byte[] content = this.getDataWithRetries();
    int contentOffset = posToOffset(pos);
    int available = content.length - contentOffset;
    int bytesToCopy = Math.min(len, available);

    for (int i = 0; i < bytesToCopy; ++i) {
      buf[off + i] = content[contentOffset + i];
    }

    return bytesToCopy;
  }

  /**
   * Does this block contain the position?
   *
   * @param pos the position
   * @return true if the byte at the position is contained by this block
   */
  public boolean contains(long pos) {
    Preconditions.checkArgument(0 <= pos, "`pos` must not be negative");

    return start <= pos && pos <= end;
  }

  /**
   * Determines the offset in the Block corresponding to a position in an object.
   *
   * @param pos the position of a byte in the object
   * @return the offset in the byte buffer underlying this Block
   */
  private int posToOffset(long pos) {
    return (int) (pos - start);
  }

  /**
   * Returns the bytes fetched by the issued {@link GetRequest}. If it receives an IOException from
   * {@link S3SdkObjectClient}, retries for MAX_RETRIES count.
   *
   * @return the bytes fetched by the issued {@link GetRequest}.
   * @throws IOException if an I/O error occurs after maximum retry counts
   */
  private byte[] getDataWithRetries() throws IOException {
    for (int i = 0; i < MAX_RETRIES; i++) {
      try {
        return this.getData();
      } catch (IOException ex) {
        if (ex.getClass() == IOException.class) {
          if (i < MAX_RETRIES - 1) {
            LOG.info("Get data failed. Retrying. Retry Count {}", i);
            generateSourceAndData();
          } else {
            LOG.error("Cannot read block file. Retry reached the limit");
            throw new IOException("Cannot read block file", ex.getCause());
          }
        } else {
          throw ex;
        }
      }
    }
    throw new IOException("Cannot read block file", new IOException("Error while getting block"));
  }

  /**
   * Returns the bytes fetched by the issued {@link GetRequest}. This method will block until the
   * data is fully available.
   *
   * @return the bytes fetched by the issued {@link GetRequest}.
   * @throws IOException if an I/O error occurs
   */
  private byte[] getData() throws IOException {
    return this.telemetry.measureJoinCritical(
        () ->
            Operation.builder()
                .name(OPERATION_BLOCK_GET_JOIN)
                .attribute(StreamAttributes.uri(this.objectKey.getS3URI()))
                .attribute(StreamAttributes.etag(this.objectKey.getEtag()))
                .attribute(StreamAttributes.range(this.range))
                .attribute(StreamAttributes.rangeLength(this.range.getLength()))
                .build(),
        this.data);
  }

  /** Closes the {@link Block} and frees up all resources it holds */
  @Override
  public void close() {
    // Only the source needs to be canceled, the continuation will cancel on its own
    this.source.cancel(false);
  }
}
