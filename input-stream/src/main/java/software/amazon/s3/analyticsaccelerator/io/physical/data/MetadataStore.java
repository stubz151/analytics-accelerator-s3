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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import lombok.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.s3.analyticsaccelerator.common.telemetry.Operation;
import software.amazon.s3.analyticsaccelerator.common.telemetry.Telemetry;
import software.amazon.s3.analyticsaccelerator.io.physical.PhysicalIOConfiguration;
import software.amazon.s3.analyticsaccelerator.request.HeadRequest;
import software.amazon.s3.analyticsaccelerator.request.ObjectClient;
import software.amazon.s3.analyticsaccelerator.request.ObjectMetadata;
import software.amazon.s3.analyticsaccelerator.util.S3URI;
import software.amazon.s3.analyticsaccelerator.util.StreamAttributes;

/** Class responsible for fetching and potentially caching object metadata. */
@SuppressFBWarnings(
    value = "SIC_INNER_SHOULD_BE_STATIC_ANON",
    justification =
        "Inner class is created very infrequently, and fluency justifies the extra pointer")
public class MetadataStore implements Closeable {
  private final ObjectClient objectClient;
  private final Telemetry telemetry;
  private final Map<S3URI, CompletableFuture<ObjectMetadata>> cache;

  private static final Logger LOG = LoggerFactory.getLogger(MetadataStore.class);
  private static final String OPERATION_METADATA_HEAD_ASYNC = "metadata.store.head.async";
  private static final String OPERATION_METADATA_HEAD_JOIN = "metadata.store.head.join";

  /**
   * Constructs a new MetadataStore.
   *
   * @param objectClient the object client to use for object store interactions.
   * @param telemetry The {@link Telemetry} to use to report measurements.
   * @param configuration a configuration of PhysicalIO.
   */
  public MetadataStore(
      @NonNull ObjectClient objectClient,
      @NonNull Telemetry telemetry,
      @NonNull PhysicalIOConfiguration configuration) {
    this.objectClient = objectClient;
    this.telemetry = telemetry;
    this.cache =
        Collections.synchronizedMap(
            new LinkedHashMap<S3URI, CompletableFuture<ObjectMetadata>>() {
              @Override
              protected boolean removeEldestEntry(
                  final Map.Entry<S3URI, CompletableFuture<ObjectMetadata>> eldest) {
                return this.size() > configuration.getMetadataStoreCapacity();
              }
            });
  }

  /**
   * Get the metadata for an object synchronously (either from cache or the underlying object
   * store).
   *
   * @param s3URI the object to fetch the metadata for
   * @return returns the {@link ObjectMetadata}.
   * @throws IOException if an I/O error occurs
   */
  public ObjectMetadata get(S3URI s3URI) throws IOException {
    return telemetry.measureJoinCritical(
        () ->
            Operation.builder()
                .name(OPERATION_METADATA_HEAD_JOIN)
                .attribute(StreamAttributes.uri(s3URI))
                .build(),
        this.asyncGet(s3URI));
  }

  /**
   * Evicts the specified key from the cache
   *
   * @param s3URI the s3 uri of the object to evict
   * @return a boolean stating if the object existed or not
   */
  public boolean evictKey(S3URI s3URI) {
    return this.cache.remove(s3URI) != null;
  }

  /**
   * Get the metadata for an object asynchronously (either from cache or the underlying object
   * store).
   *
   * @param s3URI the object to fetch the metadata for
   * @return returns the {@link CompletableFuture} that holds object's metadata.
   */
  public synchronized CompletableFuture<ObjectMetadata> asyncGet(S3URI s3URI) {
    return this.cache.computeIfAbsent(
        s3URI,
        uri ->
            telemetry.measureCritical(
                () ->
                    Operation.builder()
                        .name(OPERATION_METADATA_HEAD_ASYNC)
                        .attribute(StreamAttributes.uri(s3URI))
                        .build(),
                objectClient.headObject(HeadRequest.builder().s3Uri(s3URI).build())));
  }

  /**
   * Allows storing of objectMetadata to cache. Useful when content length is already known, so can
   * skip the HEAD request.
   *
   * @param s3URI the object to store metadata for
   * @param objectMetadata Object metadata
   */
  public synchronized void storeObjectMetadata(S3URI s3URI, ObjectMetadata objectMetadata) {
    if (objectMetadata != null) {
      this.cache.put(s3URI, CompletableFuture.completedFuture(objectMetadata));
    }
  }

  /**
   * Utility method that cancels a {@link CompletableFuture} ignoring any exceptions.
   *
   * @param future an instance of {@link CompletableFuture} to cancel
   */
  private void safeCancel(CompletableFuture<ObjectMetadata> future) {
    if (!future.isDone()) {
      try {
        future.cancel(false);
      } catch (Exception e) {
        LOG.error("Error cancelling ObjectMetadata future", e);
      }
    }
  }

  /** Closes the {@link MetadataStore} and frees up all resources it holds. */
  @Override
  public void close() {
    this.cache.values().forEach(this::safeCancel);
  }
}
