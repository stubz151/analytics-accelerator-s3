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
package software.amazon.s3.analyticsaccelerator;

import java.io.UncheckedIOException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import lombok.Getter;
import lombok.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.s3.analyticsaccelerator.common.telemetry.ConfigurableTelemetry;
import software.amazon.s3.analyticsaccelerator.common.telemetry.Operation;
import software.amazon.s3.analyticsaccelerator.common.telemetry.Telemetry;
import software.amazon.s3.analyticsaccelerator.exceptions.ExceptionHandler;
import software.amazon.s3.analyticsaccelerator.request.*;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

/** Object client, based on AWS SDK v2 */
public class S3SdkObjectClient implements ObjectClient {
  private static final String HEADER_USER_AGENT = "User-Agent";
  private static final String HEADER_REFERER = "Referer";
  private static final Logger LOG = LoggerFactory.getLogger(S3SdkObjectClient.class);

  @Getter @NonNull private final S3AsyncClient s3AsyncClient;
  @NonNull private final Telemetry telemetry;
  @NonNull private final UserAgent userAgent;
  private final boolean closeAsyncClient;

  /**
   * Create an instance of a S3 client, with default configuration, for interaction with Amazon S3
   * compatible object stores. This takes ownership of the passed client and will close it on its
   * own close().
   *
   * @param s3AsyncClient Underlying client to be used for making requests to S3.
   */
  public S3SdkObjectClient(S3AsyncClient s3AsyncClient) {
    this(s3AsyncClient, ObjectClientConfiguration.DEFAULT);
  }

  /**
   * Create an instance of a S3 client, with default configuration, for interaction with Amazon S3
   * compatible object stores. This takes ownership of the passed client and will close it on its
   * own close().
   *
   * @param s3AsyncClient Underlying client to be used for making requests to S3.
   * @param closeAsyncClient if true, close the passed client on close.
   */
  public S3SdkObjectClient(S3AsyncClient s3AsyncClient, boolean closeAsyncClient) {
    this(s3AsyncClient, ObjectClientConfiguration.DEFAULT, closeAsyncClient);
  }

  /**
   * Create an instance of a S3 client, for interaction with Amazon S3 compatible object stores.
   * This takes ownership of the passed client and will close it on its own close().
   *
   * @param s3AsyncClient Underlying client to be used for making requests to S3.
   * @param objectClientConfiguration Configuration for object client.
   */
  public S3SdkObjectClient(
      S3AsyncClient s3AsyncClient, ObjectClientConfiguration objectClientConfiguration) {
    this(s3AsyncClient, objectClientConfiguration, true);
  }

  /**
   * Create an instance of a S3 client, for interaction with Amazon S3 compatible object stores.
   *
   * @param s3AsyncClient Underlying client to be used for making requests to S3.
   * @param objectClientConfiguration Configuration for object client.
   * @param closeAsyncClient if true, close the passed client on close.
   */
  public S3SdkObjectClient(
      @NonNull S3AsyncClient s3AsyncClient,
      @NonNull ObjectClientConfiguration objectClientConfiguration,
      boolean closeAsyncClient) {
    this.s3AsyncClient = s3AsyncClient;
    this.closeAsyncClient = closeAsyncClient;
    this.telemetry =
        new ConfigurableTelemetry(objectClientConfiguration.getTelemetryConfiguration());
    this.userAgent = new UserAgent();
    this.userAgent.prepend(objectClientConfiguration.getUserAgentPrefix());
  }

  /** Closes the underlying client if instructed by the constructor. */
  @Override
  public void close() {
    if (this.closeAsyncClient) {
      s3AsyncClient.close();
    }
  }

  /**
   * Make a headObject request to the object store.
   *
   * @param headRequest The HEAD request to be sent
   * @return HeadObjectResponse
   */
  @Override
  public CompletableFuture<ObjectMetadata> headObject(HeadRequest headRequest) {
    HeadObjectRequest.Builder builder =
        HeadObjectRequest.builder()
            .bucket(headRequest.getS3Uri().getBucket())
            .key(headRequest.getS3Uri().getKey());

    // Add User-Agent header to the request.
    builder.overrideConfiguration(
        AwsRequestOverrideConfiguration.builder()
            .putHeader(HEADER_USER_AGENT, this.userAgent.getUserAgent())
            .build());

    return this.telemetry
        .measureCritical(
            () ->
                Operation.builder()
                    .name(ObjectClientTelemetry.OPERATION_HEAD)
                    .attribute(ObjectClientTelemetry.uri(headRequest.getS3Uri()))
                    .build(),
            s3AsyncClient
                .headObject(builder.build())
                .thenApply(
                    headObjectResponse ->
                        ObjectMetadata.builder()
                            .contentLength(headObjectResponse.contentLength())
                            .etag(headObjectResponse.eTag())
                            .build()))
        .exceptionally(handleException(headRequest.getS3Uri()));
  }

  /**
   * Make a getObject request to the object store.
   *
   * @param getRequest The GET request to be sent
   * @return ResponseInputStream<GetObjectResponse>
   */
  @Override
  public CompletableFuture<ObjectContent> getObject(GetRequest getRequest) {
    return getObject(getRequest, null);
  }

  /**
   * Make a getObject request to the object store.
   *
   * @param getRequest The GET request to be sent
   * @param streamContext audit headers to be attached in the request header
   * @return ResponseInputStream<GetObjectResponse>
   */
  @Override
  public CompletableFuture<ObjectContent> getObject(
      GetRequest getRequest, StreamContext streamContext) {

    GetObjectRequest.Builder builder =
        GetObjectRequest.builder()
            .bucket(getRequest.getS3Uri().getBucket())
            .ifMatch(getRequest.getEtag())
            .key(getRequest.getS3Uri().getKey());

    final String range = getRequest.getRange().toHttpString();
    builder.range(range);

    final String referrerHeader;
    if (streamContext != null) {
      referrerHeader = streamContext.modifyAndBuildReferrerHeader(getRequest);
    } else {
      referrerHeader = getRequest.getReferrer().toString();
    }

    LOG.info("auditHeaders {}", referrerHeader);
    StringBuilder stringBuilder = new StringBuilder();

    builder.overrideConfiguration(
        AwsRequestOverrideConfiguration.builder()
            .putHeader(HEADER_REFERER, referrerHeader)
            .putHeader(HEADER_USER_AGENT, this.userAgent.getUserAgent())
            .build());

    return this.telemetry.measureCritical(
        () ->
            Operation.builder()
                .name(ObjectClientTelemetry.OPERATION_GET)
                .attribute(ObjectClientTelemetry.uri(getRequest.getS3Uri()))
                .attribute(ObjectClientTelemetry.rangeLength(getRequest.getRange()))
                .attribute(ObjectClientTelemetry.range(getRequest.getRange()))
                .build(),
        s3AsyncClient
            .getObject(builder.build(), CustomTransformer.toBlockingInputStream(stringBuilder))
            .thenApply(
                responseInputStream -> ObjectContent.builder().stream(responseInputStream).build())
            .exceptionally(handleException(getRequest.getS3Uri(), stringBuilder.toString())));
  }

  private <T> Function<Throwable, T> handleException(S3URI s3Uri, String inspector) {
    return throwable -> {
      System.out.println("no no no" + throwable);
      LOG.error("Inspector found on client: {}", inspector);
      Throwable cause =
          Optional.ofNullable(throwable.getCause())
              .filter(
                  t ->
                      throwable instanceof CompletionException
                          || throwable instanceof ExecutionException)
              .orElse(throwable);
      throw new UncheckedIOException(ExceptionHandler.toIOException(cause, s3Uri));
    };
  }

  private <T> Function<Throwable, T> handleException(S3URI s3Uri) {
    return throwable -> {
      System.out.println("no no no" + throwable);
      Throwable cause =
          Optional.ofNullable(throwable.getCause())
              .filter(
                  t ->
                      throwable instanceof CompletionException
                          || throwable instanceof ExecutionException)
              .orElse(throwable);
      throw new UncheckedIOException(ExceptionHandler.toIOException(cause, s3Uri));
    };
  }
}
