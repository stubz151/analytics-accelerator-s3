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

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.SdkResponse;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.async.SdkPublisher;
import software.amazon.awssdk.http.async.AbortableInputStreamSubscriber;

/** @param <ResponseT> */
public class CustomTransformer<ResponseT extends SdkResponse>
    implements AsyncResponseTransformer<ResponseT, ResponseInputStream<ResponseT>> {
  private volatile CompletableFuture<ResponseInputStream<ResponseT>> future;
  private volatile ResponseT response;
  private final StringBuilder inspectorGadget;

  /** @param inspectorGadget */
  public CustomTransformer(StringBuilder inspectorGadget) {
    this.inspectorGadget = inspectorGadget;
  }

  /** @return */
  public String getInspector() {
    return inspectorGadget.toString();
  }

  /** @return */
  public CompletableFuture<ResponseInputStream<ResponseT>> prepare() {
    CompletableFuture<ResponseInputStream<ResponseT>> result = new CompletableFuture<>();
    inspectorGadget
        .append("Thread ")
        .append(Thread.currentThread().getName())
        .append("Preparing future\n");
    this.future = result;
    return result;
  }

  /** @param response */
  public void onResponse(ResponseT response) {
    this.response = response;
    inspectorGadget
        .append("Thread ")
        .append(Thread.currentThread().getName())
        .append("Setting response for future\n");
  }

  /** @param publisher */
  public void onStream(SdkPublisher<ByteBuffer> publisher) {
    inspectorGadget
        .append("Thread ")
        .append(Thread.currentThread().getName())
        .append("Starting thread\n");
    AbortableInputStreamSubscriber inputStreamSubscriber =
        AbortableInputStreamSubscriber.builder().build();
    publisher.subscribe(inputStreamSubscriber);
    inspectorGadget
        .append("Thread ")
        .append(Thread.currentThread().getName())
        .append("Set subscriber\n");
    this.future.complete(new ResponseInputStream<>(this.response, inputStreamSubscriber));
    inspectorGadget
        .append("Thread ")
        .append(Thread.currentThread().getName())
        .append("Finished stream\n");
  }

  /** @param error */
  public void exceptionOccurred(Throwable error) {
    inspectorGadget
        .append("Thread ")
        .append(Thread.currentThread().getName())
        .append("Exception on thread: ")
        .append(error)
        .append("\n");
    this.future.completeExceptionally(error);
  }

  /**
   * @param inspectorGadget
   * @return a response
   * @param <ResponseT>
   */
  static <ResponseT extends SdkResponse>
      AsyncResponseTransformer<ResponseT, ResponseInputStream<ResponseT>> toBlockingInputStream(
          StringBuilder inspectorGadget) {
    return new CustomTransformer<>(inspectorGadget);
  }
}
