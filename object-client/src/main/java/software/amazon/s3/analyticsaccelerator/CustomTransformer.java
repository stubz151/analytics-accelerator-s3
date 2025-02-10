package software.amazon.s3.analyticsaccelerator;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.SdkResponse;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.async.SdkPublisher;
import software.amazon.awssdk.http.async.AbortableInputStreamSubscriber;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;


public class CustomTransformer<ResponseT extends SdkResponse> implements AsyncResponseTransformer<ResponseT, ResponseInputStream<ResponseT>> {
  private volatile CompletableFuture<ResponseInputStream<ResponseT>> future;
  private volatile ResponseT response;

  public CustomTransformer() {
  }

  public CompletableFuture<ResponseInputStream<ResponseT>> prepare() {
    CompletableFuture<ResponseInputStream<ResponseT>> result = new CompletableFuture<>();
    System.out.println("Thread " + Thread.currentThread().getName() + "Preparing future");
    this.future = result;
    return result;
  }

  public void onResponse(ResponseT response) {
    this.response = response;
    System.out.println("Thread " + Thread.currentThread().getName() + "Setting response for future");
  }

  public void onStream(SdkPublisher<ByteBuffer> publisher) {
    System.out.println("Thread " + Thread.currentThread().getName() + "Starting stream");
    AbortableInputStreamSubscriber inputStreamSubscriber = AbortableInputStreamSubscriber.builder().build();
    publisher.subscribe(inputStreamSubscriber);
    System.out.println("Thread " + Thread.currentThread().getName() + "Set subscriber");
    this.future.complete(new ResponseInputStream<>(this.response, inputStreamSubscriber));
    System.out.println("Thread " + Thread.currentThread().getName() + "Finished stream");
  }

  public void exceptionOccurred(Throwable error) {
    System.out.println("Thread " + Thread.currentThread().getName() + "Excepotion on thread " + error);
    this.future.completeExceptionally(error);
  }

  static <ResponseT extends SdkResponse>
  AsyncResponseTransformer<ResponseT, ResponseInputStream<ResponseT>> toBlockingInputStream() {
    return new CustomTransformer<>();
  }
}