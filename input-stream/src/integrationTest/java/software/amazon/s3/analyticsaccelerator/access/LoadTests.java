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
package software.amazon.s3.analyticsaccelerator.access;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.internal.crt.S3CrtAsyncClient;
import software.amazon.s3.analyticsaccelerator.CustomExecutorService;
import software.amazon.s3.analyticsaccelerator.S3SdkObjectClient;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStream;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStreamConfiguration;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStreamFactory;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

public class LoadTests {

  @Test
  public void testfish() throws IOException, InterruptedException {
    String bucket = System.getenv("BUCKET");

    List<String> fileNames = new ArrayList<>();
    // Generate file names with part numbers from 1 to 44
    for (int startPart = 1; startPart <= 63; startPart++) {
      for (int endPart = startPart; endPart <= 44; endPart++) {

        String prefix = String.format("%04d", startPart);
        String suffix = String.format("%02d", endPart);
        String uris = String.format("dataset/store_sales/%s_part_%s.parquet", prefix, suffix);
        fileNames.add(uris);
      }
    }
    Collections.shuffle(fileNames);
    List<Thread> threads = new ArrayList<>();
    try {
      ExecutorService customExecutor = CustomExecutorService.createCustomExecutorService(300);
      S3AsyncClient s3Client =
          S3CrtAsyncClient.builder()
              .region(Region.US_EAST_1)
              .maxConcurrency(10)
              .futureCompletionExecutor(customExecutor)
              .build();

      S3SeekableInputStreamFactory s3SeekableInputStreamFactory =
          new S3SeekableInputStreamFactory(
              new S3SdkObjectClient(s3Client), S3SeekableInputStreamConfiguration.DEFAULT);
      for (String runUri : fileNames) {
        Thread thread =
            new Thread(
                () -> {
                  S3URI s3URI = S3URI.of(bucket, runUri);

                  try {
                    S3SeekableInputStream stream = s3SeekableInputStreamFactory.createStream(s3URI);
                    System.out.println(stream.readTail(new byte[100000], 0, 100000));
                  } catch (IOException e) {
                    System.out.println("starting error");
                    System.out.println(e);
                    throw new RuntimeException(e);
                  }
                });
        threads.add(thread);
      }
    } catch (Exception e) {
      System.out.println("error starting");
      throw new RuntimeException(e);
    }
    System.out.println("actually passed one");
    ExecutorService executor = Executors.newFixedThreadPool(1);

    for (Thread thread : threads) {
      executor.submit(
          () -> {
            try {
              thread.start();
            } catch (Exception e) {
              System.out.println("nah");
              System.out.println(e);
              e.printStackTrace();
            }
          });
    }

    executor.shutdown();
    executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);

    for (Thread thread : threads) {
      try {
        thread.join();
      } catch (InterruptedException e) {
        System.out.println("interrupted");
        System.out.println(e);
        e.printStackTrace();
      }
    }
  }
}
