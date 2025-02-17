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

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/** */
public class CustomExecutorService {
  public static ExecutorService createCustomExecutorService(int threadCount) {
    AtomicLong threadCounter = new AtomicLong(0);

    ThreadFactory threadFactory =
        runnable -> {
          Thread thread = new Thread(runnable);
          thread.setName("custom-executor-thread-" + threadCounter.incrementAndGet());
          return thread;
        };

    ExecutorService executor = Executors.newFixedThreadPool(threadCount, threadFactory);

    return new LoggingExecutorService(executor);
  }

  /** */
  private static class LoggingExecutorService implements ExecutorService {
    private final ExecutorService delegate;

    public LoggingExecutorService(ExecutorService delegate) {
      this.delegate = delegate;
    }

    /** @param command the runnable task */
    @Override
    public void execute(Runnable command) {
      delegate.execute(
          () -> {
            long startTime = System.nanoTime();
            System.out.println(
                "Thread " + Thread.currentThread().getName() + " started at " + startTime);
            try {
              command.run();
            } finally {
              long endTime = System.nanoTime();
              System.out.println(
                  "Thread "
                      + Thread.currentThread().getName()
                      + " finished at "
                      + endTime
                      + " (duration: "
                      + TimeUnit.NANOSECONDS.toMillis(endTime - startTime)
                      + "ms)");
            }
          });
    }

    /** */
    // Delegate the rest of the ExecutorService methods to the underlying executor
    @Override
    public void shutdown() {
      delegate.shutdown();
    }

    /** @return */
    @Override
    public List<Runnable> shutdownNow() {
      return delegate.shutdownNow();
    }

    /** @return */
    @Override
    public boolean isShutdown() {
      return delegate.isShutdown();
    }

    /** @return */
    @Override
    public boolean isTerminated() {
      return delegate.isTerminated();
    }

    /**
     * @param timeout the maximum time to wait
     * @param unit the time unit of the timeout argument
     * @return
     * @throws InterruptedException
     */
    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
      return delegate.awaitTermination(timeout, unit);
    }

    /**
     * @param tasks the collection of tasks
     * @return
     * @param <T>
     * @throws InterruptedException
     */
    @Override
    public <T> java.util.List<java.util.concurrent.Future<T>> invokeAll(
        java.util.Collection<? extends java.util.concurrent.Callable<T>> tasks)
        throws InterruptedException {
      return delegate.invokeAll(tasks);
    }

    /**
     * @param tasks the collection of tasks
     * @param timeout the maximum time to wait
     * @param unit the time unit of the timeout argument
     * @return
     * @param <T>
     * @throws InterruptedException
     */
    @Override
    public <T> java.util.List<java.util.concurrent.Future<T>> invokeAll(
        java.util.Collection<? extends java.util.concurrent.Callable<T>> tasks,
        long timeout,
        TimeUnit unit)
        throws InterruptedException {
      return delegate.invokeAll(tasks, timeout, unit);
    }

    /**
     * @param tasks the collection of tasks
     * @return
     * @param <T>
     * @throws InterruptedException
     * @throws java.util.concurrent.ExecutionException
     */
    @Override
    public <T> T invokeAny(java.util.Collection<? extends java.util.concurrent.Callable<T>> tasks)
        throws InterruptedException, java.util.concurrent.ExecutionException {
      return delegate.invokeAny(tasks);
    }

    /**
     * @param tasks the collection of tasks
     * @param timeout the maximum time to wait
     * @param unit the time unit of the timeout argument
     * @return
     * @param <T>
     * @throws InterruptedException
     * @throws java.util.concurrent.ExecutionException
     * @throws java.util.concurrent.TimeoutException
     */
    @Override
    public <T> T invokeAny(
        java.util.Collection<? extends java.util.concurrent.Callable<T>> tasks,
        long timeout,
        TimeUnit unit)
        throws InterruptedException, java.util.concurrent.ExecutionException,
            java.util.concurrent.TimeoutException {
      return delegate.invokeAny(tasks, timeout, unit);
    }

    /**
     * @param task the task to submit
     * @return
     */
    @Override
    public java.util.concurrent.Future<?> submit(Runnable task) {
      return delegate.submit(task);
    }

    /**
     * @param task the task to submit
     * @param result the result to return
     * @return
     * @param <T>
     */
    @Override
    public <T> java.util.concurrent.Future<T> submit(Runnable task, T result) {
      return delegate.submit(task, result);
    }

    /**
     * @param task the task to submit
     * @return
     * @param <T>
     */
    @Override
    public <T> java.util.concurrent.Future<T> submit(java.util.concurrent.Callable<T> task) {
      return delegate.submit(task);
    }
  }

  /** @param args */
  public static void main(String[] args) {
    ExecutorService executor = createCustomExecutorService(4);

    for (int i = 0; i < 10; i++) {
      executor.execute(
          () -> {
            try {
              Thread.sleep(1000);
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
          });
    }

    executor.shutdown();
    try {
      executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}
