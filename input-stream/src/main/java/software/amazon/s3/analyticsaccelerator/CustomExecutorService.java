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
  public static ExecutorService createCustomExecutorService(
      int threadCount, StringBuilder inspector) {
    AtomicLong threadCounter = new AtomicLong(0);

    ThreadFactory threadFactory =
        runnable -> {
          Thread thread = new Thread(runnable);
          thread.setName("custom-executor-thread-" + threadCounter.incrementAndGet());
          return thread;
        };

    ExecutorService executor = Executors.newFixedThreadPool(threadCount, threadFactory);

    return new LoggingExecutorService(executor, inspector);
  }

  /** */
  private static class LoggingExecutorService implements ExecutorService {
    private final ExecutorService delegate;
    private final StringBuilder inspector;

    public LoggingExecutorService(ExecutorService delegate, StringBuilder inspector) {
      this.delegate = delegate;
      this.inspector = inspector;
    }

    /** @param command the runnable task */
    @Override
    public void execute(Runnable command) {
      delegate.execute(
          () -> {
            long startTime = System.nanoTime();
            inspector
                .append("\nThread ")
                .append(Thread.currentThread().getName())
                .append(" started at ")
                .append(startTime);
            try {
              command.run();
            } finally {
              long endTime = System.nanoTime();
              inspector
                  .append("\nThread ")
                  .append(Thread.currentThread().getName())
                  .append(" finished at ")
                  .append(endTime)
                  .append(" (duration: ")
                  .append(TimeUnit.NANOSECONDS.toMillis(endTime - startTime))
                  .append("ms)");
            }
          });
    }

    /** */
    // Delegate the rest of the ExecutorService methods to the underlying executor
    @Override
    public void shutdown() {
      long startTime = System.nanoTime();
      inspector
          .append("\nThread ")
          .append(Thread.currentThread().getName())
          .append(" Thread shutdown ")
          .append(startTime);
      delegate.shutdown();
    }

    /** @return */
    @Override
    public List<Runnable> shutdownNow() {
      long startTime = System.nanoTime();
      inspector
          .append("\nThread ")
          .append(Thread.currentThread().getName())
          .append(" Thread shutdown ")
          .append(startTime);
      return delegate.shutdownNow();
    }

    /** @return */
    @Override
    public boolean isShutdown() {
      long startTime = System.nanoTime();
      inspector
          .append("\nThread ")
          .append(Thread.currentThread().getName())
          .append(" Thread shutdown ")
          .append(startTime);
      return delegate.isShutdown();
    }

    /** @return */
    @Override
    public boolean isTerminated() {
      long startTime = System.nanoTime();
      inspector
          .append("\nThread ")
          .append(Thread.currentThread().getName())
          .append(" Thread shutdown ")
          .append(startTime);
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
      long startTime = System.nanoTime();
      inspector
          .append("\nThread ")
          .append(Thread.currentThread().getName())
          .append(" Awaiting terminiation ")
          .append(startTime);
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
      long startTime = System.nanoTime();
      inspector
          .append("\nThread ")
          .append(Thread.currentThread().getName())
          .append(" Thread invokeAll at long: ")
          .append(startTime);
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
      long startTime = System.nanoTime();
      inspector
          .append("\nThread ")
          .append(Thread.currentThread().getName())
          .append(" Thread interrupted invokeAll at long: ")
          .append(startTime);
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
      long startTime = System.nanoTime();
      inspector
          .append("\nThread ")
          .append(Thread.currentThread().getName())
          .append(" Thread interrupted invokeAll at long: ")
          .append(startTime);
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
      long startTime = System.nanoTime();
      inspector
          .append("\nThread ")
          .append(Thread.currentThread().getName())
          .append(" Thread interrupted invokeAll at long: ")
          .append(startTime);
      return delegate.invokeAny(tasks, timeout, unit);
    }

    /**
     * @param task the task to submit
     * @return
     */
    @Override
    public java.util.concurrent.Future<?> submit(Runnable task) {
      long startTime = System.nanoTime();
      inspector
          .append("\nThread ")
          .append(Thread.currentThread().getName())
          .append(" Thread submitted ")
          .append(startTime)
          .append(task.toString());
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
}
