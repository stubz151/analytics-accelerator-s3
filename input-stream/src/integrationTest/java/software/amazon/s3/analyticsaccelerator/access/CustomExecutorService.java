package software.amazon.s3.analyticsaccelerator.access;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;


public class CustomExecutorService {
  public static ExecutorService createCustomExecutorService(int threadCount) {
    AtomicLong threadCounter = new AtomicLong(0);

    ThreadFactory threadFactory = runnable -> {
      Thread thread = new Thread(runnable);
      thread.setName("custom-executor-thread-" + threadCounter.incrementAndGet());
      return thread;
    };

    ExecutorService executor = Executors.newFixedThreadPool(threadCount, threadFactory);

    return new LoggingExecutorService(executor);
  }

  private static class LoggingExecutorService implements ExecutorService {
    private final ExecutorService delegate;

    public LoggingExecutorService(ExecutorService delegate) {
      this.delegate = delegate;
    }

    @Override
    public void execute(Runnable command) {
      delegate.execute(() -> {
        long startTime = System.nanoTime();
        System.out.println("Thread " + Thread.currentThread().getName() + " started at " + startTime);
        try {
          command.run();
        } finally {
          long endTime = System.nanoTime();
          System.out.println("Thread " + Thread.currentThread().getName() + " finished at " + endTime + " (duration: " + TimeUnit.NANOSECONDS.toMillis(endTime - startTime) + "ms)");
        }
      });
    }

    // Delegate the rest of the ExecutorService methods to the underlying executor
    @Override
    public void shutdown() {
      delegate.shutdown();
    }

    @Override
    public List<Runnable> shutdownNow() {
      return delegate.shutdownNow();
    }

    @Override
    public boolean isShutdown() {
      return delegate.isShutdown();
    }

    @Override
    public boolean isTerminated() {
      return delegate.isTerminated();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
      return delegate.awaitTermination(timeout, unit);
    }

    @Override
    public <T> java.util.List<java.util.concurrent.Future<T>> invokeAll(java.util.Collection<? extends java.util.concurrent.Callable<T>> tasks) throws InterruptedException {
      return delegate.invokeAll(tasks);
    }

    @Override
    public <T> java.util.List<java.util.concurrent.Future<T>> invokeAll(java.util.Collection<? extends java.util.concurrent.Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException {
      return delegate.invokeAll(tasks, timeout, unit);
    }

    @Override
    public <T> T invokeAny(java.util.Collection<? extends java.util.concurrent.Callable<T>> tasks) throws InterruptedException, java.util.concurrent.ExecutionException {
      return delegate.invokeAny(tasks);
    }

    @Override
    public <T> T invokeAny(java.util.Collection<? extends java.util.concurrent.Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException, java.util.concurrent.ExecutionException, java.util.concurrent.TimeoutException {
      return delegate.invokeAny(tasks, timeout, unit);
    }

    @Override
    public java.util.concurrent.Future<?> submit(Runnable task) {
      return delegate.submit(task);
    }

    @Override
    public <T> java.util.concurrent.Future<T> submit(Runnable task, T result) {
      return delegate.submit(task, result);
    }

    @Override
    public <T> java.util.concurrent.Future<T> submit(java.util.concurrent.Callable<T> task) {
      return delegate.submit(task);
    }
  }

  public static void main(String[] args) {
    ExecutorService executor = createCustomExecutorService(4);

    for (int i = 0; i < 10; i++) {
      executor.execute(() -> {
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
