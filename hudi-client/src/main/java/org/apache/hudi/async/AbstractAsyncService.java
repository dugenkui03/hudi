/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.async;

import org.apache.hudi.common.util.collection.Pair;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Base Class for running clean/delta-sync/compaction in separate thread and controlling their life-cycle.
 *
 * 执行清除、"异步增量"和压缩的基本类：在不同的线程执行，并且控制他们的生命周期。
 */
public abstract class AbstractAsyncService implements Serializable {

  // 静态日志
  private static final Logger LOG = LogManager.getLogger(AbstractAsyncService.class);

  // Flag to track if the service is started.
  // 服务是否启动，todo volatile
  private boolean started;

  // Flag indicating shutdown is externally requested
  // 是否请求关闭，todo volatile
  private boolean shutdownRequested;

  // Flag indicating the service is shutdown
  // 服务是否已经关闭
  private volatile boolean shutdown;

  // Executor Service for running delta-sync/compaction
  // 执行异步增量和压缩的线程池
  private transient ExecutorService executor;

  // Future tracking delta-sync/compaction
  // 追踪 增量和压缩的 Future
  private transient CompletableFuture future;

  // Run in daemon mode
  // 是否异步的方式执行
  private final boolean runInDaemonMode;

  protected AbstractAsyncService() {
    this(false);
  }

  protected AbstractAsyncService(boolean runInDaemonMode) {
    // todo 这个是啥鬼
    shutdownRequested = false;
    this.runInDaemonMode = runInDaemonMode;
  }

  // 是否请求关闭
  protected boolean isShutdownRequested() {
    return shutdownRequested;
  }

  // 服务是否已经关闭
  protected boolean isShutdown() {
    return shutdown;
  }

  /**
   * Wait till the service shutdown. If the service shutdown with exception, it will be thrown
   * 
   * @throws ExecutionException
   * @throws InterruptedException
   */
  public void waitForShutdown() throws ExecutionException, InterruptedException {
    try {
      future.get();
    } catch (ExecutionException ex) {
      // 服务关闭的时候如果出现执行异常，先打印异常、在抛出去。中断异常则不作处理
      LOG.error("Service shutdown with error", ex);
      throw ex;
    }
  }

  /**
   * Request shutdown either forcefully or gracefully.
   * Graceful shutdown allows the service to finish up the current round of work and shutdown.
   * For graceful shutdown, it waits till the service is shutdown
   * 
   * @param force Forcefully shutdown
   *              fixme 强制、或者优雅的关闭线程池
   */
  public void shutdown(boolean force) {
    // 如果没有请求过关闭 或者 是强制关闭为真
    // 才会执行关闭逻辑
    if (!shutdownRequested || force) {
      // 标识为 "请求过关闭"
      shutdownRequested = true;
      // todo 为啥会有null的情况
      if (executor != null) {
        // 如果是强制关闭，则 shutdownNow
        if (force) {
          executor.shutdownNow();
        }
        // 不是强制关闭，shutdown
        else {
          executor.shutdown();
          try {
            // Wait for some max time after requesting shutdown
            // todo 这个时间默认24小时，应该可以指定时间
            executor.awaitTermination(24, TimeUnit.HOURS);
          } catch (InterruptedException ie) {
            LOG.error("Interrupted while waiting for shutdown", ie);
          }
        }
      }
    }
  }

  /**
   * Start the service.
   * Runs the service in a different thread and returns.
   * Also starts a monitor thread to run-callbacks in case of shutdown
   * 
   * @param onShutdownCallback
   */
  public void start(Function<Boolean, Boolean> onShutdownCallback) {
    // ？启动服务？
    Pair<CompletableFuture, ExecutorService> res = startService();
    future = res.getKey();
    executor = res.getValue();
    started = true;
    monitorThreads(onShutdownCallback);
  }

  /**
   * Service implementation. fixme ？具体的启动服务的行为？？
   */
  protected abstract Pair<CompletableFuture, ExecutorService> startService();

  /**
   * A monitor thread is started which would trigger a callback if the service is shutdown.
   *
   * 启动监视器线程，在服务关闭的时候执行回调。
   */
  private void monitorThreads(Function<Boolean, Boolean> onShutdownCallback) {

    // 创建线程工厂：是否是守护守护进程、线程名称
    ThreadFactory factory = runnable -> {
      // todo 线程名称
      Thread t = new Thread(runnable, "Monitor Thread");
      t.setDaemon(isRunInDaemonMode());
      return t;
    };

    Executors.newSingleThreadExecutor(factory).submit(() -> {
      boolean error = false;
      try {
        future.get();
      } catch (ExecutionException ex) {
        LOG.error("Monitor noticed one or more threads failed. Requesting graceful shutdown of other threads", ex);
        error = true;
      } catch (InterruptedException ie) {
        LOG.error("Got interrupted Monitoring threads", ie);
        error = true;
      } finally {
        /**
         * 最后将服务标识为 关闭；
         * 如果回调方法不为null、则执行回调方法，参数为 future.get() 是否发生了异常；
         * 非强制关闭；
         */
        shutdown = true;
        if (null != onShutdownCallback) {
          onShutdownCallback.apply(error);
        }
        shutdown(false);
      }
    });
  }

  public boolean isRunInDaemonMode() {
    return runInDaemonMode;
  }
}
