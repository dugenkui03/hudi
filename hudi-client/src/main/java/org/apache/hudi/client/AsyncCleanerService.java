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

package org.apache.hudi.client;

import org.apache.hudi.async.AbstractAsyncService;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Clean service running concurrently with write operation.
 *
 * fixme 清除操作、和写操作并发执行。
 */
class AsyncCleanerService extends AbstractAsyncService {

  // 静态日志
  private static final Logger LOG = LogManager.getLogger(AsyncCleanerService.class);

  // 写操作客户端
  private final HoodieWriteClient<?> writeClient;

  // ？清除实例时间？
  private final String cleanInstantTime;

  // (new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>()))
  private final transient ExecutorService executor = Executors.newSingleThreadExecutor();


  /**
   * @param writeClient 写客户端
   * @param cleanInstantTime  清除 瞬时 时间
   */
  protected AsyncCleanerService(HoodieWriteClient<?> writeClient,
                                String cleanInstantTime) {
      this.writeClient = writeClient;
      this.cleanInstantTime = cleanInstantTime;
  }


  @Override
  protected Pair<CompletableFuture, ExecutorService> startService() {
      return Pair.of(CompletableFuture.supplyAsync(() -> {
        writeClient.clean(cleanInstantTime);
        return true;
      }), executor);
  }

  public static AsyncCleanerService startAsyncCleaningIfEnabled(HoodieWriteClient writeClient,
                                                                String instantTime) {
    AsyncCleanerService asyncCleanerService = null;
    if (writeClient.getConfig().isAutoClean() && writeClient.getConfig().isAsyncClean()) {
      LOG.info("Auto cleaning is enabled. Running cleaner async to write operation");
      asyncCleanerService = new AsyncCleanerService(writeClient, instantTime);
      asyncCleanerService.start(null);
    } else {
      LOG.info("Auto cleaning is not enabled. Not running cleaner now");
    }
    return asyncCleanerService;
  }

  public static void waitForCompletion(AsyncCleanerService asyncCleanerService) {
    if (asyncCleanerService != null) {
      LOG.info("Waiting for async cleaner to finish");
      try {
        asyncCleanerService.waitForShutdown();
      } catch (Exception e) {
        throw new HoodieException("Error waiting for async cleaning to finish", e);
      }
    }
  }

  public static void forceShutdown(AsyncCleanerService asyncCleanerService) {
    if (asyncCleanerService != null) {
      LOG.info("Shutting down async cleaner");
      asyncCleanerService.shutdown(true);
    }
  }
}
