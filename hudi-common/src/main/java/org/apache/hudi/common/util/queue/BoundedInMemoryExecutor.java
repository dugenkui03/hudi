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

package org.apache.hudi.common.util.queue;

import org.apache.hudi.common.util.DefaultSizeEstimator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.SizeEstimator;
import org.apache.hudi.exception.HoodieException;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Executor which orchestrates concurrent producers and consumers communicating through a bounded in-memory queue.
 * This class takes as input the size limit, queue producer(s), consumer and transformer and exposes API to orchestrate
 * concurrent execution of these actors communicating through a central bounded queue
 */
public class BoundedInMemoryExecutor<I, O, E> {


  /**
   * fixme 执行线程池、任务队列、生产者、消费者
   */

  // Executor service used for launching writer thread.
  private final ExecutorService executorService;
  // Used for buffering records which is controlled by HoodieWriteConfig#WRITE_BUFFER_LIMIT_BYTES.
  private final BoundedInMemoryQueue<I, O> queue;
  // Producers
  private final List<BoundedInMemoryQueueProducer<I>> producers;
  // Consumer
  private final Option<BoundedInMemoryQueueConsumer<O, E>> consumer;

  public BoundedInMemoryExecutor(long bufferLimitInBytes,
                                 BoundedInMemoryQueueProducer<I> producer,
                                 Option<BoundedInMemoryQueueConsumer<O, E>> consumer,
                                 Function<I, O> transformFunction) {
      this(bufferLimitInBytes, Arrays.asList(producer), consumer, transformFunction, new DefaultSizeEstimator<>());
  }

  public BoundedInMemoryExecutor(long bufferLimitInBytes,
                                 List<BoundedInMemoryQueueProducer<I>> producers,
                                 Option<BoundedInMemoryQueueConsumer<O, E>> consumer,
                                 Function<I, O> transformFunction,
                                 SizeEstimator<O> sizeEstimator) {
        this.producers = producers;
        this.consumer = consumer;
        // Ensure single thread for each producer thread and one for consumer
        this.executorService = Executors.newFixedThreadPool(producers.size() + 1);
        this.queue = new BoundedInMemoryQueue<>(bufferLimitInBytes, transformFunction, sizeEstimator);
  }

  /**
   * Callback to implement environment specific behavior before executors (producers/consumer) run.
   */
  public void preExecute() {
    // Do Nothing in general context
  }

  /**
   * Start all Producers.
   *
   * 启动生产者。
   */
  public ExecutorCompletionService<Boolean> startProducers() {
    // Latch to control when and which producer thread will close the queue
    // 控制 哪些消费者、什么时候关闭 的倒计时锁
    final CountDownLatch latch = new CountDownLatch(producers.size());

    // todo 多线程池的包装
    final ExecutorCompletionService<Boolean> completionService = new ExecutorCompletionService(executorService);

    // 遍历 生产者
    // todo 什么逻辑
    producers.stream().map(producer ->
            completionService.submit(
                    () -> {
                        try {
                            // 执行的前置动作
                            preExecute();
                            producer.produce(queue);
                        } catch (Exception e) {
                            // 标记错误
                            queue.markAsFailed(e);
                            // 抛异常
                            throw e;
                        } finally {
                            synchronized (latch) {
                                latch.countDown();
                                if (latch.getCount() == 0) {
                                  // Mark production as done so that consumer will be able to exit
                                  queue.close();
                                }
                            }
                        }
                        return true;
            })
            // todo 不用收集吧？
    ).collect(Collectors.toList());

    return completionService;
  }


  // Start only consumer.
  // 启动一个消费者
  private Future<E> startConsumer() {
      return consumer.map(consumer ->
              // fixme 提交一个任务
              executorService.submit(() -> {
                  // starting consumer thread
                  preExecute();
                  try {
                      // Queue Consumption is done; notifying producer threads
                      return consumer.consume(queue);
                  } catch (Exception e) {
                      queue.markAsFailed(e);
                      throw e;
                  }
              })
      ).orElse(CompletableFuture.completedFuture(null));
  }

  // Main API to run both production and consumption.
  // fixme 主要的api，运行生产者和消费者
  public E execute() {
    try {
      // 启动生产者
      startProducers();

      // 启动消费者
      Future<E> future = startConsumer();

      // Wait for consumer to be done
      // 等待消费者操作结束
      return future.get();
    } catch (Exception e) {
      throw new HoodieException(e);
    }
  }

  public boolean isRemaining() {
    return queue.iterator().hasNext();
  }

  public void shutdownNow() {
    executorService.shutdownNow();
  }

  public BoundedInMemoryQueue<I, O> getQueue() {
    return queue;
  }
}
