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

package org.apache.hudi.common.metrics;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Lightweight Counter for Hudi Metrics.
 *
 * hudi指标的轻量级计数器。
 */
public class Counter implements Metric {

  private final AtomicLong count = new AtomicLong();

  public void increment() {
    this.add(1);
  }

  public void add(long n) {
    this.count.addAndGet(n);
  }

  @Override
  public Long getValue() {
    return count.get();
  }

}