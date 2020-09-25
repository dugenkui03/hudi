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

package org.apache.hudi.metrics;

import org.apache.hudi.common.metrics.Registry;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;

import com.codahale.metrics.MetricRegistry;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.Closeable;
import java.util.Map;

/**
 * This is the main class of the metrics system.
 *
 * 指标系统的核心类。
 */
public class Metrics {

  // 日志类
  private static final Logger LOG = LogManager.getLogger(Metrics.class);

  // 是否初始化了
  private static volatile boolean initialized = false;

  // 自己
  private static Metrics metrics = null;
  private final MetricRegistry registry;
  private MetricsReporter reporter;

  private Metrics(HoodieWriteConfig metricConfig) {
    registry = new MetricRegistry();

    reporter = MetricsReporterFactory.createReporter(metricConfig, registry);
    if (reporter == null) {
      throw new RuntimeException("Cannot initialize Reporter.");
    }
    reporter.start();

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        registerHoodieCommonMetrics();
        reporter.report();
        if (getReporter() != null) {
          getReporter().close();
        }
      } catch (Exception e) {
        LOG.warn("Error while closing reporter", e);
      }
    }));
  }

  private void registerHoodieCommonMetrics() {
    registerGauges(Registry.getAllMetrics(true, true), Option.empty());
  }

  public static Metrics getInstance() {
    assert initialized;
    return metrics;
  }

  // 静态、加锁，初始化指标系统
  public static synchronized void init(HoodieWriteConfig metricConfig) {
    // 如果已经初始化了，返回
    if (initialized) {
      return;
    }

    // 初始化单利模式
    try {
      metrics = new Metrics(metricConfig);
    } catch (Exception e) {
      throw new HoodieException(e);
    }

    // 修改初始化标识
    initialized = true;
  }

  public static void registerGauges(Map<String, Long> metricsMap, Option<String> prefix) {
    // 不为空时添加后缀 .
    String metricPrefix = prefix.isPresent() ? prefix.get() + "." : "";
    metricsMap.forEach((k, v) -> registerGauge(metricPrefix + k, v));
  }

  public static void registerGauge(String metricName, final long value) {
    try {
      MetricRegistry registry = Metrics.getInstance().getRegistry();
      HoodieGauge guage = (HoodieGauge) registry.gauge(metricName, () -> new HoodieGauge<>(value));
      guage.setValue(value);
    } catch (Exception e) {
      // todo 格式
      // Here we catch all exception, so the major upsert pipeline will not be affected if the
      // metrics system has some issues.
      // todo 是否是高频操作
      LOG.error("Failed to send metrics: ", e);
    }
  }

  public MetricRegistry getRegistry() {
    return registry;
  }

  public Closeable getReporter() {
    return reporter.getReporter();
  }
}
