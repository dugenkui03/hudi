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

package org.apache.hudi.common.util;

import java.io.Serializable;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Provides functionality same as java.util.Optional but is also made Serializable.
 * Additional APIs are provided to convert to/from java.util.Optional.
 *
 * 提供类似Optional的功能，但是数据可序列化；并且该类提供了api用于在该类和Optional之间的转换；
 */
public final class Option<T> implements Serializable {

  private static final long serialVersionUID = 0L;

  // 空值
  private static final Option<?> NULL_VAL = new Option<>();

  private final T val;

  /**
   * Convert to java Optional.
   * 转换为 java 的 Optional。
   */
  public Optional<T> toJavaOptional() {
    return Optional.ofNullable(val);
  }

  /**
   * Convert from java.util.Optional.
   * 从 java 的 Optional 转换而来。
   * 
   * @param v java.util.Optional object
   * @param <T> type of the value stored in java.util.Optional object
   * @return Option
   */
  public static <T> Option<T> fromJavaOptional(Optional<T> v) {
    return Option.ofNullable(v.orElse(null));
  }

  private Option() {
    this.val = null;
  }

  private Option(T val) {
    if (null == val) {
      throw new NullPointerException("Expected a non-null value. Got null");
    }
    this.val = val;
  }

  public static <T> Option<T> empty() {
    return (Option<T>) NULL_VAL;
  }

  public static <T> Option<T> of(T value) {
    return new Option<>(value);
  }

  public static <T> Option<T> ofNullable(T value) {
    return null == value ? empty() : of(value);
  }

  public boolean isPresent() {
    return null != val;
  }

  public T get() {
    if (null == val) {
      throw new NoSuchElementException("No value present in Option");
    }
    return val;
  }

  public void ifPresent(Consumer<? super T> consumer) {
    if (val != null) {
      consumer.accept(val);
    }
  }

  public <U> Option<U> map(Function<? super T, ? extends U> mapper) {
    // todo  Objects.requireNonNull(mapper);
    // 区别是，即使 !isPresent() 也可以检查出mapper为空
    if (!isPresent()) {
      return empty();
    } else {
      return Option.ofNullable(mapper.apply(val));
    }
  }

  public T orElse(T other) {
    return val != null ? val : other;
  }

  public T orElseGet(Supplier<? extends T> other) {
    return val != null ? val : other.get();
  }

  public <X extends Throwable> T orElseThrow(Supplier<? extends X> exceptionSupplier) throws X {
    if (val != null) {
      return val;
    } else {
      throw exceptionSupplier.get();
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Option<?> option = (Option<?>) o;
    return Objects.equals(val, option.val);
  }

  @Override
  public int hashCode() {
    return Objects.hash(val);
  }

  @Override
  public String toString() {
    // todo 需要改一下，对以下两种情况做区分
    //  System.out.println(Option.of("null").toString());
    //  System.out.println(Option.empty().toString());
    //  return value != null ? String.format("Optional[%s]", value) : "Optional.empty";
    return "Option{val=" + val + '}';
  }
}
