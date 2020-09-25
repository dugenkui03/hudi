/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.callback.util;

import org.apache.hudi.callback.HoodieWriteCommitCallback;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieCommitCallbackException;

import static org.apache.hudi.config.HoodieWriteCommitCallbackConfig.CALLBACK_CLASS_PROP;

/**
 * Factory, help to create {@link HoodieWriteCommitCallback}.
 *
 * 协助创建 HoodieWriteCommitCallback 类
 */
public class HoodieCommitCallbackFactory {
  public static HoodieWriteCommitCallback create(HoodieWriteConfig config) {
      String callbackClass = config.getCallbackClass();
      // todo 不用 isNotBlack() 嘛
      if (!StringUtils.isNullOrEmpty(callbackClass)) {
          // 创建一个 callbackClass 对象
          Object instance = ReflectionUtils.loadClass(callbackClass, config);

          // 如果不是 HoodieWriteCommitCallback 实现类
          if (!(instance instanceof HoodieWriteCommitCallback)) {
            // todo 为啥还反射 HoodieWriteCommitCallback.class.getSimpleName()
            throw new HoodieCommitCallbackException(callbackClass + " is not a subclass of " + HoodieWriteCommitCallback.class.getSimpleName());
          }

          // 返回创建的 HoodieWriteCommitCallback 实例
          return (HoodieWriteCommitCallback) instance;
      } else {
          throw new HoodieCommitCallbackException(
                  String.format("The value of the config option %s can not be null or empty" , CALLBACK_CLASS_PROP)
          );
      }
  }

}
