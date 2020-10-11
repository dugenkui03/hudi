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

package org.apache.hudi.callback.common;

import java.io.Serializable;

/**
 * Base callback message, which contains commitTime and tableName only for now.
 *
 * 基本的回调消息，饱含提交时间和表名。
 */
public class HoodieWriteCommitCallbackMessage implements Serializable {

  private static final long serialVersionUID = -3033643980627719561L;

  /**
   * CommitTime for one batch write, this is required.
   *
   * 一次批量写的提交时间
   */
  private String commitTime;

  /**
   * Table name this batch commit to.
   *
   * 批量提交的表名称
   */
  private String tableName;

  /**
   * BathPath the table located.
   *
   * 表路径。
   */
  private String basePath;

  public HoodieWriteCommitCallbackMessage() {
  }

  /**
   * 表路径、表名和提交时间
   * @param commitTime
   * @param tableName
   * @param basePath
   */
  public HoodieWriteCommitCallbackMessage(String commitTime,
                                          String tableName,
                                          String basePath) {
    this.commitTime = commitTime;
    this.tableName = tableName;
    this.basePath = basePath;
  }

  public String getCommitTime() {
    return commitTime;
  }

  public void setCommitTime(String commitTime) {
    this.commitTime = commitTime;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public String getBasePath() {
    return basePath;
  }

  public void setBasePath(String basePath) {
    this.basePath = basePath;
  }
}
