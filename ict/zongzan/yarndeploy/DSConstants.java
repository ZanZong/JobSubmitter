/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ict.zongzan.yarndeploy;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Constants used in both Client and Application Master
 * Created by Zongzan on 2016/11/4.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class DSConstants {

  /**
   * Environment key name denoting the timeline domain ID.
   */
    public static final String JOBSUBMITTERDOMAIN = "JOBSUBMITTERDOMAIN";

    public static final String TASKNUM = "TASKNUM";
    public static final String TASKIDSTRING = "TASKIDSTRING";

    //分号当字符串分隔符
    public static final String SPLIT = ";";

    //Task类的成员变量名称，用来解析Json
    public static final String RESOURCEREQUESTS = "resourceRequests";
    public static final String TASKID = "taskId";
    public static final String JOBID = "jobId";
    public static final String JARPATH = "jarPath";
    public static final String TASKJARLEN = "taskJarLen";
    public static final String TASKJARTIMESTAMP = "taskJarTimestamp";
    public static final String TASKJARLOCATIOIN = "taskJarLocation";
    public static final String NEXTTASK = "nextTask";
    public static final String PRIORITY = "priority";

    //Resource类的成员变量
    public static final String CORES = "cores";
    public static final String RAM = "RAM";
    public static final String LOCALDISKSPACE = "localDiskSpace";
    public static final String SCPS = "scps";
}
