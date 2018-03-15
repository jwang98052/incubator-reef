// Licensed to the Apache Software Foundation(ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
// 
//   http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.IMRU.OnREEF.Parameters
{
    /// <summary>
    /// After a task is submitted, if it hung or too slow and keeps in Submitted state without running,
    /// after the driver receives the first failed task and waited for SubmittedTaskTimeoutMs,
    /// the driver will kill the evaluator. The default is 10min
    /// </summary>
    [NamedParameter("Timeout for submitted task in milliseconds", "SubmittedTaskTimeout", "600000")]
    public sealed class SubmittedTaskTimeoutMs : Name<int>
    {
    }
}
