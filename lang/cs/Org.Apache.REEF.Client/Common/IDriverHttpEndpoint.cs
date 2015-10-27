/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

using Org.Apache.REEF.Client.API;
using Org.Apache.REEF.Common.Attributes;

namespace Org.Apache.REEF.Client.Common
{
    [Unstable("0.13", "Working in progress for what to return after submit")]
    public interface IDriverHttpEndpoint
    {
        /// <summary>
        /// Get Uri result
        /// </summary>
        /// <param name="url"></param>
        /// <returns></returns>
        string GetUrlResult(string url);

        /// <summary>
        /// Get Driver Uri
        /// </summary>
        string DriverUrl { get; }

        /// <summary>
        /// Get application id for submitted job
        /// </summary>
        /// <param name="filePath"></param>
        /// <returns></returns>
        string GetAppId(string filePath);

        /// <summary>
        /// Allow to set and get Application state
        /// </summary>
        ApplicationState AppStatus { get; set; }
    }
}
