﻿// Licensed to the Apache Software Foundation (ASF) under one
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

using System.Globalization;
using System.IO;
using Org.Apache.REEF.Client.Local;
using Org.Apache.REEF.Client.Yarn;
using Org.Apache.REEF.Client.Yarn.RestClient;
using Org.Apache.REEF.IMRU.OnREEF.Client;
using Org.Apache.REEF.Network;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;

namespace Org.Apache.REEF.IMRU.Examples
{
    /// <summary>
    /// Configuration for Runtime for IMRU on REEF.
    /// </summary>
    /// <typeparam name="TMapInput">The type of the side information provided to the Map function</typeparam>
    /// <typeparam name="TMapOutput">The return type of the Map function</typeparam>
    /// <typeparam name="TResult">The return type of the computation.</typeparam>
    internal static class OnREEFIMRURunTimeConfiguration<TMapInput, TMapOutput, TResult>
    {
        /// <summary>
        /// Function that specifies local runtime configuration for IMRU
        /// </summary>
        /// <returns>The local runtime configuration</returns>
        internal static IConfiguration GetLocalIMRUConfiguration(int numNodes, params string[] runTimeDir)
        {
            IConfiguration runtimeConfig;
            IConfiguration imruClientConfig =
                REEFIMRUClientConfiguration.ConfigurationModule.Build();

            if (runTimeDir.Length != 0)
            {
                runtimeConfig = LocalRuntimeClientConfiguration.ConfigurationModule
                    .Set(LocalRuntimeClientConfiguration.NumberOfEvaluators,
                        numNodes.ToString(CultureInfo.InvariantCulture))
                    .Set(LocalRuntimeClientConfiguration.RuntimeFolder, runTimeDir[0])
                    .Build();
            }
            else
            {
                runtimeConfig = LocalRuntimeClientConfiguration.ConfigurationModule
                   .Set(LocalRuntimeClientConfiguration.NumberOfEvaluators,
                       numNodes.ToString(CultureInfo.InvariantCulture))
                   .Build();
            }

            return Configurations.Merge(runtimeConfig, imruClientConfig, GetTcpConfiguration());
        }

        /// <summary>
        /// Function that specifies yarn runtime configuration for IMRU on the cluster
        /// </summary>
        /// <returns>The yarn runtime configuration</returns>
        internal static IConfiguration GetYarnIMRUConfiguration()
        { 
            IConfiguration imruClientConfig =
                REEFIMRUClientConfiguration.ConfigurationModule.Build();

            var runtimeConfig = YARNClientConfiguration.ConfigurationModule
                .Set(YARNClientConfiguration.JobSubmissionFolderPrefix, @"/vol1/tmp")
                .Set(YARNClientConfiguration.SecurityTokenKind, "TrustedApplicationTokenIdentifier")
                .Set(YARNClientConfiguration.SecurityTokenService, "TrustedApplicationTokenIdentifier")
                .Build();

            string token = "TrustedApplication007";
            File.WriteAllText("SecurityTokenId", token);
            File.WriteAllText("SecurityTokenPwd", "none");

            var yarnProviderConfig = TangFactory.GetTang().NewConfigurationBuilder()
                .BindImplementation(GenericType<IUrlProvider>.Class, GenericType<YarnConfigurationUrlProvider>.Class)
                .Build();

            return Configurations.Merge(runtimeConfig, imruClientConfig, GetTcpConfiguration(), yarnProviderConfig);
        }

        private static IConfiguration GetTcpConfiguration()
        {
            return TcpClientConfigurationModule.ConfigurationModule
                .Set(TcpClientConfigurationModule.MaxConnectionRetry, "200")
                .Set(TcpClientConfigurationModule.SleepTime, "1000")
                .Build();
        }
    }
}