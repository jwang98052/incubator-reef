// Licensed to the Apache Software Foundation (ASF) under one
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

using System;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading;
using Newtonsoft.Json;
using Org.Apache.REEF.Client.API;
using Org.Apache.REEF.Client.Common;
using Org.Apache.REEF.Client.Local;
using Org.Apache.REEF.Client.Yarn;
using Org.Apache.REEF.Client.Yarn.RestClient;
using Org.Apache.REEF.Client.YARN.HDI;
using Org.Apache.REEF.Client.YARN.RestClient.DataModel;
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.IO.FileSystem.AzureBlob;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Utilities.Runtime.Yarn;

namespace Org.Apache.REEF.Examples.HelloREEF
{
    /// <summary>
    /// A Tool that submits HelloREEFDriver for execution.
    /// </summary>
    public sealed class HelloREEF
    {
        private const string Local = "local";
        private const string YARN = "yarn";
        private const string YARNRest = "yarnrest";
        private const string HDInsight = "hdi";
        private readonly IREEFClient _reefClient;
        private readonly JobRequestBuilder _jobRequestBuilder;
        private const string StateRequestUrlTemplate = "{0}ws/v1/cluster/apps/{1}/state";

        private static readonly Logger _Logger = Logger.GetLogger(typeof(HelloREEF));

        [Inject]
        private HelloREEF(IREEFClient reefClient, JobRequestBuilder jobRequestBuilder)
        {
            _reefClient = reefClient;
            _jobRequestBuilder = jobRequestBuilder;
        }

        /// <summary>
        /// Runs HelloREEF using the IREEFClient passed into the constructor.
        /// </summary>
        private void Run()
        {
            // The driver configuration contains all the needed bindings.
            var helloDriverConfiguration = DriverConfiguration.ConfigurationModule
                .Set(DriverConfiguration.OnEvaluatorAllocated, GenericType<HelloDriver>.Class)
                .Set(DriverConfiguration.OnDriverStarted, GenericType<HelloDriver>.Class)
                .Build();

            // The JobSubmission contains the Driver configuration as well as the files needed on the Driver.
            var helloJobRequest = _jobRequestBuilder
                .AddDriverConfiguration(helloDriverConfiguration)
                .AddGlobalAssemblyForType(typeof(HelloDriver))
                .SetJavaLogLevel(JavaLoggingSetting.Verbose)
                .SetJobIdentifier("HelloREEF")
                .Build();

            var r = _reefClient.SubmitAndGetJobStatus(helloJobRequest);

            _Logger.Log(Level.Info, "Application ID : {0}, DriverUrl : {1}.", r.AppId, r.DriverUrl);
            ////var rmUrls = YarnConfiguration.GetConfiguration().GetYarnRMWebappEndpoints().Select(uri => uri.ToString());
            ////foreach (var url in rmUrls)
            ////{
            ////    _Logger.Log(Level.Info, "Yarn RM Web point : {0}.", url);
            ////    var s = GetParentAppStatus(url, r.AppId);
            ////    _Logger.Log(Level.Info, "current App status : {0}.", s);
            ////}

            ////var finalSate = PullFinalJobStatus(r);

            ////foreach (var url in rmUrls)
            ////{
            ////    var s = GetParentAppStatus(url, r.AppId);
            ////    _Logger.Log(Level.Info, "Final App status : {0}.", s);
            ////}

            ////if (finalSate != FinalState.SUCCEEDED)
            ////{
            ////    throw new ApplicationException("Hello REEF final state is :" + finalSate.ToString());
            ////}
        }

        private string GetParentAppStatus(string rmUrl, string parentApplicationId)
        {
            var url = GetApplicationStateUrl(rmUrl, parentApplicationId);

            _Logger.Log(Level.Info, "Attempting to check status of parent Application ID: {0} using URL: {1}",
                parentApplicationId, url);

            var rmClient = new WebClient();
            var json = rmClient.DownloadString(url);
            _Logger.Log(Level.Info, "Check for Application ID: {0}. downloaded json: {1}",
                parentApplicationId, json);

            dynamic stateObject = JsonConvert.DeserializeObject(json);
            string appStatus = stateObject == null ? null : stateObject.state.ToString();
            return appStatus;
        }

        private string GetApplicationStateUrl(string rmUrl, string appid)
        {
            return string.Format(StateRequestUrlTemplate, rmUrl, appid);
        }

        private FinalState PullFinalJobStatus(IJobSubmissionResult jobSubmitionResult)
        {
            int n = 0;
            var state = jobSubmitionResult.FinalState;
            while (state.Equals(FinalState.UNDEFINED) && n++ < 100)
            {
                Thread.Sleep(2000);
                state = jobSubmitionResult.FinalState;
            }

            _Logger.Log(Level.Info, "Application state : {0}.", state);

            return state;
        }

        /// <summary>
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        private static IConfiguration GetRuntimeConfiguration(string name)
        {
            switch (name)
            {
                case Local:
                    return LocalRuntimeClientConfiguration.ConfigurationModule
                        .Set(LocalRuntimeClientConfiguration.NumberOfEvaluators, "2")
                        .Build();
                case YARN:
                    var c = YARNClientConfiguration.ConfigurationModule
                       .Set(YARNClientConfiguration.SecurityTokenKind, "TrustedApplicationTokenIdentifier")
                       .Set(YARNClientConfiguration.SecurityTokenService, "TrustedApplicationTokenIdentifier")
                       .Build();

                    string token = "TrustedApplication007";
                    File.WriteAllText("SecurityTokenId", token);
                    File.WriteAllText("SecurityTokenPwd", "none");

                    IConfiguration tcpPortConfig = TcpPortConfigurationModule.ConfigurationModule
                        .Set(TcpPortConfigurationModule.PortRangeStart, "2000")
                        .Set(TcpPortConfigurationModule.PortRangeCount, "20")
                        .Build();

                    var c2 = TangFactory.GetTang().NewConfigurationBuilder()
                        .BindImplementation(GenericType<IUrlProvider>.Class, GenericType<YarnConfigurationUrlProvider>.Class)
                        .Build();

                    return Configurations.Merge(c, tcpPortConfig, c2);
                case YARNRest:
                    return YARNClientConfiguration.ConfigurationModuleYARNRest.Build();
                case HDInsight:
                    // To run against HDInsight please replace placeholders below, with actual values for
                    // connection string, container name (available at Azure portal) and HDInsight 
                    // credentials (username and password)
                    const string connectionString = "ConnString";
                    const string continerName = "foo";
                    return HDInsightClientConfiguration.ConfigurationModule
                        .Set(HDInsightClientConfiguration.HDInsightPasswordParameter, @"pwd")
                        .Set(HDInsightClientConfiguration.HDInsightUsernameParameter, @"foo")
                        .Set(HDInsightClientConfiguration.HDInsightUrlParameter, @"https://foo.azurehdinsight.net/")
                        .Set(HDInsightClientConfiguration.JobSubmissionDirectoryPrefix, string.Format(@"/{0}/tmp", continerName))
                        .Set(AzureBlockBlobFileSystemConfiguration.ConnectionString, connectionString)
                        .Build();
                default:
                    throw new Exception("Unknown runtime: " + name);
            }
        }

        public static void Main(string[] args)
        {
            TangFactory.GetTang().NewInjector(GetRuntimeConfiguration(args.Length > 0 ? args[0] : Local)).GetInstance<HelloREEF>().Run();
        }
    }
}