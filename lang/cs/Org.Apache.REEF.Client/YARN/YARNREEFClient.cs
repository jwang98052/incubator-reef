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

using System;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Org.Apache.REEF.Client.API;
using Org.Apache.REEF.Client.Common;
using Org.Apache.REEF.Client.Yarn.RestClient;
using Org.Apache.REEF.Client.YARN;
using Org.Apache.REEF.Client.YARN.Parameters;
using Org.Apache.REEF.Client.YARN.RestClient.DataModel;
using Org.Apache.REEF.Common.Attributes;
using Org.Apache.REEF.Common.Files;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Client.Yarn
{
    internal sealed class YarnREEFClient : IREEFClient
    {
        /// <summary>
        /// The class name that contains the Java counterpart for this client.
        /// </summary>
        private const string JavaClassName = "org.apache.reef.bridge.client.YarnJobSubmissionClient";

        private static readonly Logger Logger = Logger.GetLogger(typeof(YarnREEFClient));
        private readonly DriverFolderPreparationHelper _driverFolderPreparationHelper;
        private readonly JavaClientLauncher _javaClientLauncher;
        private readonly string _securityTokenKind;
        private readonly string _securityTokenService;
        private readonly string _jobSubmissionPrefix;
        private string _driverUrl;
        private readonly REEFFileNames _fileNames;
        private readonly YarnClient _yarnClient;
        private Application _application;
        private string _pointerFileName;
        private string _applicationId;
        private readonly HttpClientHelper _httpClientHelper;

        [Inject]
        internal YarnREEFClient(JavaClientLauncher javaClientLauncher,
            DriverFolderPreparationHelper driverFolderPreparationHelper,
            REEFFileNames fileNames,
            YarnCommandLineEnvironment yarn,
            YarnClient yarnClient,
            [Parameter(typeof(SecurityTokenKindParameter))] string securityTokenKind,
            [Parameter(typeof(SecurityTokenServiceParameter))] string securityTokenService,
            [Parameter(typeof(JobSubmissionDirectoryPrefixParameter))] string jobSubmissionPrefix)
        {
            _jobSubmissionPrefix = jobSubmissionPrefix;
            _securityTokenKind = securityTokenKind;
            _securityTokenService = securityTokenService;
            _javaClientLauncher = javaClientLauncher;
            _javaClientLauncher.AddToClassPath(yarn.GetYarnClasspathList());
            _driverFolderPreparationHelper = driverFolderPreparationHelper;
            _fileNames = fileNames;
            _yarnClient = yarnClient;
            _httpClientHelper = new HttpClientHelper();
        }

        public void Submit(IJobSubmission jobSubmission)
        {
            // Prepare the job submission folder
            var driverFolderPath = CreateDriverFolder(jobSubmission.JobIdentifier);
            Logger.Log(Level.Info, "Preparing driver folder in " + driverFolderPath);

            Launch(jobSubmission, driverFolderPath);
        }

        public IDriverHttpEndpoint SubmitAndGetDriverUrl(IJobSubmission jobSubmission)
        {
            // Prepare the job submission folder
            var driverFolderPath = CreateDriverFolder(jobSubmission.JobIdentifier);
            Logger.Log(Level.Info, "Preparing driver folder in " + driverFolderPath);

            Launch(jobSubmission, driverFolderPath);

            _pointerFileName = Path.Combine(driverFolderPath, _fileNames.DriverHttpEndpoint);
            _driverUrl = _httpClientHelper.GetDriverUrlForYarn(_pointerFileName);

            return _httpClientHelper;
        }

        /// <summary>
        /// Pull Job status from Yarn
        /// </summary>
        /// <returns></returns>
        public async Task<ApplicationState> GetJobStatus()
        {
            _applicationId = _httpClientHelper.GetAppId(_pointerFileName);
            _application = await _yarnClient.GetApplicationAsync(_applicationId);

            Logger.Log(Level.Info, string.Format("_application status {0}, Progress: {1}, trackingUri: {2}, Name: {3}.  ", 
                _application.FinalStatus, _application.Progress, _application.TrackingUI, _application.Name));

            ApplicationState finalState;
            if (Enum.TryParse(_application.FinalStatus, true, out finalState))
            {
                return finalState;  
            }

            throw new ApplicationException(string.Format(CultureInfo.CurrentCulture, 
                "The state {0} returned cannot be parsed. Check if the status returned from yarn match enum defination", _application.FinalStatus));
        }

        private void Launch(IJobSubmission jobSubmission, string driverFolderPath)
        {
            _driverFolderPreparationHelper.PrepareDriverFolder(jobSubmission, driverFolderPath);

            //TODO: Remove this when we have a generalized way to pass config to java
            var javaParams = TangFactory.GetTang()
                .NewInjector(jobSubmission.DriverConfigurations.ToArray())
                .GetInstance<ClrClient2JavaClientCuratedParameters>();

            // Submit the driver
            _javaClientLauncher.Launch(
                JavaClassName,
                driverFolderPath, // arg: 0
                jobSubmission.JobIdentifier, // arg: 1
                jobSubmission.DriverMemory.ToString(), // arg: 2
                javaParams.TcpPortRangeStart.ToString(), // arg: 3
                javaParams.TcpPortRangeCount.ToString(), // arg: 4
                javaParams.TcpPortRangeTryCount.ToString(), // arg: 5
                javaParams.MaxApplicationSubmissions.ToString(), // arg: 6
                javaParams.DriverRestartEvaluatorRecoverySeconds.ToString(), // arg: 7
                _securityTokenKind, // arg: 8
                _securityTokenService, // arg: 9
                _jobSubmissionPrefix // arg: 10
                );
            Logger.Log(Level.Info, "Submitted the Driver for execution." + jobSubmission.JobIdentifier);
        }

        public string DriverUrl
        {
            get { return _driverUrl; }
        }

        /// <summary>
        /// Creates the temporary directory to hold the job submission.
        /// </summary>
        /// <returns>The path to the folder created.</returns>
        private string CreateDriverFolder(string jobId)
        {
            var timestamp = DateTime.Now.ToString("yyyyMMddHHmmssfff");
            return Path.GetFullPath(Path.Combine(Path.GetTempPath(), string.Join("-", "reef", jobId, timestamp)));
        }
    }
}