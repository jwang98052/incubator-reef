﻿/**
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
using System.Collections.Generic;
using System.Globalization;
using System.Net;
using System.Threading;
using Org.Apache.Reef.Common.io;
using Org.Apache.Reef.Driver.Context;
using Org.Apache.Reef.IO.Network.Group.Codec;
using Org.Apache.Reef.IO.Network.Group.Config;
using Org.Apache.Reef.IO.Network.Group.Task.Impl;
using Org.Apache.Reef.IO.Network.Naming;
using Org.Apache.Reef.IO.Network.NetworkService;
using Org.Apache.Reef.Services;
using Org.Apache.Reef.Utilities.Logging;
using Org.Apache.Reef.Tang.Annotations;
using Org.Apache.Reef.Tang.Formats;
using Org.Apache.Reef.Tang.Implementations;
using Org.Apache.Reef.Tang.Interface;
using Org.Apache.Reef.Tang.Util;
using Org.Apache.Reef.Wake.Remote;

namespace Org.Apache.Reef.IO.Network.Group.Driver.Impl
{
    /// <summary>
    /// Used to create Communication Groups for MPI Operators on the Reef driver.
    /// Also manages configuration for MPI tasks/services.
    /// </summary>
    public class MpiDriver : IMpiDriver
    {
        private const string MasterTaskContextName = "MasterTaskContext";
        private const string SlaveTaskContextName = "SlaveTaskContext";

        private static Logger LOGGER = Logger.GetLogger(typeof(MpiDriver));

        private string _driverId;
        private string _nameServerAddr;
        private int _nameServerPort;
        private int _contextIds;

        private Dictionary<string, ICommunicationGroupDriver> _commGroups; 
        private AvroConfigurationSerializer _configSerializer;
        private NameServer _nameServer;

        /// <summary>
        /// Create a new MpiDriver object.
        /// </summary>
        /// <param name="driverId">Identifer for the REEF driver</param>
        /// <param name="masterTaskId">Identifer for MPI master task</param>
        /// <param name="configSerializer">Used to serialize task configuration</param>
        [Inject]
        public MpiDriver(
            [Parameter(typeof(MpiConfigurationOptions.DriverId))] string driverId,
            [Parameter(typeof(MpiConfigurationOptions.MasterTaskId))] string masterTaskId,
            AvroConfigurationSerializer configSerializer)
        {
            _driverId = driverId;
            _contextIds = -1;
            MasterTaskId = masterTaskId;

            _configSerializer = configSerializer;
            _commGroups = new Dictionary<string, ICommunicationGroupDriver>();
            _nameServer = new NameServer(0);

            IPEndPoint localEndpoint = _nameServer.LocalEndpoint;
            _nameServerAddr = localEndpoint.Address.ToString();
            _nameServerPort = localEndpoint.Port;
        }

        /// <summary>
        /// Returns the identifier for the master task
        /// </summary>
        public string MasterTaskId { get; private set; }

        /// <summary>
        /// Create a new CommunicationGroup with the given name and number of tasks/operators. 
        /// </summary>
        /// <param name="groupName">The new group name</param>
        /// <param name="numTasks">The number of tasks/operators in the group.</param>
        /// <returns>The new Communication Group</returns>
        public ICommunicationGroupDriver NewCommunicationGroup(string groupName, int numTasks)
        {
            if (string.IsNullOrEmpty(groupName))
            {
                throw new ArgumentNullException("groupName");
            }
            else if (numTasks < 1)
            {
                throw new ArgumentException("NumTasks must be greater than 0");                
            }
            else if (_commGroups.ContainsKey(groupName))
            {
                throw new ArgumentException("Group Name already registered with MpiDriver");
            }

            var commGroup = new CommunicationGroupDriver(groupName, _driverId, numTasks, _configSerializer);
            _commGroups[groupName] = commGroup;
            return commGroup;
        }

        /// <summary>
        /// Generates context configuration with a unique identifier.
        /// </summary>
        /// <returns>The configured context configuration</returns>
        public IConfiguration GetContextConfiguration()
        {
            int contextNum = Interlocked.Increment(ref _contextIds);
            string id = (contextNum == 0) 
                ? MasterTaskContextName 
                : GetSlaveTaskContextName(contextNum);

            return ContextConfiguration.ConfigurationModule
                .Set(ContextConfiguration.Identifier, id)
                .Build();
        }

        /// <summary>
        /// Get the service configuration required for running MPI on Reef tasks.
        /// </summary>
        /// <returns>The service configuration for the Reef tasks</returns>
        public IConfiguration GetServiceConfiguration()
        {
            IConfiguration serviceConfig = ServiceConfiguration.ConfigurationModule
                .Set(ServiceConfiguration.Services, GenericType<NetworkService<GroupCommunicationMessage>>.Class)
                .Build();

            return TangFactory.GetTang().NewConfigurationBuilder(serviceConfig)
                .BindImplementation(
                    GenericType<IObserver<NsMessage<GroupCommunicationMessage>>>.Class,
                    GenericType<MpiNetworkObserver>.Class)
                .BindImplementation(
                    GenericType<ICodec<GroupCommunicationMessage>>.Class,
                    GenericType<GroupCommunicationMessageCodec>.Class)
                .BindNamedParameter<NamingConfigurationOptions.NameServerAddress, string>(
                    GenericType<NamingConfigurationOptions.NameServerAddress>.Class, 
                    _nameServerAddr)
                .BindNamedParameter<NamingConfigurationOptions.NameServerPort, int>(
                    GenericType<NamingConfigurationOptions.NameServerPort>.Class, 
                    _nameServerPort.ToString(CultureInfo.InvariantCulture))
                .Build();
        }

        /// <summary>
        /// Get the configuration for a particular task.  
        ///
        /// The task may belong to many Communication Groups, so each one is serialized
        /// in the configuration as a SerializedGroupConfig.
        ///
        /// The user must merge their part of task configuration (task id, task class)
        /// with this returned MPI task configuration.
        /// </summary>
        /// <param name="taskId">The id of the task Configuration to generate</param>
        /// <returns>The MPI task configuration with communication group and
        /// operator configuration set.</returns>
        public IConfiguration GetMpiTaskConfiguration(string taskId)
        {
            var confBuilder = TangFactory.GetTang().NewConfigurationBuilder();

            foreach (ICommunicationGroupDriver commGroup in _commGroups.Values)
            {
                var taskConf = commGroup.GetGroupTaskConfiguration(taskId);
                if (taskConf != null)
                {
                    confBuilder.BindSetEntry<MpiConfigurationOptions.SerializedGroupConfigs, string>(
                        GenericType<MpiConfigurationOptions.SerializedGroupConfigs>.Class,
                        _configSerializer.ToString(taskConf));
                }
            }

            return confBuilder.Build();
        }

        /// <summary>
        /// Checks whether this active context can be used to run the Master Task.
        /// </summary>
        /// <param name="activeContext">The active context to check</param>
        /// <returns>True if the active context can run the Master task,
        /// otherwise false.</returns>
        public bool IsMasterTaskContext(IActiveContext activeContext)
        {
            return activeContext.Id.Equals(MasterTaskContextName);
        }

        /// <summary>
        /// Checks whether this context configuration is used to configure the Master Task.
        /// </summary>
        /// <param name="contextConfiguration">The context configuration to check</param>
        /// <returns>True if the context configuration is used to configure the Master
        /// Task, otherwise false.</returns>
        public bool IsMasterContextConfiguration(IConfiguration contextConfiguration)
        {
            return Utilities.Utils.GetContextId(contextConfiguration).Equals(MasterTaskContextName);
        }

        /// <summary>
        /// Gets the context number associated with the Active Context id.
        /// </summary>
        /// <param name="activeContext">The active context to check</param>
        /// <returns>The context number associated with the active context id</returns>
        public int GetContextNum(IActiveContext activeContext)
        {
            if (activeContext.Id.Equals(MasterTaskContextName))
            {
                return 0;
            }

            string[] parts = activeContext.Id.Split('-');
            if (parts.Length != 2)
            {
                throw new ArgumentException("Invalid id in active context");
            }

            return int.Parse(parts[1], CultureInfo.InvariantCulture);
        }

        private string GetSlaveTaskContextName(int contextNum)
        {
            return string.Format(CultureInfo.InvariantCulture, "{0}-{1}", SlaveTaskContextName, contextNum);
        }
    }
}
