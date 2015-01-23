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

using Org.Apache.Reef.Common.Evaluator;
using Org.Apache.Reef.Common.Evaluator.Context;
using Org.Apache.Reef.Common.io;
using Org.Apache.Reef.Common.ProtoBuf.ReefProtocol;
using Org.Apache.Reef.Tang.Interface;
using Org.Apache.Reef.Wake.Remote;
using Org.Apache.Reef.Wake.Time;
using System;

namespace Org.Apache.Reef.Evaluator
{
    // TODO: merge with EvaluatorConfigurations class
    public class EvaluatorSettings
    {
        private string _applicationId;

        private string _evaluatorId;

        private int _heartBeatPeriodInMs;

        private int _maxHeartbeatRetries;

        private ContextConfiguration _rootContextConfig;

        private IClock _clock;

        private IRemoteManager<REEFMessage> _remoteManager;

        private IInjector _injector;

        private EvaluatorOperationState _operationState;

        private INameClient _nameClient;

        public EvaluatorSettings(
            string applicationId,
            string evaluatorId,
            int heartbeatPeriodInMs,
            int maxHeartbeatRetries,
            ContextConfiguration rootContextConfig,
            IClock clock,
            IRemoteManager<REEFMessage> remoteManager,
            IInjector injecor)
        {
            if (string.IsNullOrWhiteSpace(evaluatorId))
            {
                throw new ArgumentNullException("evaluatorId");
            }
            if (rootContextConfig == null)
            {
                throw new ArgumentNullException("rootContextConfig");
            }
            if (clock == null)
            {
                throw new ArgumentNullException("clock");
            }
            if (remoteManager == null)
            {
                throw new ArgumentNullException("remoteManager");
            }
            if (injecor == null)
            {
                throw new ArgumentNullException("injecor");
            }
            _applicationId = applicationId;
            _evaluatorId = evaluatorId;
            _heartBeatPeriodInMs = heartbeatPeriodInMs;
            _maxHeartbeatRetries = maxHeartbeatRetries;
            _rootContextConfig = rootContextConfig;
            _clock = clock;
            _remoteManager = remoteManager;
            _injector = injecor;
            _operationState = EvaluatorOperationState.OPERATIONAL;
        }

        public EvaluatorOperationState OperationState
        {
            get
            {
                return _operationState;
            }

            set
            {
                _operationState = value;
            }
        }

        public string EvalutorId
        {
            get
            {
                return _evaluatorId;
            }
        }

        public int HeartBeatPeriodInMs
        {
            get
            {
                return _heartBeatPeriodInMs;
            }
        }

        public string ApplicationId
        {
            get
            {
                return _applicationId;
            }
        }

        public int MaxHeartbeatFailures
        {
            get
            {
                return _maxHeartbeatRetries;
            }
        }

        public ContextConfiguration RootContextConfig
        {
            get
            {
                return _rootContextConfig;
            }
        }

        public IClock RuntimeClock
        {
            get
            {
                return _clock;
            }
        }

        public INameClient NameClient
        {
            get
            {
                return _nameClient;
            }

            set
            {
                _nameClient = value;
            }
        }

        public IRemoteManager<REEFMessage> RemoteManager
        {
            get
            {
                return _remoteManager;
            }
        }

        public IInjector Injector
        {
            get
            {
                return _injector;
            }
        }
    }
}
