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

using System;
using System.Threading;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Common.Telemetry;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Tests.Functional.Telemetry
{
    public class MetricsTask : ITask
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(MetricsTask));

        public const string TestCounter1 = "TestCounter1";

        private readonly IEvaluatorMetrics _evaluatorMetrics;
        private readonly ICounters _counters;

        [Inject]
        private MetricsTask(IEvaluatorMetrics evaluatorMetrics)
        {
            _evaluatorMetrics = evaluatorMetrics;
            _counters = _evaluatorMetrics.GetMetricsCounters();
            _counters.TryRegisterCounter(TestCounter1, "This is test counter1");
        }

        public byte[] Call(byte[] memento)
        {
            for (int i = 0; i < 100; i++)
            {
                _counters.Increment(TestCounter1, 1);
                Thread.Sleep(100);
            }
            return null;
        }

        public void Dispose()
        {
        }
    }
}
