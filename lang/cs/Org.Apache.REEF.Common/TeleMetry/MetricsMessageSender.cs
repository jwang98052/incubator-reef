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

using Org.Apache.REEF.Common.Context;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Common.Telemetry
{
    public class MetricsMessageSender : IContextMessageSource
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(MetricsMessageSender));

        private readonly IEvaluatorMetrics _evaluatorMetrics;
        public const string MessageSourceId = "ContextMessageSourceID";

        [Inject]
        private MetricsMessageSender(IEvaluatorMetrics evaluatorMetrics)
        {
            _evaluatorMetrics = evaluatorMetrics;
        }

        public Optional<ContextMessage> Message
        {
            get
            {
                ICounter counter;
                _evaluatorMetrics.GetMetricsCounters().TryGetValue("TestCounter1", out counter);

                if (counter != null)
                {
                    Logger.Log(Level.Info, "$$$$$$$$MetricsMessageSender:" + counter.Value + ",   Serialized counter" + _evaluatorMetrics.Serialize());
                }

                if (_evaluatorMetrics.Serialize() != null)
                {
                    return Optional<ContextMessage>.Of(
                        ContextMessage.From(MessageSourceId,
                            ByteUtilities.StringToByteArrays(_evaluatorMetrics.Serialize())));
                }
                return Optional<ContextMessage>.Empty();
            }
        }
    }
}