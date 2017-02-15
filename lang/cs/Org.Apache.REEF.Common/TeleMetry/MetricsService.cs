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
using System.Collections.Concurrent;
using System.Collections.Generic;
using Org.Apache.REEF.Common.Context;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Common.Telemetry
{
    public class MetricsService : IObserver<IContextMessage>, IObserver<IDriverMetrics>
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(MetricsService));
        private readonly IDictionary<string, ICounter> _counters = new ConcurrentDictionary<string, ICounter>();
        private readonly ISet<IMetricsSink> _metricsSinks;
        private int _totalIncrementSinceLastSink;

        [Inject]
        private MetricsService([Parameter(typeof(MetricSinks))] ISet<IMetricsSink> metricsSinks)
        {
            _metricsSinks = metricsSinks;
        }

        public void OnNext(IContextMessage contextMessage)
        {
            var msgReceived = ByteUtilities.ByteArraysToString(contextMessage.Message);
            Logger.Log(Level.Info, "$$$$$$$$$$$Received context message: " + msgReceived);
            var counters = new EvaluatorMetrics(msgReceived).GetMetricsCounters();
            var counterCollection = counters.GetCounters();
            
            foreach (var counter in counterCollection)
            {
                ICounter c;
                if (_counters.TryGetValue(counter.Name, out c))
                {
                    _totalIncrementSinceLastSink += counter.Value - c.Value;
                    _counters[counter.Name] = counter;
                }
                else
                {
                    _counters.Add(counter.Name, counter);
                    _totalIncrementSinceLastSink += counter.Value;
                }
                
                Logger.Log(Level.Info, "$$$$$$$$$$$ counter name: {0}, value: {1}, desc: {2}, time: {3}.", counter.Name, counter.Value, counter.Description, new DateTime(counter.Timestamp));
            }

            if (TriggerSink())
            {
                SinkCounters();
                _totalIncrementSinceLastSink = 0;
            }
        }

        /// <summary>
        /// The condition can be modified later
        /// </summary>
        /// <returns></returns>
        private bool TriggerSink()
        {
            return _totalIncrementSinceLastSink > 1;
        }

        /// <summary>
        /// Call each Sink to sink the data in the counters
        /// </summary>
        private void SinkCounters()
        {
            var set = new HashSet<KeyValuePair<string, string>>();
            foreach (var c in _counters)
            {
                set.Add(new KeyValuePair<string, string>(c.Key, c.Value.Value.ToString()));
            }

            Sink(set);
        }

        private void Sink(ISet<KeyValuePair<string, string>> set)
        {
            foreach (var s in _metricsSinks)
            {
                s.Sink(set);
            }
        }

        public void OnCompleted()
        {
            throw new NotImplementedException();
        }

        public void OnError(Exception error)
        {
            throw new NotImplementedException();
        }

        public void OnNext(IDriverMetrics value)
        {
            Logger.Log(Level.Info, "$$$$$$$$$$$MetricsService IDriverMetrics onNext is called: " + value.SystemState);
            var set = new HashSet<KeyValuePair<string, string>>();
            set.Add(new KeyValuePair<string, string>("SystemState", value.SystemState));
            Sink(set);
        }
    }
}
