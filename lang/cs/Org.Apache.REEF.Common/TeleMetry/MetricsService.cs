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
using Org.Apache.REEF.Common.Context;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Common.Telemetry
{
    public class MetricsService : IObserver<IContextMessage>
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(MetricsService));

        [Inject]
        private MetricsService()
        {            
        }

        public void OnNext(IContextMessage contextMessage)
        {
            var msgReceived = ByteUtilities.ByteArraysToString(contextMessage.Message);
            Logger.Log(Level.Info, "$$$$$$$$$$$Received context message: " + msgReceived);
            var c = new Counters(msgReceived);
            var c1 = c.GetCounters();
            foreach (var c2 in c1)
            {
                Logger.Log(Level.Info, "$$$$$$$$$$$ counter name: {0}, value: {1}, desc: {2}, time: {3}.", c2.Name, c2.Value, c2.Description, new DateTime(c2.Timestamp));
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
    }
}
