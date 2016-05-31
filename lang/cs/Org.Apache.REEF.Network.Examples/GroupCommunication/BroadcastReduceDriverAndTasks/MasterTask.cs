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
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Common.Tasks.Events;
using Org.Apache.REEF.Network.Group.Operators;
using Org.Apache.REEF.Network.Group.Task;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Network.Examples.GroupCommunication.BroadcastReduceDriverAndTasks
{
    public class MasterTask : ITask, IObserver<ICloseEvent>
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(MasterTask));

        private readonly int _numIters;
        private readonly int _numReduceSenders;

        private readonly IGroupCommClient _groupCommClient;
        private readonly ICommunicationGroupClient _commGroup;
        private readonly IBroadcastSender<int> _broadcastSender;
        private readonly IReduceReceiver<int> _sumReducer;
        private bool _break;
        private bool _stoped;

        [Inject]
        public MasterTask(
            [Parameter(typeof(GroupTestConfig.NumIterations))] int numIters,
            [Parameter(typeof(GroupTestConfig.NumEvaluators))] int numEvaluators,
            IGroupCommClient groupCommClient)
        {
            Logger.Log(Level.Info, "Hello from master task");
            _numIters = numIters;
            _numReduceSenders = numEvaluators - 1;
            _groupCommClient = groupCommClient;

            _commGroup = groupCommClient.GetCommunicationGroup(GroupTestConstants.GroupName);
            _broadcastSender = _commGroup.GetBroadcastSender<int>(GroupTestConstants.BroadcastOperatorName);
            _sumReducer = _commGroup.GetReduceReceiver<int>(GroupTestConstants.ReduceOperatorName);
        }

        public byte[] Call(byte[] memento)
        {
            Stopwatch broadcastTime = new Stopwatch();
            Stopwatch reduceTime = new Stopwatch();

            for (int i = 1; i <= _numIters; i++)
            {
                if (_break)
                {
                    Logger.Log(Level.Info, "$$$$$$$$$$$$$$returning from slave task by clsoe event");
                    _stoped = true;
                    return null;
                }
                if (i == 2)
                {
                    broadcastTime.Reset();
                    reduceTime.Reset();
                }

                broadcastTime.Start();

                // Each slave task calculates the nth triangle number
                _broadcastSender.Send(i);
                broadcastTime.Stop();

                reduceTime.Start();

                // Sum up all of the calculated triangle numbers
                int sum = _sumReducer.Reduce();
                reduceTime.Stop();

                Logger.Log(Level.Info, "Received sum: {0} on iteration: {1}", sum, i);

                int expected = TriangleNumber(i) * _numReduceSenders;
                if (sum != TriangleNumber(i) * _numReduceSenders)
                {
                    throw new Exception("Expected " + expected + " but got " + sum);
                }

                if (i >= 2)
                {
                    var msg = string.Format("Average time (milliseconds) taken for broadcast: {0} and reduce: {1}",
                            broadcastTime.ElapsedMilliseconds / ((double)(i - 1)),
                            reduceTime.ElapsedMilliseconds / ((double)(i - 1)));
                    Logger.Log(Level.Info, msg);
                }
                //// simulate fail
                ////if (i > 1)
                ////{
                ////    throw new Exception("$$$$$$$$$$$$$$in master throw exception");
                ////}
            }

            _stoped = true;
            return null;
        }

        public void Dispose()
        {
            _groupCommClient.Dispose();
        }

        private int TriangleNumber(int n)
        {
            return Enumerable.Range(1, n).Sum();
        }

        public void OnNext(ICloseEvent value)
        {
            Logger.Log(Level.Info, "#######################SlaveTask ICloseEvent");
            _break = true;
            Thread.Sleep(2000);
            if (!_stoped)
            {
                throw new SystemException("Kille by driver.");
            }
        }

        public void OnError(Exception error)
        {
            throw new NotImplementedException();
        }

        public void OnCompleted()
        {
            throw new NotImplementedException();
        }
    }
}
