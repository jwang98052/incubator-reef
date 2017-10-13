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
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Examples.HelloREEF
{
    /// <summary>
    /// The Driver for HelloREEF: It requests a single Evaluator and then submits the HelloTask to it.
    /// </summary>
    public sealed class HelloDriverYarn : IObserver<IAllocatedEvaluator>, IObserver<IDriverStarted>, 
        IObserver<IFailedEvaluator>, IObserver<IFailedTask>, IObserver<ICompletedTask>, IObserver<IRunningTask>
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(HelloDriver));
        private readonly IEvaluatorRequestor _evaluatorRequestor;

        /// <summary>
        /// List of node names for desired evaluators
        /// </summary>
        private readonly IList<string> _nodeNames;

        /// <summary>
        /// Specify if the desired node names is relaxed
        /// </summary>
        private readonly bool _relaxLocality;

        private readonly int _numberOfContainers;

        /// <summary>
        /// Constructor of the driver
        /// </summary>
        /// <param name="evaluatorRequestor">Evaluator Requestor</param>
        /// <param name="nodeNames">Node names for evaluators</param>
        /// <param name="relaxLocality">Relax indicator of evaluator node request</param>
        /// <param name="numberOfContainers">Relax indicator of evaluator node request</param>
        [Inject]
        private HelloDriverYarn(IEvaluatorRequestor evaluatorRequestor,
            [Parameter(typeof(NodeNames))] ISet<string> nodeNames,
            [Parameter(typeof(RelaxLocality))] bool relaxLocality,
            [Parameter(typeof(NumberOfContainers))] int numberOfContainers)
        {
            Logger.Log(Level.Info, "HelloDriverYarn Driver: numberOfContainers: {0}.", numberOfContainers);
           _evaluatorRequestor = evaluatorRequestor;
            _nodeNames = nodeNames.ToList();
            _relaxLocality = relaxLocality;
            _numberOfContainers = numberOfContainers;
        }

        /// <summary>
        /// Submits the HelloTask to the Evaluator.
        /// </summary>
        /// <param name="allocatedEvaluator"></param>
        public void OnNext(IAllocatedEvaluator allocatedEvaluator)
        {
            var d1 = DateTime.Now;
            Logger.Log(Level.Info, "Received allocatedEvaluator-HostName: {0}, id {1}, thread id: {2}, time {3} ", allocatedEvaluator.GetEvaluatorDescriptor().NodeDescriptor.HostName, allocatedEvaluator.Id, Thread.CurrentThread.ManagedThreadId, d1);
            using (Logger.LogFunction("IAllocatedEvaluator handler:" + allocatedEvaluator.Id + "IAllocatedEvaluator.OnNext"))
            {
                var taskConfiguration = TaskConfiguration.ConfigurationModule
                    .Set(TaskConfiguration.Identifier, "HelloTask-" + allocatedEvaluator.Id)
                    .Set(TaskConfiguration.Task, GenericType<HelloTask>.Class)
                    .Build();
                 allocatedEvaluator.SubmitTask(taskConfiguration);
            }
            var d2 = DateTime.Now;
            var di = (d2 - d1).Milliseconds;
            Logger.Log(Level.Info, "End of allocatedEvaluator-HostName: {0}, id {1}, thread id: {2}, time {3}, duration: {4} ", allocatedEvaluator.GetEvaluatorDescriptor().NodeDescriptor.HostName, allocatedEvaluator.Id, Thread.CurrentThread.ManagedThreadId, d2, di);
        }                                              

        public void OnError(Exception error)
        {
            throw error;
        }

        public void OnCompleted()
        {
        }

        /// <summary>
        /// Called to start the user mode driver
        /// </summary>
        /// <param name="driverStarted"></param>
        public void OnNext(IDriverStarted driverStarted)
        {
            Logger.Log(Level.Info, string.Format("Received IDriverStarted at {0}", driverStarted.StartTime));

            if (_nodeNames != null && _nodeNames.Count > 0)
            {
                for (var i = 0; i < _numberOfContainers; i++)
                {
                    _evaluatorRequestor.Submit(_evaluatorRequestor.NewBuilder()
                        .AddNodeNames(_nodeNames)
                        .SetMegabytes(64)
                        .SetNumber(1)
                        .SetRelaxLocality(_relaxLocality)
                        .Build());
                }
            }
            else
            {
                _evaluatorRequestor.Submit(_evaluatorRequestor.NewBuilder()
                    .SetMegabytes(64)
                    .SetNumber(_numberOfContainers)
                    .SetCores(1)
                    .Build());
            }
        }

        void IObserver<ICompletedTask>.OnNext(ICompletedTask value)
        {
            Logger.Log(Level.Info, string.Format("Received ICompletedTask: {0} with evaluator id: {1} at {2}", value.Id, value.ActiveContext.EvaluatorId, DateTime.Now));
            value.ActiveContext.Dispose();
        }

        void IObserver<IFailedTask>.OnNext(IFailedTask value)
        {
            Logger.Log(Level.Info, string.Format("Received IFailedTask: {0} with evaluator id: {1} at {2}.", value.Id, value.GetActiveContext().Value.EvaluatorId, DateTime.Now));
            value.GetActiveContext().Value.Dispose();
        }

        void IObserver<IFailedEvaluator>.OnNext(IFailedEvaluator value)
        {
            Logger.Log(Level.Info, string.Format("Received IFailedEvaluator: {0} at {1}", value.Id, DateTime.Now));
        }

        void IObserver<IRunningTask>.OnNext(IRunningTask value)
        {
            Logger.Log(Level.Info, string.Format("Received IRunningTask: {0} with evaluator id: {1} at {2}", value.Id, value.ActiveContext.EvaluatorId, DateTime.Now));
        }

        void IObserver<ICompletedTask>.OnError(Exception error)
        {
            throw new NotImplementedException();
        }

        void IObserver<ICompletedTask>.OnCompleted()
        {
            throw new NotImplementedException();
        }

        void IObserver<IFailedTask>.OnError(Exception error)
        {
            throw new NotImplementedException();
        }

        void IObserver<IFailedTask>.OnCompleted()
        {
            throw new NotImplementedException();
        }
        void IObserver<IFailedEvaluator>.OnError(Exception error)
        {
            throw new NotImplementedException();
        }

        void IObserver<IFailedEvaluator>.OnCompleted()
        {
            throw new NotImplementedException();
        }

        void IObserver<IRunningTask>.OnError(Exception error)
        {
            throw new NotImplementedException();
        }

        void IObserver<IRunningTask>.OnCompleted()
        {
            throw new NotImplementedException();
        }
    }

    [NamedParameter(documentation: "Set of node names for evaluators")]
    internal class NodeNames : Name<ISet<string>>
    {    
    }

    [NamedParameter(documentation: "RelaxLocality for specifying evaluator node names", shortName: "RelaxLocality", defaultValue: "true")]
    internal class RelaxLocality : Name<bool>
    {        
    }

    [NamedParameter(documentation: "NumberOfContainers", defaultValue: "1")]
    internal class NumberOfContainers : Name<int>
    {
    }
}