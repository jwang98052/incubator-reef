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
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Utilities.Diagnostics;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.IMRU.OnREEF.Driver
{
    internal sealed class EvaluatorManager
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(EvaluatorManager));
        internal const string MasterBatchId = "MasterBatchId";
        internal const string MapperBatchId = "MapperBatchId";

        private readonly IDictionary<string, IAllocatedEvaluator> _allocatedEvaluators = new Dictionary<string, IAllocatedEvaluator>();
        private readonly IDictionary<string, IFailedEvaluator> _failedEvaluators = new Dictionary<string, IFailedEvaluator>();
        private readonly ISet<string> _contextLoadedEvaluators = new HashSet<string>(); 

        private readonly int _totalExpectedEvaluators;
        private readonly int _allowedNumberOfEvaluatorFailures;
        private readonly IEvaluatorRequestor _evaluatorRequestor;
        private string _masterEvaluatorId = null;

        private readonly EvaluatorSpecification _updateEvaluatorSpecification;
        private readonly EvaluatorSpecification _mapperEvaluatorSpecification;

        internal EvaluatorManager(
            int totalEvaluators,
            int allowedNumberOfEvaluatorFailures, 
            IEvaluatorRequestor evaluatorRequestor,
            EvaluatorSpecification updateEvaluatorSpecification, 
            EvaluatorSpecification mapperEvaluatorSpecification)
        {
            _totalExpectedEvaluators = totalEvaluators;
            _allowedNumberOfEvaluatorFailures = allowedNumberOfEvaluatorFailures;
            _evaluatorRequestor = evaluatorRequestor;
            _updateEvaluatorSpecification = updateEvaluatorSpecification;
            _mapperEvaluatorSpecification = mapperEvaluatorSpecification;
        }

        /// <summary>
        /// Request update/master evaluator from resource manager
        /// </summary>
        internal void RequestUpdateEvaluator()
        {
            _evaluatorRequestor.Submit(
                _evaluatorRequestor.NewBuilder()
                    .SetCores(_updateEvaluatorSpecification.Core)
                    .SetMegabytes(_updateEvaluatorSpecification.Megabytes)
                    .SetNumber(1)
                    .SetEvaluatorBatchId(MasterBatchId)
                    .Build());
        }

        /// <summary>
        /// Request map evaluators from resource manager
        /// </summary>
        /// <param name="numEvaluators">Number of evaluators to request</param>
        internal void RequestMapEvaluators(int numEvaluators)
        {
            _evaluatorRequestor.Submit(
                _evaluatorRequestor.NewBuilder()
                    .SetMegabytes(_mapperEvaluatorSpecification.Megabytes)
                    .SetNumber(numEvaluators)
                    .SetCores(_mapperEvaluatorSpecification.Core)
                    .SetEvaluatorBatchId(MapperBatchId)
                    .Build());
        }

        internal void AddAllocatedEvaluator(IAllocatedEvaluator evaluator)
        {
            if (IsAlloctedEvaluator(evaluator.Id))
            {
                string msg = string.Format("The allocated evaluator {0} already exists.", evaluator.Id);
                Exceptions.Throw(new IMRUSystemException(msg), Logger);
            }

            _allocatedEvaluators.Add(evaluator.Id, evaluator);
        }

        internal void RomoveAllocatedEvaluator(string evaluatorId)
        {
            if (!IsAlloctedEvaluator(evaluatorId))
            {
                string msg = string.Format("The allocated evaluator to be removed {0} does not exist.", evaluatorId);
                Exceptions.Throw(new IMRUSystemException(msg), Logger);
            }
            _allocatedEvaluators.Remove(evaluatorId);
        }

        internal void AddContextLoadedEvalutor(string evaluatorId)
        {
            if (!IsAlloctedEvaluator(evaluatorId))
            {
                string msg = string.Format("The evaluator that get context loaded {0} does not exist.", evaluatorId);
                Exceptions.Throw(new IMRUSystemException(msg), Logger);
            }

            if (IsContextLoadedEvaluator(evaluatorId))
            {
                string msg = string.Format("The evaluator {0} already has context loaded.", evaluatorId);
                Exceptions.Throw(new IMRUSystemException(msg), Logger);
            }

            _contextLoadedEvaluators.Add(evaluatorId);
        }

        internal bool IsContextLoadedEvaluator(string evaluatorId)
        {
            return _contextLoadedEvaluators.Contains(evaluatorId);
        }

        internal bool IsAlloctedEvaluator(string evaluatorId)
        {
            return _allocatedEvaluators.ContainsKey(evaluatorId);
        }

        internal void RecordFailedEvaluator(IFailedEvaluator evaluator)
        {
            if (!IsAlloctedEvaluator(evaluator.Id))
            {
                string msg = string.Format("The failed evaluator {0} does not exist in allocated evaluator collections.", evaluator.Id);
                Exceptions.Throw(new IMRUSystemException(msg), Logger);
            }
            _allocatedEvaluators.Remove(evaluator.Id);

            if (_failedEvaluators.ContainsKey(evaluator.Id))
            {
                string msg = string.Format("The failed evaluator {0} has been recorded.", evaluator.Id);
                Exceptions.Throw(new IMRUSystemException(msg), Logger);
            }
            _failedEvaluators.Add(evaluator.Id, evaluator);

            //// Evaluator can fail before the context is loaded. If the context has been loaded for the failed evaluator, remove it from  _contextLoadedEvaluators
            if (IsContextLoadedEvaluator(evaluator.Id))
            {
                _contextLoadedEvaluators.Remove(evaluator.Id);
            }
        }

        internal bool ReachedMaximumNumberOfEvaluatorFailures
        {
            get { return _failedEvaluators.Count >= _allowedNumberOfEvaluatorFailures; }
        }

        internal int AllowedNumberOfEvaluatorFailures
        {
            get { return _allowedNumberOfEvaluatorFailures; }
        }

        internal void ResetFailedEvalutors()
        {
            _failedEvaluators.Clear();
        }

        internal void SetMasterEvaluatorId(string evaluatorId)
        {
            if (_masterEvaluatorId != null)
            {
                string msg = string.Format("There is already a master evaluator {0}", _masterEvaluatorId);
                Exceptions.Throw(new IMRUSystemException(msg), Logger);
            }
            _masterEvaluatorId = evaluatorId;
        }

        internal bool IsMasterEvaluator(IAllocatedEvaluator evaluator)
        {
            return evaluator.EvaluatorBatchId.Equals(MasterBatchId);
        }

        internal bool IsMasterEvaluatorId(string evaluatorId)
        {
            return _masterEvaluatorId.Equals(evaluatorId);
        }

        internal bool IsMasterFailed()
        {
            return _failedEvaluators.Values.Any(e => IsMasterEvaluatorId(e.Id));
        }

        internal int NumberofFailedMappers()
        {
            if (IsMasterFailed())
            {
                return _failedEvaluators.Count - 1;
            }
            return _failedEvaluators.Count;
        }

        internal int NumberOfAllocatedEvaluators
        {
            get { return _allocatedEvaluators.Count; }
        }

        /// <summary>
        /// Checks if all the expected Evaluators are allocated.
        /// </summary>
        internal bool AreAllEvaluatorsAllocated
        {
            get { return _totalExpectedEvaluators == NumberOfAllocatedEvaluators; }
        }

        internal int NumberOfMissingEvaluators
        {
            get { return _totalExpectedEvaluators - NumberOfAllocatedEvaluators; }
        }
    }
}
