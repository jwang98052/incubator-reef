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
using System.Globalization;
using System.Linq;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.IMRU.API;
using Org.Apache.REEF.IMRU.OnREEF.Driver.StateMachine;
using Org.Apache.REEF.IMRU.OnREEF.IMRUTasks;
using Org.Apache.REEF.IMRU.OnREEF.MapInputWithControlMessage;
using Org.Apache.REEF.IMRU.OnREEF.Parameters;
using Org.Apache.REEF.IMRU.OnREEF.ResultHandler;
using Org.Apache.REEF.IO.PartitionedData;
using Org.Apache.REEF.Network.Group.Config;
using Org.Apache.REEF.Network.Group.Driver;
using Org.Apache.REEF.Network.Group.Pipelining;
using Org.Apache.REEF.Network.Group.Pipelining.Impl;
using Org.Apache.REEF.Network.Group.Topology;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Diagnostics;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.IMRU.OnREEF.Driver
{
    /// <summary>
    /// Implements the IMRU driver on REEF
    /// </summary>
    /// <typeparam name="TMapInput">Map Input</typeparam>
    /// <typeparam name="TMapOutput">Map output</typeparam>
    /// <typeparam name="TResult">Result</typeparam>
    /// <typeparam name="TPartitionType">Type of data partition (Generic type in IInputPartition)</typeparam>
    internal sealed class IMRUDriver<TMapInput, TMapOutput, TResult, TPartitionType> 
        : IObserver<IDriverStarted>,
        IObserver<IAllocatedEvaluator>,
        IObserver<IActiveContext>,
        IObserver<ICompletedTask>,
        IObserver<IFailedEvaluator>,
        IObserver<IFailedContext>,
        IObserver<IFailedTask>,
        IObserver<IRunningTask>,
        IObserver<IDictionary<string, IActiveContext>>
    {
        private static readonly Logger Logger =
            Logger.GetLogger(typeof(IMRUDriver<TMapInput, TMapOutput, TResult, TPartitionType>));

        private readonly ConfigurationManager _configurationManager;
        private readonly int _totalMappers;
        private readonly IGroupCommDriver _groupCommDriver;
        private readonly ConcurrentStack<IConfiguration> _perMapperConfiguration;
        private readonly ISet<IPerMapperConfigGenerator> _perMapperConfigs;
        private readonly TaskManager _taskManager;
        private readonly bool _invokeGC;
        private readonly object _lock = new object();

        private readonly ActiveContextManager _contextManager;
        private readonly EvaluatorManager _evaluatorManager;
        private readonly int _maxRetryNumberForFaultTolerant;
        private SystemStateMachine _systemState;
        private bool _recoveryMode = false;
        private int _numberOfRetryForFaultTolerant = 0;

        private readonly ServiceAndContextConfigurationProvider<TMapInput, TMapOutput, TPartitionType>
            _serviceAndContextConfigurationProvider;

        [Inject]
        private IMRUDriver(IPartitionedInputDataSet dataSet,
            [Parameter(typeof(PerMapConfigGeneratorSet))] ISet<IPerMapperConfigGenerator> perMapperConfigs,
            ConfigurationManager configurationManager,
            IEvaluatorRequestor evaluatorRequestor,
            [Parameter(typeof(CoresPerMapper))] int coresPerMapper,
            [Parameter(typeof(CoresForUpdateTask))] int coresForUpdateTask,
            [Parameter(typeof(MemoryPerMapper))] int memoryPerMapper,
            [Parameter(typeof(MemoryForUpdateTask))] int memoryForUpdateTask,
            [Parameter(typeof(AllowedFailedEvaluatorsFraction))] double failedEvaluatorsFraction,
            [Parameter(typeof(MaxRetryNumberInRecovery))] int maxRetryNumberInRecovery,
            [Parameter(typeof(InvokeGC))] bool invokeGC,
            IGroupCommDriver groupCommDriver)
        {
            _configurationManager = configurationManager;
            _groupCommDriver = groupCommDriver;
            _perMapperConfigs = perMapperConfigs;
            _totalMappers = dataSet.Count;

            var allowedFailedEvaluators = (int)(failedEvaluatorsFraction * dataSet.Count);

            _contextManager = new ActiveContextManager(_totalMappers + 1);
            _contextManager.Subscribe(this);

            EvaluatorSpecification updateSpec = new EvaluatorSpecification(memoryForUpdateTask, coresForUpdateTask);
            EvaluatorSpecification mapperSpec = new EvaluatorSpecification(memoryPerMapper, coresPerMapper);

            _evaluatorManager = new EvaluatorManager(_totalMappers + 1, allowedFailedEvaluators, evaluatorRequestor, updateSpec, mapperSpec);
            _taskManager = new TaskManager(_totalMappers + 1, _groupCommDriver.MasterTaskId, _groupCommDriver);

            ////fault tolerant
            _maxRetryNumberForFaultTolerant = maxRetryNumberInRecovery;

            _invokeGC = invokeGC;

            _perMapperConfiguration = ConstructPerMapperConfigStack(_totalMappers);
            _serviceAndContextConfigurationProvider =
                new ServiceAndContextConfigurationProvider<TMapInput, TMapOutput, TPartitionType>(dataSet);

            var msg =
                string.Format("map task memory:{0}, update task memory:{1}, map task cores:{2}, update task cores:{3}",
                    memoryPerMapper,
                    memoryForUpdateTask,
                    coresPerMapper,
                    coresForUpdateTask);
            Logger.Log(Level.Info, msg);
        }

        /// <summary>
        /// Requests for evaluators
        /// </summary>
        /// <param name="value">Event fired when driver started</param>
        public void OnNext(IDriverStarted value)
        {
            StartAction();
        }

        #region IAllocatedEvaluator
        /// <summary>
        /// Case WaitingForEvaluator
        ///    Add Evaluator to the Evaluator List
        ///    submit Context and Services
        /// Case Fail
        ///    Do nothing
        /// Other cases - not expected
        /// </summary>
        /// <param name="allocatedEvaluator">The allocated evaluator</param>
        public void OnNext(IAllocatedEvaluator allocatedEvaluator)
        {
            Logger.Log(Level.Verbose, string.Format(CultureInfo.InvariantCulture, "AllocatedEvaluator EvaluatorBatchId [{0}], memory [{1}], systemState {2}.", allocatedEvaluator.EvaluatorBatchId, allocatedEvaluator.GetEvaluatorDescriptor().Memory, _systemState.CurrentState));
            lock (_lock)
            {
                switch (_systemState.CurrentState)
                {
                    case SystemState.WaitingForEvaluator:
                        _evaluatorManager.AddAllocatedEvaluator(allocatedEvaluator);
                        SubmitContextAndService(allocatedEvaluator);
                        break;
                    case SystemState.Fail:
                        Logger.Log(Level.Verbose, "Receiving IAllocatedEvaluator event, but system is in FAIL state, ignore it.");
                        break;
                    default:
                        UnexpectedState(allocatedEvaluator.Id, "IAllocatedEvaluator");
                        break;
                }
            }
        }

        /// <summary>
        /// Gets context and service configuration for evaluator depending
        /// on whether it is for update function or for map function
        /// Then submits context and Service with the configuration
        /// </summary>
        /// <param name="allocatedEvaluator"></param>
        private void SubmitContextAndService(IAllocatedEvaluator allocatedEvaluator)
        {
            ContextAndServiceConfiguration configs;
            if (_evaluatorManager.IsEvaluatorForMaster(allocatedEvaluator))
            {
                configs =
                    _serviceAndContextConfigurationProvider
                        .GetContextConfigurationForMasterEvaluatorById(
                            allocatedEvaluator.Id);
            }
            else
            {
                configs = _serviceAndContextConfigurationProvider
                    .GetDataLoadingConfigurationForEvaluatorById(
                        allocatedEvaluator.Id);
            }
            allocatedEvaluator.SubmitContextAndService(configs.Context, configs.Service);
        }

        #endregion IAllocatedEvaluator

        #region IActiveContext
        /// <summary>
        /// Adds active context to _activeContexts collection. 
        /// Case WaitingForEvaluator
        ///    Adds Active Context to Active Context Manager
        ///    If the contexts reach to the total number, triggers Submit Tasks Action in ActiveContextManager
        /// Case Fail:
        ///    Closes the ActiveContext
        /// Other cases - not expected
        /// </summary>
        /// <param name="activeContext"></param>
        public void OnNext(IActiveContext activeContext)
        {
            Logger.Log(Level.Verbose, string.Format(CultureInfo.InvariantCulture, "Received Active Context {0}, systemState {1}.", activeContext.Id, _systemState.CurrentState));
            lock (_lock)
            {
                switch (_systemState.CurrentState)
                {
                    case SystemState.WaitingForEvaluator:
                        _contextManager.Add(activeContext);
                        break;
                    case SystemState.Fail:
                        Logger.Log(Level.Info, "Received IActiveContext event, but system is in FAIL state. Closing the context.");
                        activeContext.Dispose();
                        break;
                    default:
                        UnexpectedState(activeContext.Id, "IActiveContext");
                        break;
                }
            }
        }
        #endregion IActiveContext

        #region submit tasks
        /// <summary>
        /// Called from ActiveContextManager when all the expected active context are received.
        /// It calls SubmitTasks().
        /// </summary>
        /// <param name="value"></param>
        public void OnNext(IDictionary<string, IActiveContext> value)
        {
            Logger.Log(Level.Info, string.Format("Received event from ActiveContextManager with NumberOfActiveContexts :" + value.Count));
            lock (_lock)
            {
                //// Change the system state to SubmittingTasks
                _systemState.MoveNext(SystemStateEvent.AllContextsAreReady);
                SubmitTasks();
            }
        }

        /// <summary>
        /// Creates a new Communication Group and adds Group Communication Operators,
        /// specifies Map or Update task to run on each active context.
        ///     Create a new Communication group
        ///     For each context, adds a task to the communication group, and adds the task to TaskManager
        ///     Make sure one master task, rest are slave tasks
        ///     When all the tasks are added, calls TaskManager to SubmitTasks()
        /// </summary>
        private void SubmitTasks()
        {
            Logger.Log(Level.Info, string.Format("SubmitTasks with system state :" + _systemState.CurrentState));
            var commGroup = AddCommunicationGroupWithOperators();
            _taskManager.Reset();

            foreach (var activeContext in _contextManager.ActiveContexts)
            {
                if (_evaluatorManager.IsMasterEvaluatorId(activeContext.EvaluatorId))
                {
                    Logger.Log(Level.Verbose, "Submitting master task");
                    commGroup.AddTask(_groupCommDriver.MasterTaskId);
                    _taskManager.AddTask(_groupCommDriver.MasterTaskId, GetUpdateTaskConfiguration(_groupCommDriver.MasterTaskId), activeContext);
                }
                else
                {
                    Logger.Log(Level.Verbose, "Submitting map task");
                    var taskId = GetTaskIdByEvaluatorId(activeContext.EvaluatorId);
                    commGroup.AddTask(taskId);
                    _taskManager.AddTask(taskId, GetMapTaskConfiguration(activeContext, taskId), activeContext);
                }
            }
            _taskManager.SubmitTasks();
        }
        #endregion submit tasks

        #region IRunningTask
        /// <summary>
        /// Case WaitingForEvaluator
        ///     Add it to RunningTasks and set task state to TaskRunning
        ///     When all the tasks are running, change system state to TasksRunning
        /// Case ShuttingDown/Fail
        ///     Change the task state to TaskRunning
        ///     Send command to close the task itself
        ///     Change the task state to TaskWaitingForClose
        /// Other cases - not expected 
        /// </summary>
        /// <param name="runningTask"></param>
        public void OnNext(IRunningTask runningTask)
        {
            Logger.Log(Level.Info, string.Format(CultureInfo.InvariantCulture, "Received IRunningTask {0}, systemState {1}", runningTask.Id, _systemState.CurrentState));
            lock (_lock)
            {
                switch (_systemState.CurrentState)
                {
                    case SystemState.SubmittingTasks:
                        _taskManager.SetRunningTask(runningTask);
                        if (_taskManager.AreAllTasksRunning())
                        {
                            _systemState.MoveNext(SystemStateEvent.AllTasksAreRunning);
                            Logger.Log(Level.Info, string.Format(CultureInfo.InvariantCulture, "All tasks are running, systemState {0}", _systemState.CurrentState));
                        }
                        break;
                    case SystemState.ShuttingDown:
                    case SystemState.Fail:
                        _taskManager.CloseRunningTaskInSystemFailure(runningTask, TaskManager.TaskKilledByDriver);
                        break;
                    default:
                        UnexpectedState(runningTask.Id, "IRuningTask");
                        break;
                }
            }
        }
        #endregion IRunningTask

        #region ICompletedTask
        /// <summary>
        /// Case TasksRunning
        ///     Updates task states to TaskCompleted
        ///     If all tasks are completed, sets system state to TasksCompleted and then go to Done action
        /// Case ShuttingDown
        ///     Update task states to TaskCompleted
        ///     if RecoveryCondition == true, Start Action
        ///     else change system state to FAIL, take FAIL action
        /// Other cases - not expected 
        /// </summary>
        /// <param name="completedTask">The link to the completed task</param>
        public void OnNext(ICompletedTask completedTask)
        {
            Logger.Log(Level.Info, string.Format(CultureInfo.InvariantCulture, "Received ICompletedTask {0}, systemState {1}", completedTask.Id, _systemState.CurrentState));

            lock (_lock)
            {
                switch (_systemState.CurrentState)
                {
                    case SystemState.TasksRunning:
                        _taskManager.SetCompletedTask(completedTask.Id);
                        if (_taskManager.AreAllTasksCompleted())
                        {
                            _systemState.MoveNext(SystemStateEvent.AllTasksAreCompleted);
                            Logger.Log(Level.Info, string.Format(CultureInfo.InvariantCulture, "All tasks are completed, systemState {0}", _systemState.CurrentState));
                            DoneAction();
                        }
                        break;
                    case SystemState.ShuttingDown:
                        //// The task might be in running state or waiting for close, set it to complete anyway to make its state final
                        _taskManager.SetCompletedTask(completedTask.Id);
                        CheckRecovery();
                        break;
                    default:
                        UnexpectedState(completedTask.Id, "ICompletedTask");
                        break;
                }
            }            
        }
        #endregion ICompletedTask

        #region IFailedEvaluator
        /// <summary>
        /// Specifies what to do when evaluator fails.
        /// If we get all completed tasks then ignore the failure
        /// Case WaitingForEvaluator
        ///     This happens in the middle of submitting contexts. We just need to simply remove the failed evaluator 
        ///     from EvaluatorManager and remove associated active context, if any, from ActiveContextManager
        ///     then checks if the system is recoverable. If yes, request another Evaluator 
        ///     If not recoverable, set system state to Fail then execute Fail action
        /// Case SubmittingTasks/TasksRunning
        ///     This happens either in the middle of Task submitting or all the tasks are running
        ///     Changes the system state to ShuttingDown
        ///     Removes Evaluator and associated context from EvaluatorManager and ActiveContextManager
        ///     Removes associated task from running task if it was running and change the task state to TaskFailedByEvaluatorFailure
        ///     Closes all the other running tasks
        ///     Checks for recovery in case it is the last failure received
        /// Case ShuttingDown
        ///     This happens when we have received either FailedEvaluator or FailedTask, some tasks are running some are in closing.
        ///     Removes Evaluator and associated context from EvaluatorManager and ActiveContextManager
        ///     Removes associated task from running task if it was running, changes the task state to ClosedTask if it was waiting for close
        ///     otherwise changes the task state to FailedTaskEvaluatorError
        ///     Checks for recovery in case it is the last failure received
        /// Other cases - not expected 
        /// </summary>
        /// <param name="failedEvaluator"></param>
        public void OnNext(IFailedEvaluator failedEvaluator)
        {
            lock (_lock)
            {
                if (_taskManager.AreAllTasksCompleted())
                {
                    Logger.Log(Level.Verbose, string.Format("Evaluator with Id: {0} failed but IMRU task is completed. So ignoring.", failedEvaluator.Id));
                    return;
                }
                Logger.Log(Level.Info, string.Format("Evaluator with Id: {0} failed with Exception: {1}", failedEvaluator.Id, failedEvaluator.EvaluatorException));

                var isMater = _evaluatorManager.IsMasterEvaluatorId(failedEvaluator.Id);

                switch (_systemState.CurrentState)
                {
                    case SystemState.WaitingForEvaluator:
                        _evaluatorManager.RecordFailedEvaluator(failedEvaluator.Id);
                        _contextManager.RemoveFailedContextInFailedEvaluator(failedEvaluator);
                        if (Recoverable())
                        {
                            _numberOfRetryForFaultTolerant++;
                            if (isMater)
                            {
                                Logger.Log(Level.Info, "Requesting a master Evaluator.");
                                _evaluatorManager.RequestUpdateEvaluator();
                            }
                            else
                            {
                                _serviceAndContextConfigurationProvider.RecordEvaluatorFailureById(failedEvaluator.Id);
                                Logger.Log(Level.Info, string.Format("Requesting a map Evaluators."));
                                _evaluatorManager.RequestMapEvaluators(1);
                            }
                        }
                        else
                        {
                            _systemState.MoveNext(SystemStateEvent.NotRecoverable);
                            FailAction();
                        }
                        break;

                    case SystemState.SubmittingTasks:
                    case SystemState.TasksRunning:
                        //// set system state to ShuttingDown
                        _systemState.MoveNext(SystemStateEvent.FailedNode);
                        _evaluatorManager.RecordFailedEvaluator(failedEvaluator.Id);
                        _contextManager.RemoveFailedContextInFailedEvaluator(failedEvaluator);
                        _taskManager.SetTaskFailByEvaluator(failedEvaluator);
                        _taskManager.CloseAllRunningTasks(TaskManager.CloseTaskByDriver);

                        //// Push evaluator id back to PartitionIdProvider if it is not master
                        if (!isMater)
                        {
                            _serviceAndContextConfigurationProvider.RecordEvaluatorFailureById(failedEvaluator.Id);
                        }

                        CheckRecovery();
                        break;

                    case SystemState.ShuttingDown:
                        _evaluatorManager.RecordFailedEvaluator(failedEvaluator.Id);
                        _contextManager.RemoveFailedContextInFailedEvaluator(failedEvaluator);
                        _taskManager.SetTaskFailByEvaluator(failedEvaluator);

                        //// Push evaluator id back to PartitionIdProvider if it is not master
                        if (!isMater)
                        {
                            _serviceAndContextConfigurationProvider.RecordEvaluatorFailureById(failedEvaluator.Id);
                        }
                        CheckRecovery();
                        break;

                    default:
                        UnexpectedState(failedEvaluator.Id, "IFailedEvaluator");
                        break;
                }
            }
        }
        #endregion IFailedEvaluator

        #region IFailedContext
        /// <summary>
        /// Specifies what to do if Failed Context is received.
        /// If we get all completed tasks then ignore the failure
        /// An exception is thrown if tasks are not completed.
        /// Fault tolerant would be similar to FailedEvaluator. It is in TODO
        /// </summary>
        /// <param name="failedContext"></param>
        public void OnNext(IFailedContext failedContext)
        {
            lock (_lock)
            {
                if (_taskManager.AreAllTasksCompleted())
                {
                    Logger.Log(Level.Info,
                        string.Format("Context with Id: {0} failed but IMRU tasks are completed. So ignoring.", failedContext.Id));
                    return;
                }

                var msg = string.Format("Context with Id: {0} failed with Evaluator id: {1}", failedContext.Id, failedContext.EvaluatorId);
                Exceptions.Throw(new Exception(msg), Logger);
            }
        }
        #endregion IFailedContext

        #region IFailedTask
        /// <summary>
        /// Specifies what to do when task fails.
        /// If we get all completed tasks then ignore the failure
        /// Case SubmittingTasks/TasksRunning
        ///     This is the first failure received
        ///     Changes the system state to ShuttingDown
        ///     Removes the task from Running Tasks if the task is running
        ///     Updates task state based on the error message
        ///     Closes all the other running tasks and set their state to TaskWaitingForClose
        ///     Check recovery
        /// Case ShuttingDown
        ///     This happens when we have received either FailedEvaluator or FailedTask, some tasks are running some are in closing.
        ///     If the task is in TaskWaitingForClose, change its state to TaskClosedByDriver
        ///     otherwise, as long as the task state is not TaskFailedByEvaluatorFailure, set task fail state based on the failedTask Message
        ///     Check recovery
        /// Other cases - not expected 
        /// </summary>
        /// <param name="failedTask"></param>
        public void OnNext(IFailedTask failedTask)
        {
            lock (_lock)
            {
                if (_taskManager.AreAllTasksCompleted())
                {
                    Logger.Log(Level.Info,
                        string.Format("Task with Id: {0} failed but IMRU task is completed. So ignoring.", failedTask.Id));
                    return;
                }

                Logger.Log(Level.Info, string.Format("Task with Id: {0} failed with message: {1}", failedTask.Id, failedTask.Message));

                switch (_systemState.CurrentState)
                {
                    case SystemState.SubmittingTasks:
                    case SystemState.TasksRunning:
                        //// set system state to ShuttingDown
                        _systemState.MoveNext(SystemStateEvent.FailedNode);
                        _taskManager.SetFailedRunningTask(failedTask);
                        _taskManager.CloseAllRunningTasks(TaskManager.CloseTaskByDriver);
                        CheckRecovery();
                        break;

                    case SystemState.ShuttingDown:
                        _taskManager.SetFailedTaskInShuttingDown(failedTask);
                        CheckRecovery();
                        break;

                    default:
                        UnexpectedState(failedTask.Id, "IFailedTask");
                        break;
                }
            }
        }
        #endregion IFailedTask

        public void OnError(Exception error)
        {
        }

        public void OnCompleted()
        {
        }

        private void UnexpectedState(string id, string eventName)
        {
            var msg = string.Format(CultureInfo.InvariantCulture,
                "Received {0} for [{1}], but system status is {2}.",
                eventName,
                id,
                _systemState.CurrentState);
            Logger.Log(Level.Warning, msg);
        }

        /// <summary>
        /// If all the tasks are in final state, if the ystem is recoverable, start recovery
        /// else, change the system state to Fail then take Fail action
        /// </summary>
        private void CheckRecovery()
        {
            if (_taskManager.AllInFinalState())
            {
                if (Recoverable())
                {
                    StartAction();
                }
                else
                {
                    _systemState.MoveNext(SystemStateEvent.NotRecoverable);
                    FailAction();
                }
            }
        }

        private string GetTaskIdByEvaluatorId(string evaluatorId)
        {
            return string.Format("{0}-{1}-Version0",
                IMRUConstants.MapTaskPrefix,
                _serviceAndContextConfigurationProvider.GetPartitionIdByEvaluatorId(evaluatorId));
        }

        private void DoneAction()
        {
            ShutDownAllEvaluators();
        }

        private void FailAction()
        {
            ShutDownAllEvaluators();
        }

        /// <summary>
        /// Shuts down evaluators once all completed task messages are received
        /// </summary>
        private void ShutDownAllEvaluators()
        {
            foreach (var context in _contextManager.ActiveContexts)
            {
                Logger.Log(Level.Info, string.Format("Disposing active context: {0}", context.Id));
                context.Dispose();
            }
        }

        /// <summary>
        /// Resets NumberOfFailedEvaluator and numberoAppErrors 
        /// changes state to WAITING_FOR_EVALUATORS
        /// If master is missing, request master
        /// Based on the count in the evaluator list, requests total - count evaluators
        /// </summary>
        private void StartAction()
        {
            lock (_lock)
            {
                bool requestMaster = !_recoveryMode || _evaluatorManager.IsMasterEvaluatorFailed();
                int mappersToRequest = _recoveryMode ? _evaluatorManager.NumberofFailedMappers() : _totalMappers;

                ////reset failures
                _evaluatorManager.ResetFailedEvaluators();
                _taskManager.Reset();

                if (_systemState == null)
                {
                    _systemState = new SystemStateMachine();
                }
                else
                {
                    _numberOfRetryForFaultTolerant++;
                    _systemState.MoveNext(SystemStateEvent.Recover);
                }

                if (requestMaster)
                {
                    Logger.Log(Level.Info, "Requesting a master Evaluator.");
                    _evaluatorManager.RequestUpdateEvaluator();
                }

                if (mappersToRequest > 0)
                {
                    Logger.Log(Level.Info, string.Format("Requesting {0} map Evaluators.", mappersToRequest));
                    _evaluatorManager.RequestMapEvaluators(mappersToRequest);
                }
            }
        }

        private bool Recoverable()
        {
            return !_evaluatorManager.ReachedMaximumNumberOfEvaluatorFailures()
                && _taskManager.NumberOfAppError() == 0
                && _numberOfRetryForFaultTolerant < _maxRetryNumberForFaultTolerant;
        }

        /// <summary>
        /// Generates map task configuration given the active context. 
        /// Merge configurations of all the inputs to the MapTaskHost.
        /// </summary>
        /// <param name="activeContext">Active context to which task needs to be submitted</param>
        /// <param name="taskId">Task Id</param>
        /// <returns>Map task configuration</returns>
        private IConfiguration GetMapTaskConfiguration(IActiveContext activeContext, string taskId)
        {
            IConfiguration mapSpecificConfig;

            if (!_perMapperConfiguration.TryPop(out mapSpecificConfig))
            {
                Exceptions.Throw(
                    new IMRUSystemException(string.Format("No per map configuration exist for the active context {0}",
                        activeContext.Id)),
                    Logger);
            }

            return TangFactory.GetTang()
                .NewConfigurationBuilder(TaskConfiguration.ConfigurationModule
                    .Set(TaskConfiguration.Identifier, taskId)
                    .Set(TaskConfiguration.Task, GenericType<MapTaskHost<TMapInput, TMapOutput>>.Class)
                    .Build(),
                    _configurationManager.MapFunctionConfiguration,
                    mapSpecificConfig,
                    GetGroupCommConfiguration())
                .BindNamedParameter<InvokeGC, bool>(GenericType<InvokeGC>.Class, _invokeGC.ToString())
                .Build();
        }

        /// <summary>
        /// Generates the update task configuration.
        /// Merge configurations of all the inputs to the UpdateTaskHost.
        /// </summary>
        /// <returns>Update task configuration</returns>
        private IConfiguration GetUpdateTaskConfiguration(string taskId)
        {
            var partialTaskConf =
                TangFactory.GetTang()
                    .NewConfigurationBuilder(TaskConfiguration.ConfigurationModule
                        .Set(TaskConfiguration.Identifier,
                            taskId)
                        .Set(TaskConfiguration.Task,
                            GenericType<UpdateTaskHost<TMapInput, TMapOutput, TResult>>.Class)
                        .Build(),
                        _configurationManager.UpdateFunctionConfiguration,
                        _configurationManager.ResultHandlerConfiguration,
                        GetGroupCommConfiguration())
                    .BindNamedParameter<InvokeGC, bool>(GenericType<InvokeGC>.Class, _invokeGC.ToString())
                    .Build();

            // This piece of code basically checks if user has given any implementation 
            // of IIMRUResultHandler. If not then bind it to default implementation which 
            // does nothing. For interfaces with generic type we cannot assign default 
            // implementation.
            try
            {
                TangFactory.GetTang()
                    .NewInjector(partialTaskConf)
                    .GetInstance<IIMRUResultHandler<TResult>>();
            }
            catch (InjectionException)
            {
                partialTaskConf = TangFactory.GetTang().NewConfigurationBuilder(partialTaskConf)
                    .BindImplementation(GenericType<IIMRUResultHandler<TResult>>.Class,
                        GenericType<DefaultResultHandler<TResult>>.Class)
                    .Build();
                Logger.Log(Level.Info,
                    "User has not given any way to handle IMRU result, defaulting to ignoring it");
            }
            return partialTaskConf;
        }

        /// <summary>
        /// Generate the group communication configuration to be added 
        /// to the tasks
        /// </summary>
        /// <returns>The group communication configuration</returns>
        private IConfiguration GetGroupCommConfiguration()
        {
            var codecConfig =
                TangFactory.GetTang()
                    .NewConfigurationBuilder(
                        StreamingCodecConfiguration<MapInputWithControlMessage<TMapInput>>.Conf.Set(
                            StreamingCodecConfiguration<MapInputWithControlMessage<TMapInput>>.Codec,
                            GenericType<MapInputWithControlMessageCodec<TMapInput>>.Class).Build(),
                        StreamingCodecConfigurationMinusMessage<TMapOutput>.Conf.Build(),
                        _configurationManager.UpdateFunctionCodecsConfiguration)
                    .Build();

            return Configurations.Merge(_groupCommDriver.GetServiceConfiguration(), codecConfig);
        }

        /// <summary>
        /// Adds broadcast and reduce operators to the default communication group
        /// </summary>
        private ICommunicationGroupDriver AddCommunicationGroupWithOperators()
        {
            var reduceFunctionConfig = _configurationManager.ReduceFunctionConfiguration;
            var mapOutputPipelineDataConverterConfig = _configurationManager.MapOutputPipelineDataConverterConfiguration;
            var mapInputPipelineDataConverterConfig = _configurationManager.MapInputPipelineDataConverterConfiguration;

            try
            {
                TangFactory.GetTang()
                    .NewInjector(mapInputPipelineDataConverterConfig)
                    .GetInstance<IPipelineDataConverter<TMapInput>>();

                mapInputPipelineDataConverterConfig =
                    TangFactory.GetTang()
                        .NewConfigurationBuilder(mapInputPipelineDataConverterConfig)
                        .BindImplementation(
                            GenericType<IPipelineDataConverter<MapInputWithControlMessage<TMapInput>>>.Class,
                            GenericType<MapInputwithControlMessagePipelineDataConverter<TMapInput>>.Class)
                        .Build();
            }
            catch (Exception)
            {
                mapInputPipelineDataConverterConfig = TangFactory.GetTang()
                    .NewConfigurationBuilder()
                    .BindImplementation(
                        GenericType<IPipelineDataConverter<MapInputWithControlMessage<TMapInput>>>.Class,
                        GenericType<DefaultPipelineDataConverter<MapInputWithControlMessage<TMapInput>>>.Class)
                    .Build();
            }

            try
            {
                TangFactory.GetTang()
                    .NewInjector(mapOutputPipelineDataConverterConfig)
                    .GetInstance<IPipelineDataConverter<TMapOutput>>();
            }
            catch (Exception)
            {
                mapOutputPipelineDataConverterConfig =
                    TangFactory.GetTang()
                        .NewConfigurationBuilder()
                        .BindImplementation(GenericType<IPipelineDataConverter<TMapOutput>>.Class,
                            GenericType<DefaultPipelineDataConverter<TMapOutput>>.Class)
                        .Build();
            }

            var commGroup =
                _groupCommDriver.NewCommunicationGroup(IMRUConstants.CommunicationGroupName, _totalMappers + 1)
                    .AddBroadcast<MapInputWithControlMessage<TMapInput>>(
                        IMRUConstants.BroadcastOperatorName,
                        _groupCommDriver.MasterTaskId,
                        TopologyTypes.Tree,
                        mapInputPipelineDataConverterConfig)
                    .AddReduce<TMapOutput>(
                        IMRUConstants.ReduceOperatorName,
                        _groupCommDriver.MasterTaskId,
                        TopologyTypes.Tree,
                        reduceFunctionConfig,
                        mapOutputPipelineDataConverterConfig)
                    .Build();

            return commGroup;
        }

        /// <summary>
        /// Construct the stack of map configuration which 
        /// is specific to each mapper. If user does not 
        /// specify any then its empty configuration
        /// </summary>
        /// <param name="totalMappers">Total mappers</param>
        /// <returns>Stack of configuration</returns>
        private ConcurrentStack<IConfiguration> ConstructPerMapperConfigStack(int totalMappers)
        {
            var perMapperConfiguration = new ConcurrentStack<IConfiguration>();
            for (int i = 0; i < totalMappers; i++)
            {
                var emptyConfig = TangFactory.GetTang().NewConfigurationBuilder().Build();
                IConfiguration config = _perMapperConfigs.Aggregate(emptyConfig,
                    (current, configGenerator) =>
                        Configurations.Merge(current, configGenerator.GetMapperConfiguration(i, totalMappers)));
                perMapperConfiguration.Push(config);
            }
            return perMapperConfiguration;
        }
    }
}