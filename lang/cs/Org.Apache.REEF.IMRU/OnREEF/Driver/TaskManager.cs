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
using System.Globalization;
using System.Linq;
using System.Text;
using Org.Apache.REEF.Common;
using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.IMRU.OnREEF.Driver.StateMachine;
using Org.Apache.REEF.Network.Group.Driver;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Utilities.Attributes;
using Org.Apache.REEF.Utilities.Diagnostics;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.IMRU.OnREEF.Driver
{
    /// <summary>
    /// Manages Tasks, maintains task states and responsible for task submission
    /// It will cover the functionality in TaskStarter which will be not used in IMRU driver. 
    /// </summary>
    [NotThreadSafe]
    internal sealed class TaskManager
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(TaskManager));

        internal const string TaskAppError = "TaskAppError";
        internal const string TaskSystemError = "TaskSystemError";
        internal const string TaskGroupCommunicationError = "TaskGroupCommunicationError";
        internal const string TaskEvaluatorError = "TaskEvaluatorError";
        internal const string TaskKilledByDriver = "TaskKilledByDriver";

        private readonly IDictionary<string, Tuple<TaskStateMachine, IConfiguration, IActiveContext>> _tasks 
            = new Dictionary<string, Tuple<TaskStateMachine, IConfiguration, IActiveContext>>();
        private readonly IDictionary<string, IRunningTask> _runingTasks = new Dictionary<string, IRunningTask>();

        private readonly IGroupCommDriver _groupCommDriver;
        private readonly int _totalExpectedTasks;
        private readonly string _masterTaskId;

        /// <summary>
        /// Creates a TaskManager for specified total number of tasks, master task id and associated IGroupCommDriver
        /// Throws IMRUSystemException if numTasks is 0, or masterTaskId or groupCommDriver is null
        /// </summary>
        /// <param name="numTasks"></param>
        /// <param name="masterTaskId"></param>
        /// <param name="groupCommDriver"></param>
        internal TaskManager(int numTasks, string masterTaskId, IGroupCommDriver groupCommDriver)
        {
            if (numTasks == 0)
            {
                Exceptions.Throw(new IMRUSystemException("Number of expected task cannot be 0"), Logger);
            }

            if (masterTaskId == null)
            {
                Exceptions.Throw(new IMRUSystemException("masterTaskId cannot be null"), Logger);
            }

            if (groupCommDriver == null)
            {
                Exceptions.Throw(new IMRUSystemException("groupCommDriver cannot be null"), Logger);
            }

            _totalExpectedTasks = numTasks;
            _masterTaskId = masterTaskId;
            _groupCommDriver = groupCommDriver;
        }

        /// <summary>
        /// Add a Task to the collection
        /// Throws IMRUSystemException in the following cases:
        ///   taskId is already added 
        ///   taskConfiguration is null
        ///   activeContext is null
        ///   trying to add extra tasks
        ///   trying to add Master Task twice
        ///   No Master Task is added in the collection
        /// </summary>
        /// <param name="taskId"></param>
        /// <param name="taskConfiguration"></param>
        /// <param name="activeContext"></param>
        internal void AddTask(string taskId, IConfiguration taskConfiguration, IActiveContext activeContext)
        {
            if (_tasks.ContainsKey(taskId))
            {
                var msg = string.Format(CultureInfo.InvariantCulture, "The task [{0}] already exists.", taskId);
                Exceptions.Throw(new IMRUSystemException(msg), Logger);
            }

            if (taskConfiguration == null)
            {
                Exceptions.Throw(new IMRUSystemException("The task configuration is null."), Logger);
            }

            if (activeContext == null)
            {
                Exceptions.Throw(new IMRUSystemException("The context is null."), Logger);
            }

            if (NumberOfTasks >= _totalExpectedTasks)
            {
                string msg = string.Format("Trying to add an additional Task {0}, but the total expected Task number {1} has been reached.", taskId, _totalExpectedTasks);
                Exceptions.Throw(new IMRUSystemException(msg), Logger);
            }

            if (taskId.Equals(_masterTaskId) && MasterTaskExists())
            {
                string msg = string.Format("Trying to add second master Task {0}.", taskId);
                Exceptions.Throw(new IMRUSystemException(msg), Logger);
            }

            _tasks.Add(taskId, new Tuple<TaskStateMachine, IConfiguration, IActiveContext>(new TaskStateMachine(), taskConfiguration, activeContext));

            if (NumberOfTasks == _totalExpectedTasks && !MasterTaskExists())
            {
                Exceptions.Throw(new IMRUSystemException("There is no master task added."), Logger);
            }
        }

        /// <summary>
        /// Returns the number of tasks in the collection
        /// </summary>
        internal int NumberOfTasks
        {
            get { return _tasks.Count; }
        }

        /// <summary>
        /// Adds the IRunningTask to the RunningTasks and update the task state to TaskRunning
        /// </summary>
        /// <param name="runningTask"></param>
        internal void SetRunningTask(IRunningTask runningTask)
        {
            if (_runingTasks.ContainsKey(runningTask.Id))
            {
                var msg = string.Format(CultureInfo.InvariantCulture, "The task [{0}] already running.", runningTask.Id);
                Exceptions.Throw(new IMRUSystemException(msg), Logger);
            }

            if (!_tasks.ContainsKey(runningTask.Id))
            {
                var msg = string.Format(CultureInfo.InvariantCulture, "The task [{0}] doesn't exist.", runningTask.Id);
                Exceptions.Throw(new IMRUSystemException(msg), Logger);
            }

            _runingTasks.Add(runningTask.Id, runningTask);
            UpdateState(runningTask.Id, TaskStateEvent.RunningTask);
        }

        /// <summary>
        /// clears the task collection
        /// </summary>
        internal void Reset()
        {
            _tasks.Clear();
            _runingTasks.Clear();
        }

        /// <summary>
        /// Removes the task from RunningTasks and change the task state from RunningTask to CompletedTask
        /// </summary>
        /// <param name="taskId"></param>
        internal void SetCompletedTask(string taskId)
        {
            RemoveRunningTask(taskId);
            UpdateState(taskId, TaskStateEvent.CompletedTask);
        }

        /// <summary>
        /// Removes the task from RunningTasks and update the task state to fail
        /// </summary>
        /// <param name="failedTask"></param>
        internal void SetFailedRunningTask(IFailedTask failedTask)
        {
            RemoveRunningTask(failedTask.Id);
            UpdateState(failedTask.Id, GetTaskErrorEvent(failedTask));
        }

        /// <summary>
        /// Task fails during shutting down:
        /// If the task sends failed event because it receives the close command from driver, update the task state to TaskClosedByDriver
        /// Task could fail by communication error or any other application or system error during this time, as long as it is not 
        /// TaskFailedByEvaluatorFailure, update the task state based on the error received. 
        /// </summary>
        /// <param name="failedTask"></param>
        internal void SetFailedTaskInShuttinDown(IFailedTask failedTask)
        {
            if (failedTask.Message.Equals(TaskKilledByDriver) && TaskState(failedTask.Id) == StateMachine.TaskState.TaskWaitingForClose)
            {
                UpdateState(failedTask.Id, TaskStateEvent.ClosedTask);
            }
            else if (TaskState(failedTask.Id) != StateMachine.TaskState.TaskFailedByEvaluatorFailure)
            {
                UpdateState(failedTask.Id, GetTaskErrorEvent(failedTask));
            }
        }

        /// <summary>
        /// Removes the task from RunningTasks if the task is running. 
        /// Sets the task state to TaskFailedByEvaluatorFailure 
        /// </summary>
        /// <param name="taskId"></param>
        internal void SetTaskFailByEvaluator(string taskId)
        {
            if (TaskState(taskId) == StateMachine.TaskState.TaskRunning)
            {
                RemoveRunningTask(taskId);
            }

            if (TaskState(taskId) != StateMachine.TaskState.TaskFailedByEvaluatorFailure)
            {
                UpdateState(taskId, TaskStateEvent.FailedTaskEvaluatorError);
            }
        }

        /// <summary>
        /// Remove a task from RunningTasks
        /// </summary>
        /// <param name="taskId"></param>
        private void RemoveRunningTask(string taskId)
        {
            if (!_runingTasks.ContainsKey(taskId))
            {
                var msg = string.Format(CultureInfo.InvariantCulture, "The task [{0}] is not running.", taskId);
                Exceptions.Throw(new IMRUSystemException(msg), Logger);
            }

            _runingTasks.Remove(taskId);
        }

        /// <summary>
        /// Update task state for a given taskId based on the task event
        /// </summary>
        /// <param name="taskId"></param>
        /// <param name="taskEvent"></param>
        internal void UpdateState(string taskId, TaskStateEvent taskEvent)
        {
            if (!_tasks.ContainsKey(taskId))
            {
                var msg = string.Format(CultureInfo.InvariantCulture, "The task [{0}] doesn't exist.", taskId);
                Exceptions.Throw(new IMRUSystemException(msg), Logger);
            }        
            GetTaskInfo(taskId).Item1.MoveNext(taskEvent);
        }

        /// <summary>
        /// Checks if all the tasks are running.
        /// </summary>
        /// <returns></returns>
        internal bool AreAllTasksAreRunning()
        {
            return _tasks.All(t => t.Value.Item1.CurrentState == StateMachine.TaskState.TaskRunning) &&
                _runingTasks.Count == _totalExpectedTasks;
        }

        /// <summary>
        /// Checks if all the tasks are completed.
        /// </summary>
        /// <returns></returns>
        internal bool AreAllTasksAreCompleted()
        {
            return AllInTheState(StateMachine.TaskState.TaskCompleted) && _runingTasks.Count == 0;
        }

        /// <summary>
        /// This is called when a task failed. Driver tries to close all the rest of the running tasks and clean the running tasks collection in the end.
        /// If all the tasks are running, the total number of running tasks should be _totalExpectedTasks -1
        /// If this happens before all the tasks are running, then the total number of running tasks should smaller than _totalExpectedTasks -1
        /// If this happens when no task is running, the total number of running tasks could be 0
        /// </summary>
        internal void CloseAllRunningTasks(string closeMessage)
        {
            Logger.Log(Level.Info, string.Format(CultureInfo.InvariantCulture, "Closing [{0}] running tasks.", _runingTasks.Count));
            foreach (var t in _runingTasks.Values)
            {
                DisposeRunningTask(t, closeMessage);
            }
            _runingTasks.Clear();
        }

        /// <summary>
        /// Closes a IRunningTask and then updates its status to WaitingTaskToClose
        /// </summary>
        /// <param name="runningTask"></param>
        /// <param name="closeMessage"></param>
        internal void CloseRunningTask(IRunningTask runningTask, string closeMessage)
        {
            DisposeRunningTask(runningTask, closeMessage);
            RemoveRunningTask(runningTask.Id);
        }

        private void DisposeRunningTask(IRunningTask runningTask, string closeMessage)
        {
            if (runningTask == null)
            {
                Exceptions.Throw(new IMRUSystemException("RunningTask is null."), Logger);
            }
            else
            {
                runningTask.Dispose(Encoding.UTF8.GetBytes(closeMessage));
                UpdateState(runningTask.Id, TaskStateEvent.WaitingTaskToClose);
            }
        }

        /// <summary>
        /// Get error type based on the information in IFailedTask 
        /// Currently we use the message in IFailedTask. 
        /// </summary>
        /// <param name="failedTask"></param>
        /// <returns></returns>
        internal TaskStateEvent GetTaskErrorEvent(IFailedTask failedTask)
        {
            var errorMessage = failedTask.Message;
            switch (errorMessage)
            {
                case TaskAppError:
                    return TaskStateEvent.FailedTaskAppError;
                case TaskSystemError:
                    return TaskStateEvent.FailedTaskSystemError;
                case TaskGroupCommunicationError:
                    return TaskStateEvent.FailedTaskCommunicationError;
                default:
                    return TaskStateEvent.FailedTaskSystemError;
            }
        }

        /// <summary>
        /// Checks if all the tasks are in final states
        /// </summary>
        /// <returns></returns>
        internal bool AllInFinalState()
        {
            return _tasks.All(t => t.Value.Item1.IsFinalState());
        }

        /// <summary>
        /// Gets current state of the task
        /// </summary>
        /// <param name="taskId"></param>
        /// <returns></returns>
        internal TaskState TaskState(string taskId)
        {
            if (!_tasks.ContainsKey(taskId))
            {
                var msg = string.Format(CultureInfo.InvariantCulture, "The task [{0}] doesn't exist.", taskId);
                Exceptions.Throw(new IMRUSystemException(msg), Logger);
            }

            var taskInfo = GetTaskInfo(taskId);
            return taskInfo.Item1.CurrentState;
        }

        /// <summary>
        /// Checks if all the tasks are in the state specified. 
        /// For example, passing TaskState.TaskRunning to check if all the tasks are running
        /// </summary>
        /// <param name="taskState"></param>
        /// <returns></returns>
        internal bool AllInTheState(TaskState taskState)
        {
            return _tasks.All(t => t.Value.Item1.CurrentState == taskState);
        }

        /// <summary>
        /// Submit all the tasks
        /// Tasks will be submitted after all the tasks are added in the collection and master task exists
        /// IMRUSystemException will be thrown if not all the tasks are added or if there is no master task
        /// </summary>
        internal void SubmitTasks()
        {
            if (NumberOfTasks < _totalExpectedTasks || !MasterTaskExists())
            {
                string msg = string.Format("Trying to submit tasks but either master task doesn't exist or number of tasks [{0}] is smaller than expected number of tasks [{1}].", NumberOfTasks, _totalExpectedTasks);
                Exceptions.Throw(new IMRUSystemException(msg), Logger);
            }

            StartTask(_masterTaskId);

            foreach (var taskId in _tasks.Keys)
            {
                if (taskId.Equals(_masterTaskId))
                {
                    continue;
                }
                StartTask(taskId);
            }
        }

        /// <summary>
        /// Starts a task and then update the task status to submitted
        /// </summary>
        /// <param name="taskId"></param>
        private void StartTask(string taskId)
        {
            Tuple<TaskStateMachine, IConfiguration, IActiveContext> taskInfo = GetTaskInfo(taskId);

            ////TODO: [REEF-1251]if system state is not SubmittingTasks, taskInfo.Item1.MoveNext(TaskStateEvent.ClosedTask); else do the following
            StartTask(taskId, taskInfo.Item2, taskInfo.Item3);
            taskInfo.Item1.MoveNext(TaskStateEvent.SubmittedTask);
        }

        /// <summary>
        /// Gets GroupCommTaskConfiguration from IGroupCommDriver and merges it with the user partial task configuration.
        /// Then submits the task with the ActiveContext.
        /// </summary>
        /// <param name="taskId"></param>
        /// <param name="userPartialTaskConf"></param>
        /// <param name="activeContext"></param>
        private void StartTask(
            string taskId,
            IConfiguration userPartialTaskConf,
            ITaskSubmittable activeContext)
        {
            var groupCommTaskConfiguration = _groupCommDriver.GetGroupCommTaskConfiguration(taskId);
            var mergedTaskConf = Configurations.Merge(userPartialTaskConf, groupCommTaskConfiguration);
            activeContext.SubmitTask(mergedTaskConf);
        }

        /// <summary>
        /// Checks if master task has been added
        /// </summary>
        /// <returns></returns>
        private bool MasterTaskExists()
        {
            return _tasks.ContainsKey(_masterTaskId);
        }

        /// <summary>
        /// Gets task Tuple based on the given taskId. 
        /// Throws IMRUSystemException if the task Tuple is not in the task collection.
        /// </summary>
        /// <param name="taskId"></param>
        /// <returns></returns>
        private Tuple<TaskStateMachine, IConfiguration, IActiveContext> GetTaskInfo(string taskId)
        {
            Tuple<TaskStateMachine, IConfiguration, IActiveContext> taskInfo;
            _tasks.TryGetValue(taskId, out taskInfo);
            if (taskInfo == null)
            {
                var msg = string.Format(CultureInfo.InvariantCulture, "The task [{0}] does not exist in the task collection.", taskId);
                Exceptions.Throw(new IMRUSystemException(msg), Logger);
            }
            return taskInfo;
        }
    }
}
