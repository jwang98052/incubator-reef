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
// software distributed under the License is distributed on anAssert.Equal
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

using System;
using NSubstitute;
using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.IMRU.OnREEF.Driver;
using Org.Apache.REEF.IMRU.OnREEF.Driver.StateMachine;
using Org.Apache.REEF.Network.Group.Driver;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Utilities;
using Xunit;

namespace Org.Apache.REEF.IMRU.Tests
{
    /// <summary>
    /// Test cases for TaskManager
    /// </summary>
    public sealed class TestTaskManager
    {
        private const string MapperTaskIdPrefix = "MapperTaskIdPrefix";
        private const string MasterTaskId = "MasterTaskId";
        private const string EvaluatorIdPrefix = "EvaluatorId";
        private const string ContextIdPrefix = "ContextId";
        private const int TotalNumberOfTasks = 3;

        /// <summary>
        /// Tests valid Add task cases
        /// </summary>
        [Fact]
        public void TestValidAdd()
        {
            var taskManager = TaskManagerWithTasksAdded();
            Assert.Equal(TotalNumberOfTasks, taskManager.NumberOfTasks);
        }

        /// <summary>
        /// Tests reset tasks in the TaskManager
        /// </summary>
        [Fact]
        public void TestReset()
        {
            var taskManager = TaskManagerWithTasksAdded();
            taskManager.Reset();
            Assert.Equal(0, taskManager.NumberOfTasks);
            Assert.Equal(0, taskManager.NumberOfAppError());
        }

        /// <summary>
        /// Tests SubmitTasks after adding all the tasks to the TaskManager
        /// </summary>
        [Fact]
        public void TestSubmitTasks()
        {
            var taskManager = TaskManagerWithTasksAdded();
            taskManager.SubmitTasks();
            Assert.True(taskManager.AreAllTasksInState(TaskState.TaskSubmitted));
        }

        /// <summary>
        /// Tests SubmitTask with a missing mapper task
        /// </summary>
        [Fact]
        public void TestMissingMapperTasksSubmit()
        {
            var taskManager = CreateTaskManager();
            taskManager.AddTask(MasterTaskId, MockConfig(), CreateMockActiveContext(0));
            taskManager.AddTask(MapperTaskIdPrefix + 1, MockConfig(), CreateMockActiveContext(1));

            Action submit = () => taskManager.SubmitTasks();
            Assert.Throws<IMRUSystemException>(submit);
        }

        /// <summary>
        /// Tests SubmitTask with missing master task
        /// </summary>
        [Fact]
        public void TestMissingMasterTaskSubmit()
        {
            var taskManager = CreateTaskManager();
            taskManager.AddTask(MapperTaskIdPrefix + 1, MockConfig(), CreateMockActiveContext(1));
            taskManager.AddTask(MapperTaskIdPrefix + 2, MockConfig(), CreateMockActiveContext(2));

            Action submit = () => taskManager.SubmitTasks();
            Assert.Throws<IMRUSystemException>(submit);
        }

        /// <summary>
        /// Tests adding all mapper tasks without master task
        /// </summary>
        [Fact]
        public void NoMasterTask()
        {
            var taskManager = CreateTaskManager();
            taskManager.AddTask(MapperTaskIdPrefix + 1, MockConfig(), CreateMockActiveContext(1));
            taskManager.AddTask(MapperTaskIdPrefix + 2, MockConfig(), CreateMockActiveContext(1));
            Action add = () => taskManager.AddTask(MapperTaskIdPrefix + 3, MockConfig(), CreateMockActiveContext(3));
            Assert.Throws<IMRUSystemException>(add);
        }

        /// <summary>
        /// Tests adding two master tasks
        /// </summary>
        [Fact]
        public void TwoMasterTasks()
        {
            var taskManager = CreateTaskManager();
            taskManager.AddTask(MasterTaskId, MockConfig(), CreateMockActiveContext(0));
            Action add = () => taskManager.AddTask(MasterTaskId, MockConfig(), CreateMockActiveContext(1));
            Assert.Throws<IMRUSystemException>(add);
        }

        /// <summary>
        /// Tests adding more than expected tasks
        /// </summary>
        [Fact]
        public void ExceededTotalNumber()
        {
            var taskManager = TaskManagerWithTasksAdded();
            Action add = () => taskManager.AddTask(MapperTaskIdPrefix + 4, MockConfig(), CreateMockActiveContext(4));
            Assert.Throws<IMRUSystemException>(add);
        }

        /// <summary>
        /// Tests adding a task with duplicated task id
        /// </summary>
        [Fact]
        public void DuplicatedTaskIdInAdd()
        {
            var taskManager = CreateTaskManager();
            taskManager.AddTask(MasterTaskId, MockConfig(), CreateMockActiveContext(0));
            taskManager.AddTask(MapperTaskIdPrefix + 1, MockConfig(), CreateMockActiveContext(1));
            Action add = () => taskManager.AddTask(MapperTaskIdPrefix + 1, MockConfig(), CreateMockActiveContext(1));
            Assert.Throws<IMRUSystemException>(add);
        }

        /// <summary>
        /// Tests invalid arguments when adding tasks
        /// </summary>
        [Fact]
        public void NullArguments()
        {
            var taskManager = CreateTaskManager();
            taskManager.AddTask(MasterTaskId, MockConfig(), CreateMockActiveContext(0));

            Action add = () => taskManager.AddTask(MapperTaskIdPrefix + 1, null, CreateMockActiveContext(1));
            Assert.Throws<IMRUSystemException>(add);

            add = () => taskManager.AddTask(MapperTaskIdPrefix + 2, MockConfig(), null);
            Assert.Throws<IMRUSystemException>(add);
        }

        /// <summary>
        /// Tests passing invalid arguments in creating TaskManager
        /// </summary>
        [Fact]
        public void InvalidArgumentsInCreatingTaskManger()
        {
            Action taskManager = () => CreateTaskManager(0, MasterTaskId, CreateMockGroupDriver());
            Assert.Throws<IMRUSystemException>(taskManager);

            taskManager = () => CreateTaskManager(1, null, CreateMockGroupDriver());
            Assert.Throws<IMRUSystemException>(taskManager);

            taskManager = () => CreateTaskManager(1, MasterTaskId, null);
            Assert.Throws<IMRUSystemException>(taskManager);
        }

        /// <summary>
        /// Tests set tasks to complete for running tasks
        /// </summary>
        [Fact]
        public void TestCompletingTasks()
        {
            var taskManager = TaskManagerWithTasksAdded();
            taskManager.SubmitTasks();

            taskManager.SetRunningTask(CreateMockRunningTask(MasterTaskId));
            taskManager.SetRunningTask(CreateMockRunningTask(MapperTaskIdPrefix + 1));
            taskManager.SetRunningTask(CreateMockRunningTask(MapperTaskIdPrefix + 2));
            Assert.True(taskManager.AreAllTasksInState(TaskState.TaskRunning));

            taskManager.SetCompletedTask(MapperTaskIdPrefix + 1);
            taskManager.SetCompletedTask(MapperTaskIdPrefix + 2);
            taskManager.SetCompletedTask(MasterTaskId);
            Assert.True(taskManager.AreAllTasksInState(TaskState.TaskCompleted));
        }

        /// <summary>
        /// Tests adding/closing running tasks
        /// </summary>
        [Fact]
        public void TestAddingClosingRunningTasks()
        {
            var taskManager = TaskManagerWithTasksAdded();
            taskManager.SubmitTasks();

            taskManager.SetRunningTask(CreateMockRunningTask(MasterTaskId));
            taskManager.SetRunningTask(CreateMockRunningTask(MapperTaskIdPrefix + 1));

            var runningTask2 = CreateMockRunningTask(MapperTaskIdPrefix + 2);
            taskManager.CloseRunningTaskInSystemFailure(runningTask2, TaskManager.CloseTaskByDriver);

            taskManager.CloseAllRunningTasks(TaskManager.CloseTaskByDriver);
            Assert.True(taskManager.AreAllTasksInState(TaskState.TaskWaitingForClose));
        }

        /// <summary>
        /// Tests set tasks to fail for running tasks
        /// </summary>
        [Fact]
        public void TestFailedRunningTasks()
        {
            var taskManager = TaskManagerWithTasksAdded();
            taskManager.SubmitTasks();

            taskManager.SetRunningTask(CreateMockRunningTask(MasterTaskId));
            taskManager.SetRunningTask(CreateMockRunningTask(MapperTaskIdPrefix + 1));
            taskManager.SetRunningTask(CreateMockRunningTask(MapperTaskIdPrefix + 2));

            taskManager.SetFailedRunningTask(CreateMockFailedTask(MapperTaskIdPrefix + 1, TaskManager.TaskAppError));
            taskManager.SetFailedRunningTask(CreateMockFailedTask(MapperTaskIdPrefix + 2, TaskManager.TaskGroupCommunicationError));
            taskManager.SetFailedRunningTask(CreateMockFailedTask(MasterTaskId, TaskManager.TaskSystemError));
            Assert.True(taskManager.AllInFinalState());
        }

        /// <summary>
        /// Tests number of application errors 
        /// </summary>
        [Fact]
        public void TestAppError()
        {
            var taskManager = TaskManagerWithTasksAdded();
            taskManager.SubmitTasks();
            taskManager.SetFailedRunningTask(CreateMockFailedTask(MapperTaskIdPrefix + 1, TaskManager.TaskAppError));
            taskManager.SetFailedRunningTask(CreateMockFailedTask(MapperTaskIdPrefix + 2, TaskManager.TaskSystemError));
            Assert.Equal(1, taskManager.NumberOfAppError());
        }

        /// <summary>
        /// Tests failed tasks in various event sequences
        /// </summary>
        [Fact]
        public void TestFailedTasks()
        {
            var taskManager = TaskManagerWithTasksAdded();
            taskManager.SubmitTasks();

            taskManager.SetRunningTask(CreateMockRunningTask(MapperTaskIdPrefix + 1));
            taskManager.SetRunningTask(CreateMockRunningTask(MapperTaskIdPrefix + 2));

            //// This task failed by evaluator then failed by itself
            var failedTask1 = CreateMockFailedTask(MapperTaskIdPrefix + 1, TaskManager.TaskSystemError);
            taskManager.SetTaskFailByEvaluator(CreateMockFailedEvaluator("eId1", failedTask1));
            Assert.Equal(TaskState.TaskFailedByEvaluatorFailure, taskManager.TaskState(MapperTaskIdPrefix + 1));
            //// no state change should happen in this case
            taskManager.SetFailedTaskInShuttingDown(failedTask1);
            Assert.Equal(TaskState.TaskFailedByEvaluatorFailure, taskManager.TaskState(MapperTaskIdPrefix + 1));

            //// This task failed by itself first, then failed by Evaluator failure
            var failedTask2 = CreateMockFailedTask(MapperTaskIdPrefix + 2, TaskManager.TaskGroupCommunicationError);
            taskManager.SetFailedRunningTask(failedTask2);
            Assert.Equal(TaskState.TaskFailedByGroupCommunication, taskManager.TaskState(MapperTaskIdPrefix + 2));
            taskManager.SetTaskFailByEvaluator(CreateMockFailedEvaluator("eId2", failedTask2));
            Assert.Equal(TaskState.TaskFailedByEvaluatorFailure, taskManager.TaskState(MapperTaskIdPrefix + 2));

            //// close the running task during shutting down
            var masterRuningTask = CreateMockRunningTask(MasterTaskId);
            taskManager.CloseRunningTaskInSystemFailure(masterRuningTask, TaskManager.CloseTaskByDriver);
            taskManager.SetFailedTaskInShuttingDown(CreateMockFailedTask(MasterTaskId, TaskManager.TaskKilledByDriver));
            Assert.Equal(TaskState.TaskClosedByDriver, taskManager.TaskState(MasterTaskId));

            Assert.True(taskManager.AllInFinalState());
        }

        /// <summary>
        /// Tests after all the tasks are running, a task fails first, then close all running tasks
        /// </summary>
        [Fact]
        public void TestFailedTasksAfterAllTasksAreRunnigScenario()
        {
            var taskManager = TaskManagerWithTasksAdded();
            taskManager.SubmitTasks();

            taskManager.SetRunningTask(CreateMockRunningTask(MasterTaskId));
            taskManager.SetRunningTask(CreateMockRunningTask(MapperTaskIdPrefix + 1));
            taskManager.SetRunningTask(CreateMockRunningTask(MapperTaskIdPrefix + 2));

            //// A task fail first
            var failedTask1 = CreateMockFailedTask(MapperTaskIdPrefix + 1, TaskManager.TaskSystemError);
            taskManager.SetFailedRunningTask(failedTask1);
            Assert.Equal(TaskState.TaskFailedBySystemError, taskManager.TaskState(MapperTaskIdPrefix + 1));

            //// system is in shutting down, close all other tasks
            taskManager.CloseAllRunningTasks(TaskManager.CloseTaskByDriver);

            //// task 2 is killed by driver
            var failedTask2 = CreateMockFailedTask(MapperTaskIdPrefix + 2, TaskManager.TaskKilledByDriver);
            taskManager.SetFailedTaskInShuttingDown(failedTask2);

            //// master task is killed by driver
            var masterTask = CreateMockFailedTask(MasterTaskId, TaskManager.TaskKilledByDriver);
            taskManager.SetFailedTaskInShuttingDown(masterTask);

            Assert.True(taskManager.AllInFinalState());
        }

        /// <summary>
        /// Tests after all the tasks are running, an evaluator fails first, then a task fails with communication error
        /// </summary>
        [Fact]
        public void TestFailedEvaluatorThenFailedTaskAfterTasksAreRunningScenario()
        {
            var taskManager = TaskManagerWithTasksAdded();
            taskManager.SubmitTasks();

            taskManager.SetRunningTask(CreateMockRunningTask(MasterTaskId));
            taskManager.SetRunningTask(CreateMockRunningTask(MapperTaskIdPrefix + 1));
            taskManager.SetRunningTask(CreateMockRunningTask(MapperTaskIdPrefix + 2));

            //// Evaluator error
            var failedTask1 = CreateMockFailedTask(MapperTaskIdPrefix + 1, TaskManager.TaskSystemError);
            taskManager.SetTaskFailByEvaluator(CreateMockFailedEvaluator("eId1", failedTask1));
            Assert.Equal(TaskState.TaskFailedByEvaluatorFailure, taskManager.TaskState(MapperTaskIdPrefix + 1));

            //// system is in shutting down, close all other tasks
            taskManager.CloseAllRunningTasks(TaskManager.CloseTaskByDriver);
            Assert.Equal(TaskState.TaskWaitingForClose, taskManager.TaskState(MapperTaskIdPrefix + 2));
            Assert.Equal(TaskState.TaskWaitingForClose, taskManager.TaskState(MasterTaskId));

            //// Another task may get failed by communication during the shutting down
            var failedTask2 = CreateMockFailedTask(MapperTaskIdPrefix + 2, TaskManager.TaskGroupCommunicationError);
            taskManager.SetFailedTaskInShuttingDown(failedTask2);
            Assert.Equal(TaskState.TaskClosedByDriver, taskManager.TaskState(MapperTaskIdPrefix + 2));

            //// The task that receives the close from driver now send failed event back to driver
            var masterTask = CreateMockFailedTask(MasterTaskId, TaskManager.TaskKilledByDriver);
            taskManager.SetFailedTaskInShuttingDown(masterTask);
            Assert.Equal(TaskState.TaskClosedByDriver, taskManager.TaskState(MasterTaskId));

            Assert.True(taskManager.AllInFinalState());
        }

        /// <summary>
        /// Tests after all the tasks are running, a task fails first, then an evaluator fails
        /// </summary>
        [Fact]
        public void TestFailedTasksThenFailedEvaluatorAfterAllTasksAreRunningScenario()
        {
            var taskManager = TaskManagerWithTasksAdded();
            taskManager.SubmitTasks();

            taskManager.SetRunningTask(CreateMockRunningTask(MasterTaskId));
            taskManager.SetRunningTask(CreateMockRunningTask(MapperTaskIdPrefix + 1));
            taskManager.SetRunningTask(CreateMockRunningTask(MapperTaskIdPrefix + 2));

            //// A task fails first
            var failedTask1 = CreateMockFailedTask(MapperTaskIdPrefix + 1, TaskManager.TaskSystemError);
            taskManager.SetFailedRunningTask(failedTask1);
            Assert.Equal(TaskState.TaskFailedBySystemError, taskManager.TaskState(MapperTaskIdPrefix + 1));

            //// system is in shutting down, close all other tasks
            taskManager.CloseAllRunningTasks(TaskManager.CloseTaskByDriver);

            //// An Evaluator fails during shut down, as the task is already in waiting for close state, its state will be changed to TaskClosedByDriver
            var failedTask2 = CreateMockFailedTask(MapperTaskIdPrefix + 2, TaskManager.TaskSystemError);
            taskManager.SetTaskFailByEvaluator(CreateMockFailedEvaluator("eId2", failedTask2));
            Assert.Equal(TaskState.TaskClosedByDriver, taskManager.TaskState(MapperTaskIdPrefix + 2));

            //// master task gets communication error before it receives close event, as the task is already in waiting for close state, its state will be changed to TaskClosedByDriver
            var masterTask = CreateMockFailedTask(MasterTaskId, TaskManager.TaskGroupCommunicationError);
            taskManager.SetFailedTaskInShuttingDown(masterTask);
            Assert.Equal(TaskState.TaskClosedByDriver, taskManager.TaskState(MasterTaskId));

            Assert.True(taskManager.AllInFinalState());
        }

        /// <summary>
        /// Test evaluator fails before any task is running after all the tasks are submitted
        /// </summary>
        [Fact]
        public void TestFailedEvaluatorBeforeAnyTaskIsRunningScenario()
        {
            var taskManager = TaskManagerWithTasksAdded();
            taskManager.SubmitTasks();

            //// Evaluator error
            var failedTask1 = CreateMockFailedTask(MapperTaskIdPrefix + 1, TaskManager.TaskSystemError);
            taskManager.SetTaskFailByEvaluator(CreateMockFailedEvaluator("eId1", failedTask1));
            Assert.Equal(TaskState.TaskFailedByEvaluatorFailure, taskManager.TaskState(MapperTaskIdPrefix + 1));

            //// there is no any running task yet
            taskManager.CloseAllRunningTasks(TaskManager.CloseTaskByDriver);

            //// task2 is running , close it
            var runingTask2 = CreateMockRunningTask(MapperTaskIdPrefix + 2);
            taskManager.CloseRunningTaskInSystemFailure(runingTask2, TaskManager.CloseTaskByDriver);
            Assert.Equal(TaskState.TaskWaitingForClose, taskManager.TaskState(MapperTaskIdPrefix + 2));

            //// master task is running, close it
            var masterTask = CreateMockRunningTask(MasterTaskId);
            taskManager.CloseRunningTaskInSystemFailure(masterTask, TaskManager.CloseTaskByDriver);
            Assert.Equal(TaskState.TaskWaitingForClose, taskManager.TaskState(MasterTaskId));

            //// received task failure because of the closing
            var failedTask2 = CreateMockFailedTask(MapperTaskIdPrefix + 2, TaskManager.TaskKilledByDriver);
            taskManager.SetFailedTaskInShuttingDown(failedTask2);

            //// received task failure because of the closing
            var failedMasterTask = CreateMockFailedTask(MasterTaskId, TaskManager.TaskKilledByDriver);
            taskManager.SetFailedTaskInShuttingDown(failedMasterTask);

            Assert.True(taskManager.AllInFinalState());
        }

        /// <summary>
        /// Tests an evaluator fails for a running task before all the tasks are running
        /// </summary>
        [Fact]
        public void TestFailedEvaluatorOnRunningTaskBeforeAllTasksAreRunningScenario()
        {
            var taskManager = TaskManagerWithTasksAdded();
            taskManager.SubmitTasks();

            taskManager.SetRunningTask(CreateMockRunningTask(MasterTaskId));
            taskManager.SetRunningTask(CreateMockRunningTask(MapperTaskIdPrefix + 1));

            //// Evaluator error
            var failedTask1 = CreateMockFailedTask(MapperTaskIdPrefix + 1, TaskManager.TaskSystemError);
            taskManager.SetTaskFailByEvaluator(CreateMockFailedEvaluator("eId1", failedTask1));
            Assert.Equal(TaskState.TaskFailedByEvaluatorFailure, taskManager.TaskState(MapperTaskIdPrefix + 1));

            //// the master task should be closed
            taskManager.CloseAllRunningTasks(TaskManager.CloseTaskByDriver);

            //// task 2 is now running, close it
            var runingTask2 = CreateMockRunningTask(MapperTaskIdPrefix + 2);
            taskManager.CloseRunningTaskInSystemFailure(runingTask2, TaskManager.CloseTaskByDriver);
            Assert.Equal(TaskState.TaskWaitingForClose, taskManager.TaskState(MapperTaskIdPrefix + 2));

            var failedTask2 = CreateMockFailedTask(MapperTaskIdPrefix + 2, TaskManager.TaskKilledByDriver);
            taskManager.SetFailedTaskInShuttingDown(failedTask2);

            var failedMasterTask = CreateMockFailedTask(MasterTaskId, TaskManager.TaskKilledByDriver);
            taskManager.SetFailedTaskInShuttingDown(failedMasterTask);

            Assert.True(taskManager.AllInFinalState());
        }

        /// <summary>
        /// Tests an evaluator fails for a non running task before all the tasks are running
        /// </summary>
        [Fact]
        public void TestFailedEvaluatorOnNoRunningTaskBeforeAllTasksAreRunningScenario()
        {
            var taskManager = TaskManagerWithTasksAdded();
            taskManager.SubmitTasks();

            taskManager.SetRunningTask(CreateMockRunningTask(MasterTaskId));
            taskManager.SetRunningTask(CreateMockRunningTask(MapperTaskIdPrefix + 1));

            //// Evaluator error
            var failedTask2 = CreateMockFailedTask(MapperTaskIdPrefix + 2, TaskManager.TaskSystemError);
            taskManager.SetTaskFailByEvaluator(CreateMockFailedEvaluator("eId2", failedTask2));
            Assert.Equal(TaskState.TaskFailedByEvaluatorFailure, taskManager.TaskState(MapperTaskIdPrefix + 2));

            //// Send event to close master task and task1
            taskManager.CloseAllRunningTasks(TaskManager.CloseTaskByDriver);

            var failedTask1 = CreateMockFailedTask(MapperTaskIdPrefix + 1, TaskManager.TaskKilledByDriver);
            taskManager.SetFailedTaskInShuttingDown(failedTask1);

            var failedMasterTask = CreateMockFailedTask(MasterTaskId, TaskManager.TaskKilledByDriver);
            taskManager.SetFailedTaskInShuttingDown(failedMasterTask);

            Assert.True(taskManager.AllInFinalState());
        }

        /// <summary>
        /// Tests a task fails before any task is running after all the tasks are submitted.
        /// </summary>
        [Fact]
        public void TestFailedTaskBeforeAnyTaskIsRunningScenario()
        {
            var taskManager = TaskManagerWithTasksAdded();
            taskManager.SubmitTasks();

            //// Evaluator error
            var failedTask1 = CreateMockFailedTask(MapperTaskIdPrefix + 1, TaskManager.TaskSystemError);
            taskManager.SetFailedRunningTask(failedTask1);
            Assert.Equal(TaskState.TaskFailedBySystemError, taskManager.TaskState(MapperTaskIdPrefix + 1));

            //// there is no any running task yet
            taskManager.CloseAllRunningTasks(TaskManager.CloseTaskByDriver);

            //// task 2 is running, now close it
            var runingTask2 = CreateMockRunningTask(MapperTaskIdPrefix + 2);
            taskManager.CloseRunningTaskInSystemFailure(runingTask2, TaskManager.CloseTaskByDriver);
            Assert.Equal(TaskState.TaskWaitingForClose, taskManager.TaskState(MapperTaskIdPrefix + 2));

            //// master task is running, close it
            var masterTask = CreateMockRunningTask(MasterTaskId);
            taskManager.CloseRunningTaskInSystemFailure(masterTask, TaskManager.CloseTaskByDriver);
            Assert.Equal(TaskState.TaskWaitingForClose, taskManager.TaskState(MasterTaskId));

            //// The task 2 could be failed by communication before receiving close event
            var failedTask2 = CreateMockFailedTask(MapperTaskIdPrefix + 2, TaskManager.TaskGroupCommunicationError);
            taskManager.SetFailedTaskInShuttingDown(failedTask2);

            //// master task failed because receiving close event
            var failedMasterTask = CreateMockFailedTask(MasterTaskId, TaskManager.TaskKilledByDriver);
            taskManager.SetFailedTaskInShuttingDown(failedMasterTask);

            Assert.True(taskManager.AllInFinalState());
        }

        /// <summary>
        /// Tests a running task fails before all the tasks are running
        /// </summary>
        [Fact]
        public void TestFailedRunningTaskBeforeAllTasksAreRunningScenario()
        {
            var taskManager = TaskManagerWithTasksAdded();
            taskManager.SubmitTasks();

            taskManager.SetRunningTask(CreateMockRunningTask(MapperTaskIdPrefix + 1));

            //// Evaluator error
            var failedTask1 = CreateMockFailedTask(MapperTaskIdPrefix + 1, TaskManager.TaskSystemError);
            taskManager.SetFailedRunningTask(failedTask1);
            Assert.Equal(TaskState.TaskFailedBySystemError, taskManager.TaskState(MapperTaskIdPrefix + 1));

            //// there is no any running task yet
            taskManager.CloseAllRunningTasks(TaskManager.CloseTaskByDriver);

            //// task 2 is running, now close it
            var runingTask2 = CreateMockRunningTask(MapperTaskIdPrefix + 2);
            taskManager.CloseRunningTaskInSystemFailure(runingTask2, TaskManager.CloseTaskByDriver);
            Assert.Equal(TaskState.TaskWaitingForClose, taskManager.TaskState(MapperTaskIdPrefix + 2));

            //// master task is running, close it
            var masterTask = CreateMockRunningTask(MasterTaskId);
            taskManager.CloseRunningTaskInSystemFailure(masterTask, TaskManager.CloseTaskByDriver);
            Assert.Equal(TaskState.TaskWaitingForClose, taskManager.TaskState(MasterTaskId));

            //// The task 2 could be failed by communication before receiving close event
            var failedTask2 = CreateMockFailedTask(MapperTaskIdPrefix + 2, TaskManager.TaskGroupCommunicationError);
            taskManager.SetFailedTaskInShuttingDown(failedTask2);

            //// master task failed because receiving close event
            var failedMasterTask = CreateMockFailedTask(MasterTaskId, TaskManager.TaskKilledByDriver);
            taskManager.SetFailedTaskInShuttingDown(failedMasterTask);

            Assert.True(taskManager.AllInFinalState());
        }

        /// <summary>
        /// Tests a non running task fails before all the tasks are running
        /// </summary>
        [Fact]
        public void TestFailedNoRunningTaskBeforeAllTasksAreRunningScenario()
        {
            var taskManager = TaskManagerWithTasksAdded();
            taskManager.SubmitTasks();

            taskManager.SetRunningTask(CreateMockRunningTask(MapperTaskIdPrefix + 1));

            //// Evaluator error
            var failedTask2 = CreateMockFailedTask(MapperTaskIdPrefix + 2, TaskManager.TaskSystemError);
            taskManager.SetFailedRunningTask(failedTask2);
            Assert.Equal(TaskState.TaskFailedBySystemError, taskManager.TaskState(MapperTaskIdPrefix + 2));

            //// there is no any running task yet
            taskManager.CloseAllRunningTasks(TaskManager.CloseTaskByDriver);

            //// master task is running, close it
            var masterTask = CreateMockRunningTask(MasterTaskId);
            taskManager.CloseRunningTaskInSystemFailure(masterTask, TaskManager.CloseTaskByDriver);
            Assert.Equal(TaskState.TaskWaitingForClose, taskManager.TaskState(MasterTaskId));

            //// The task 1 could be failed by communication before receiving close event
            var failedTask1 = CreateMockFailedTask(MapperTaskIdPrefix + 1, TaskManager.TaskGroupCommunicationError);
            taskManager.SetFailedTaskInShuttingDown(failedTask1);

            //// master task failed could be failed by communication error as well
            var failedMasterTask = CreateMockFailedTask(MasterTaskId, TaskManager.TaskGroupCommunicationError);
            taskManager.SetFailedTaskInShuttingDown(failedMasterTask);

            Assert.True(taskManager.AllInFinalState());
        }

        /// <summary>
        /// Creates a TaskManger for testing with defined constants in the class
        /// </summary>
        /// <returns></returns>
        private static TaskManager CreateTaskManager()
        {
            return CreateTaskManager(TotalNumberOfTasks, MasterTaskId, CreateMockGroupDriver());
        }

        /// <summary>
        /// Creates a TaskManager with specified numTasks, masterTaskId and IGroupCommDriver
        /// </summary>
        /// <param name="numTasks"></param>
        /// <param name="masterTaskId"></param>
        /// <param name="groupCommDriver"></param>
        /// <returns></returns>
        private static TaskManager CreateTaskManager(int numTasks, string masterTaskId, IGroupCommDriver groupCommDriver)
        {
            var taskManager = new TaskManager(numTasks, masterTaskId, groupCommDriver);
            return taskManager;
        }

        /// <summary>
        /// Creates a TaskManager and add one master task and two mapping tasks
        /// </summary>
        /// <returns></returns>
        private static TaskManager TaskManagerWithTasksAdded()
        {
            var taskManager = CreateTaskManager();
            taskManager.AddTask(MasterTaskId, MockConfig(), CreateMockActiveContext(0));
            taskManager.AddTask(MapperTaskIdPrefix + 1, MockConfig(), CreateMockActiveContext(1));
            taskManager.AddTask(MapperTaskIdPrefix + 2, MockConfig(), CreateMockActiveContext(2));
            return taskManager;
        }

        /// <summary>
        /// Creates a mock IActiveContext
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        private static IActiveContext CreateMockActiveContext(int id)
        {
            var mockActiveContext = Substitute.For<IActiveContext>();
            mockActiveContext.Id.Returns(ContextIdPrefix + id);
            mockActiveContext.EvaluatorId.Returns(EvaluatorIdPrefix + ContextIdPrefix + id);
            return mockActiveContext;
        }

        /// <summary>
        /// Creates a mock IGroupCommDriver
        /// </summary>
        /// <returns></returns>
        private static IGroupCommDriver CreateMockGroupDriver()
        {
            var groupDriver = Substitute.For<IGroupCommDriver>();
            groupDriver.MasterTaskId.Returns(MasterTaskId);
            groupDriver.GetGroupCommTaskConfiguration(MasterTaskId).Returns(MockConfig());
            groupDriver.GetGroupCommTaskConfiguration(MapperTaskIdPrefix + 1).Returns(MockConfig());
            groupDriver.GetGroupCommTaskConfiguration(MapperTaskIdPrefix + 2).Returns(MockConfig());
            return groupDriver;
        }

        /// <summary>
        /// Creates a mock FailedTask with specified taskId and error message
        /// </summary>
        /// <param name="taskId"></param>
        /// <param name="errorMsg"></param>
        /// <returns></returns>
        private static IFailedTask CreateMockFailedTask(string taskId, string errorMsg)
        {
            IFailedTask failedtask = Substitute.For<IFailedTask>();
            failedtask.Id.Returns(taskId);
            failedtask.Message.Returns(errorMsg);
            return failedtask;
        }

        /// <summary>
        /// Creates a mock running task with the taskId specified
        /// </summary>
        /// <param name="taskId"></param>
        /// <returns></returns>
        private static IRunningTask CreateMockRunningTask(string taskId)
        {
            var runningTask = Substitute.For<IRunningTask>();
            runningTask.Id.Returns(taskId);
            return runningTask;
        }

        /// <summary>
        /// Creates a mock IFailedEvaluator with the specified IFailedTask associated
        /// </summary>
        /// <param name="evaluatorId"></param>
        /// <param name="failedTask"></param>
        /// <returns></returns>
        private static IFailedEvaluator CreateMockFailedEvaluator(string evaluatorId, IFailedTask failedTask)
        {
            var failedEvalutor = Substitute.For<IFailedEvaluator>();
            failedEvalutor.Id.Returns(evaluatorId);
            failedEvalutor.FailedTask.Returns(Optional<IFailedTask>.Of(failedTask));
            return failedEvalutor;
        }

        /// <summary>
        /// Creates a mock IConfiguration
        /// </summary>
        /// <returns></returns>
        private static IConfiguration MockConfig()
        {
            return TangFactory.GetTang().NewConfigurationBuilder().Build();
        }
    }
}