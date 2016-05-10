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
using NSubstitute;
using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.IMRU.OnREEF.Driver;
using Org.Apache.REEF.IMRU.OnREEF.Driver.StateMachine;
using Org.Apache.REEF.Network.Group.Driver;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Xunit;
using Xunit.Sdk;

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
        }

        /// <summary>
        /// Tests SubmitTasks after adding all the tasks to the TaskManager
        /// </summary>
        [Fact]
        public void TestSubmitTasks()
        {
            var taskManager = TaskManagerWithTasksAdded();
            taskManager.SubmitTasks();
            Assert.True(taskManager.AllInTheState(TaskState.TaskSubmitted));
        }

        /// <summary>
        /// Test adding/closing running tasks
        /// </summary>
        [Fact]
        public void TestRunningTasks()
        {
            var taskManager = TaskManagerWithTasksAdded();
            taskManager.SubmitTasks();

            taskManager.SetRunningTask(CreateMockRunningTask(MasterTaskId));
            taskManager.SetRunningTask(CreateMockRunningTask(MapperTaskIdPrefix + 1));
            Assert.False(taskManager.AreAllTasksAreRunning());
            var runningTask2 = CreateMockRunningTask(MapperTaskIdPrefix + 2);
            taskManager.SetRunningTask(runningTask2);
            Assert.True(taskManager.AreAllTasksAreRunning());

            taskManager.CloseRunningTask(runningTask2, "Remove task");
            taskManager.CloseAllRunningTasks("Close all");
            Assert.True(taskManager.AllInTheState(TaskState.TaskWaitingForClose));
        }

        /// <summary>
        /// Test set tasks to complete for running tasks
        /// </summary>
        [Fact]
        public void TestCompletingTasks()
        {
            var taskManager = TaskManagerWithTasksAdded();
            taskManager.SubmitTasks();

            taskManager.SetRunningTask(CreateMockRunningTask(MasterTaskId));
            taskManager.SetRunningTask(CreateMockRunningTask(MapperTaskIdPrefix + 1));
            taskManager.SetRunningTask(CreateMockRunningTask(MapperTaskIdPrefix + 2));

            taskManager.SetCompletedTask(MapperTaskIdPrefix + 1);
            taskManager.SetCompletedTask(MapperTaskIdPrefix + 2);
            taskManager.SetCompletedTask(MasterTaskId);
            Assert.True(taskManager.AllInTheState(TaskState.TaskCompleted));
        }

        /// <summary>
        /// Test set tasks to fail for running tasks
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
        /// Test failed tasks in various event sequences
        /// </summary>
        [Fact]
        public void TestFailedTasks()
        {
            var taskManager = TaskManagerWithTasksAdded();
            taskManager.SubmitTasks();

            taskManager.SetRunningTask(CreateMockRunningTask(MasterTaskId));
            taskManager.SetRunningTask(CreateMockRunningTask(MapperTaskIdPrefix + 1));
            taskManager.SetRunningTask(CreateMockRunningTask(MapperTaskIdPrefix + 2));

            //// This task is failed by evaluator failure
            taskManager.SetTaskFailByEvaluator(MapperTaskIdPrefix + 1);
            //// no state change should happen in this case
            taskManager.SetFailedTaskInShuttinDown(CreateMockFailedTask(MapperTaskIdPrefix + 1, TaskManager.TaskSystemError));

            //// This task failed by itself first, then failed by Evaluator failure
            taskManager.SetFailedRunningTask(CreateMockFailedTask(MapperTaskIdPrefix + 2, TaskManager.TaskGroupCommunicationError));
            Assert.Equal(taskManager.TaskState(MapperTaskIdPrefix + 2), TaskState.TaskFailedByGroupCommunication);
            taskManager.SetTaskFailByEvaluator(MapperTaskIdPrefix + 2);
            Assert.Equal(taskManager.TaskState(MapperTaskIdPrefix + 2), TaskState.TaskFailedByEvaluatorFailure);

            //// Close rest of the running tasks
            taskManager.CloseAllRunningTasks("Close all");

            //// receives FailedTask for the closing task
            taskManager.SetFailedTaskInShuttinDown(CreateMockFailedTask(MasterTaskId, TaskManager.TaskKilledByDriver));
            Assert.Equal(taskManager.TaskState(MasterTaskId), TaskState.TaskClosedByDriver);

            Assert.True(taskManager.AllInFinalState());
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
        /// Tests valid update task states
        /// </summary>
        [Fact]
        public void TestUpdateState()
        {
            var taskManager = TaskManagerWithTasksAdded();

            taskManager.UpdateState(MasterTaskId, TaskStateEvent.SubmittedTask);
            Assert.Equal(TaskState.TaskSubmitted, taskManager.TaskState(MasterTaskId));
            taskManager.UpdateState(MapperTaskIdPrefix + 1, TaskStateEvent.SubmittedTask);
            Assert.Equal(TaskState.TaskSubmitted, taskManager.TaskState(MapperTaskIdPrefix + 1));
            taskManager.UpdateState(MapperTaskIdPrefix + 2, TaskStateEvent.ClosedTask);
            Assert.Equal(TaskState.TaskClosedByDriver, taskManager.TaskState(MapperTaskIdPrefix + 2));
        }

        /// <summary>
        /// Tests AllInFinalState
        /// </summary>
        [Fact]
        public void TestAllInFinalState()
        {
            var taskManager = TaskManagerWithTasksAdded();

            taskManager.UpdateState(MasterTaskId, TaskStateEvent.ClosedTask);
            taskManager.UpdateState(MapperTaskIdPrefix + 1, TaskStateEvent.ClosedTask);
            taskManager.UpdateState(MapperTaskIdPrefix + 2, TaskStateEvent.ClosedTask);
            Assert.True(taskManager.AllInFinalState());
        }

        /// <summary>
        /// Tests update state for invalid task id
        /// </summary>
        [Fact]
        public void TestUpdateStateforInvalidTaskId()
        {
            var taskManager = TaskManagerWithTasksAdded();

            Action update = () => taskManager.UpdateState(MapperTaskIdPrefix + 3, TaskStateEvent.SubmittedTask);
            Assert.Throws<IMRUSystemException>(update);
        }

        /// <summary>
        /// Create a TaskManger for testing
        /// </summary>
        /// <returns></returns>
        private TaskManager CreateTaskManager()
        {
            return CreateTaskManager(TotalNumberOfTasks, MasterTaskId, CreateMockGroupDriver());
        }

        private TaskManager CreateTaskManager(int numTasks, string masterTaskId, IGroupCommDriver groupCommDriver)
        {
            var taskManager = new TaskManager(numTasks, masterTaskId, groupCommDriver);
            return taskManager;
        }

        /// <summary>
        /// Create a TaskManager and add one master task and two mapping tasks
        /// </summary>
        /// <returns></returns>
        private TaskManager TaskManagerWithTasksAdded()
        {
            var taskManager = CreateTaskManager();
            taskManager.AddTask(MasterTaskId, MockConfig(), CreateMockActiveContext(0));
            taskManager.AddTask(MapperTaskIdPrefix + 1, MockConfig(), CreateMockActiveContext(1));
            taskManager.AddTask(MapperTaskIdPrefix + 2, MockConfig(), CreateMockActiveContext(2));
            return taskManager;
        }

        /// <summary>
        /// Create a mock IActiveContext
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        private static IActiveContext CreateMockActiveContext(int id)
        {
            IActiveContext mockActiveContext = Substitute.For<IActiveContext>();
            mockActiveContext.Id.Returns(ContextIdPrefix + id);
            mockActiveContext.EvaluatorId.Returns(EvaluatorIdPrefix + ContextIdPrefix + id);
            return mockActiveContext;
        }

        /// <summary>
        /// Create a mock IGroupCommDriver
        /// </summary>
        /// <returns></returns>
        private IGroupCommDriver CreateMockGroupDriver()
        {
            var groupDriver = Substitute.For<IGroupCommDriver>();
            groupDriver.MasterTaskId.Returns(MasterTaskId);
            groupDriver.GetGroupCommTaskConfiguration(MasterTaskId).Returns(MockConfig());
            groupDriver.GetGroupCommTaskConfiguration(MapperTaskIdPrefix + 1).Returns(MockConfig());
            groupDriver.GetGroupCommTaskConfiguration(MapperTaskIdPrefix + 2).Returns(MockConfig());
            return groupDriver;
        }

        private IFailedTask CreateMockFailedTask(string taskId, string errorMsg)
        {
            IFailedTask failedtask = Substitute.For<IFailedTask>();
            failedtask.Id.Returns(taskId);
            failedtask.Message.Returns(errorMsg);
            return failedtask;
        }

        private IRunningTask CreateMockRunningTask(string taskId)
        {
            var runningTask = Substitute.For<IRunningTask>();
            runningTask.Id.Returns(taskId);
            return runningTask;
        }

        /// <summary>
        /// Create a mock IConfiguration
        /// </summary>
        /// <returns></returns>
        private IConfiguration MockConfig()
        {
            return TangFactory.GetTang().NewConfigurationBuilder().Build();
        }
    }
}
