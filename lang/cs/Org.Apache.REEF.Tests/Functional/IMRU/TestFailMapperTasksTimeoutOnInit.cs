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
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

using Org.Apache.REEF.Driver.Bridge;
using Org.Apache.REEF.IMRU.API;
using Org.Apache.REEF.IMRU.Examples.PipelinedBroadcastReduce;
using Org.Apache.REEF.IMRU.OnREEF.Driver;
using Org.Apache.REEF.IMRU.OnREEF.Parameters;
using Org.Apache.REEF.Network.Group.Config;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Xunit;
using TestSenderMapFunction = Org.Apache.REEF.IMRU.Examples.PipelinedBroadcastReduce.PipelinedBroadcastAndReduceWithFaultTolerant.SenderMapFunctionFT;

namespace Org.Apache.REEF.Tests.Functional.IMRU
{
    [Collection("FunctionalTests")]
    public sealed class TestFailMapperTasksTimeoutOnInit : TestFailMapperEvaluators
    {
        /// <summary>
        /// This test throws exception in two tasks during task initialization stage. 
        /// Current exception handling code can't distinguish this from communication failure, so job is retried.
        /// </summary>
        [Fact]
        public override void TestFailedMapperOnLocalRuntime()
        {
            int chunkSize = 2;
            int dims = 100;
            int iterations = 200;
            int mapperMemory = 5120;
            int updateTaskMemory = 5120;
            int numTasks = 9;
            string testFolder = DefaultRuntimeFolder + TestId;
            TestBroadCastAndReduce(false,
                numTasks,
                chunkSize,
                dims,
                iterations,
                mapperMemory,
                updateTaskMemory,
                2,
                testFolder);
            string[] lines = ReadLogFile(DriverStdout, "driver", testFolder, 600);
            var completedTaskCount = GetMessageCount(lines, CompletedTaskMessage);
            var failedEvaluatorCount = GetMessageCount(lines, FailedEvaluatorMessage);
            var failedTaskCount = GetMessageCount(lines, FailedTaskMessage);
            var jobSuccess = GetMessageCount(lines, DoneActionMessage);

            // In each retry, there are 1 failed tasks.
            // Rest of the tasks should be canceled and send completed task event to the driver. 
            ////Assert.Equal(NumberOfRetry - 1, failedEvaluatorCount);
            ////Assert.Equal(NumberOfRetry * 2, failedTaskCount);
            ////Assert.True(((NumberOfRetry + 1) * numTasks) - failedTaskCount >= completedTaskCount);
            ////Assert.True((NumberOfRetry * numTasks) - failedTaskCount < completedTaskCount);

            // eventually job succeeds
            Assert.Equal(1, jobSuccess);
            ////CleanUp(testFolder);
        }

        protected override IConfiguration DriverEventHandlerConfigurations<TMapInput, TMapOutput, TResult, TPartitionType>()
        {
            ////return REEF.Driver.DriverConfiguration.ConfigurationModule
            ////    .Set(REEF.Driver.DriverConfiguration.OnEvaluatorAllocated,
            ////        GenericType<IMRUDriver<TMapInput, TMapOutput, TResult, TPartitionType>>.Class)
            ////    .Set(REEF.Driver.DriverConfiguration.OnDriverStarted,
            ////        GenericType<IMRUDriver<TMapInput, TMapOutput, TResult, TPartitionType>>.Class)
            ////    .Set(REEF.Driver.DriverConfiguration.OnContextActive,
            ////        GenericType<IMRUDriver<TMapInput, TMapOutput, TResult, TPartitionType>>.Class)
            ////    .Set(REEF.Driver.DriverConfiguration.OnTaskCompleted,
            ////        GenericType<IMRUDriver<TMapInput, TMapOutput, TResult, TPartitionType>>.Class)
            ////    .Set(REEF.Driver.DriverConfiguration.OnEvaluatorFailed,
            ////        GenericType<IMRUDriver<TMapInput, TMapOutput, TResult, TPartitionType>>.Class)
            ////    .Set(REEF.Driver.DriverConfiguration.OnContextFailed,
            ////        GenericType<IMRUDriver<TMapInput, TMapOutput, TResult, TPartitionType>>.Class)
            ////    .Set(REEF.Driver.DriverConfiguration.OnTaskFailed,
            ////        GenericType<IMRUDriver<TMapInput, TMapOutput, TResult, TPartitionType>>.Class)
            ////    .Set(REEF.Driver.DriverConfiguration.OnTaskRunning,
            ////        GenericType<IMRUDriver<TMapInput, TMapOutput, TResult, TPartitionType>>.Class)
            ////    .Set(REEF.Driver.DriverConfiguration.CustomTraceLevel, TraceLevel.Verbose.ToString())
            ////    .Build();

            return TangFactory.GetTang().NewConfigurationBuilder(base.DriverEventHandlerConfigurations<TMapInput, TMapOutput, TResult, TPartitionType>())
                .BindIntNamedParam<SubmittedTaskTimeoutMs>("10")
                .Build();
        }

        /// <summary>
        /// Mapper function configuration. Subclass can override it to have its own test function.
        /// </summary>
        /// <returns></returns>
        protected override IConfiguration BuildMapperFunctionConfig()
        {
            var c = IMRUMapConfiguration<int[], int[]>.ConfigurationModule
                .Set(IMRUMapConfiguration<int[], int[]>.MapFunction,
                    GenericType<TestSenderMapFunction>.Class)
                .Build();

            return TangFactory.GetTang().NewConfigurationBuilder(c)
                .BindSetEntry<PipelinedBroadcastAndReduceWithFaultTolerant.TaskIdsToFail, string>(GenericType<PipelinedBroadcastAndReduceWithFaultTolerant.TaskIdsToFail>.Class, "IMRUMap-RandomInputPartition-2-")
                .BindIntNamedParam<PipelinedBroadcastAndReduceWithFaultTolerant.FailureType>(PipelinedBroadcastAndReduceWithFaultTolerant.FailureType.TaskHungInInit.ToString())
                .BindNamedParameter(typeof(MaxRetryNumberInRecovery), NumberOfRetry.ToString())
                .BindNamedParameter(typeof(PipelinedBroadcastAndReduceWithFaultTolerant.TotalNumberOfForcedFailures), NumberOfRetry.ToString())
                .BindIntNamedParam<GroupCommConfigurationOptions.RetryCountWaitingForRegistration>("2")
                .Build();
        }
    }
}