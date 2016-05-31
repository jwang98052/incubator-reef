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
using System.Globalization;
using System.IO;
using System.Linq;
using Org.Apache.REEF.IMRU.API;
using Org.Apache.REEF.IMRU.Examples.PipelinedBroadcastReduce;
using Org.Apache.REEF.IMRU.OnREEF.Driver;
using Org.Apache.REEF.IMRU.OnREEF.Parameters;
using Org.Apache.REEF.IO.PartitionedData.Random;
using Org.Apache.REEF.Network.Examples.GroupCommunication.BroadcastReduceDriverAndTasks;
using Org.Apache.REEF.Network.Group.Config;
using Org.Apache.REEF.Network.Group.Driver;
using Org.Apache.REEF.Network.Group.Driver.Impl;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Diagnostics;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Wake.StreamingCodec.CommonStreamingCodecs;

namespace Org.Apache.REEF.Tests.Functional.IMRU
{
    /// <summary>
    /// IMRU test base class that defines basic configurations for IMRU driver that can be shared by subclasses. 
    /// </summary>
    public abstract class IMRUBrodcastReduceTestBase : ReefFunctionalTest
    {
        protected static readonly Logger Logger = Logger.GetLogger(typeof(IMRUBrodcastReduceTestBase));

        /// <summary>
        /// Abstract method for subclass to override it to provide configurations for driver handlers 
        /// </summary>
        /// <typeparam name="TMapInput"></typeparam>
        /// <typeparam name="TMapOutput"></typeparam>
        /// <typeparam name="TResult"></typeparam>
        /// <typeparam name="TPartitionType"></typeparam>
        /// <returns></returns>
        protected abstract IConfiguration DriverEventHandlerConfigurations<TMapInput, TMapOutput, TResult, TPartitionType>();

        /// <summary>
        /// This method provide a default way to call TestRun. 
        /// It gets driver configurations from base class, including the DriverEventHandlerConfigurations defined by subclass,
        /// then calls TestRun for running the test.
        /// Subclass can override it if they have different parameters for the test
        /// </summary>
        /// <param name="runOnYarn"></param>
        /// <param name="numTasks"></param>
        /// <param name="chunkSize"></param>
        /// <param name="dims"></param>
        /// <param name="iterations"></param>
        /// <param name="mapperMemory"></param>
        /// <param name="updateTaskMemory"></param>
        /// <param name="testFolder"></param>
        protected void TestBroadCastAndReduce(bool runOnYarn, int numTasks, int chunkSize, int dims, int iterations, int mapperMemory, int updateTaskMemory, string testFolder)
        {
            string runPlatform = runOnYarn ? "yarn" : "local";
            TestRun(DriverConfigurations<int[], int[], int[], Stream>(
                CreateIMRUJobDefinitionBuilder(numTasks - 1, chunkSize, iterations, dims, mapperMemory, updateTaskMemory),
                DriverEventHandlerConfigurations<int[], int[], int[], Stream>()),
                typeof(BroadcastReduceDriver),
                numTasks,
                "BroadcastReduceDriver",
                runPlatform,
                testFolder);
        }

        /// <summary>
        /// Build driver configurations
        /// </summary>
        /// <typeparam name="TMapInput"></typeparam>
        /// <typeparam name="TMapOutput"></typeparam>
        /// <typeparam name="TResult"></typeparam>
        /// <typeparam name="TPartitionType"></typeparam>
        /// <param name="jobDefinition"></param>
        /// <param name="driverHandlerConfig"></param>
        /// <returns></returns>
        protected static IConfiguration DriverConfigurations<TMapInput, TMapOutput, TResult, TPartitionType>(IMRUJobDefinition jobDefinition, IConfiguration driverHandlerConfig)
        {
            string driverId = string.Format("IMRU-{0}-Driver", jobDefinition.JobName);
            IConfiguration overallPerMapConfig = null;
            var configurationSerializer = new AvroConfigurationSerializer();

            try
            {
                overallPerMapConfig = Configurations.Merge(jobDefinition.PerMapConfigGeneratorConfig.ToArray());
            }
            catch (Exception e)
            {
                Exceptions.Throw(e, "Issues in merging PerMapCOnfigGenerator configurations", Logger);
            }

            var imruDriverConfiguration = TangFactory.GetTang().NewConfigurationBuilder(new[]
            {
                driverHandlerConfig,
                CreateGroupCommunicationConfigurations<TMapInput, TMapOutput, TResult, TPartitionType>(jobDefinition, driverId),
                jobDefinition.PartitionedDatasetConfiguration,
                overallPerMapConfig
            })
                .BindNamedParameter(typeof(SerializedMapConfiguration),
                    configurationSerializer.ToString(jobDefinition.MapFunctionConfiguration))
                .BindNamedParameter(typeof(SerializedUpdateConfiguration),
                    configurationSerializer.ToString(jobDefinition.UpdateFunctionConfiguration))
                .BindNamedParameter(typeof(SerializedMapInputCodecConfiguration),
                    configurationSerializer.ToString(jobDefinition.MapInputCodecConfiguration))
                .BindNamedParameter(typeof(SerializedMapInputPipelineDataConverterConfiguration),
                    configurationSerializer.ToString(jobDefinition.MapInputPipelineDataConverterConfiguration))
                .BindNamedParameter(typeof(SerializedUpdateFunctionCodecsConfiguration),
                    configurationSerializer.ToString(jobDefinition.UpdateFunctionCodecsConfiguration))
                .BindNamedParameter(typeof(SerializedMapOutputPipelineDataConverterConfiguration),
                    configurationSerializer.ToString(jobDefinition.MapOutputPipelineDataConverterConfiguration))
                .BindNamedParameter(typeof(SerializedReduceConfiguration),
                    configurationSerializer.ToString(jobDefinition.ReduceFunctionConfiguration))
                .BindNamedParameter(typeof(SerializedResultHandlerConfiguration),
                    configurationSerializer.ToString(jobDefinition.ResultHandlerConfiguration))
                .BindNamedParameter(typeof(MemoryPerMapper),
                    jobDefinition.MapperMemory.ToString(CultureInfo.InvariantCulture))
                .BindNamedParameter(typeof(MemoryForUpdateTask),
                    jobDefinition.UpdateTaskMemory.ToString(CultureInfo.InvariantCulture))
                .BindNamedParameter(typeof(CoresPerMapper),
                    jobDefinition.MapTaskCores.ToString(CultureInfo.InvariantCulture))
                .BindNamedParameter(typeof(CoresForUpdateTask),
                    jobDefinition.UpdateTaskCores.ToString(CultureInfo.InvariantCulture))
                .BindNamedParameter(typeof(InvokeGC),
                    jobDefinition.InvokeGarbageCollectorAfterIteration.ToString(CultureInfo.InvariantCulture))
                .Build();
            return imruDriverConfiguration;
        }

        /// <summary>
        /// Create group communication configurations 
        /// </summary>
        /// <typeparam name="TMapInput"></typeparam>
        /// <typeparam name="TMapOutput"></typeparam>
        /// <typeparam name="TResult"></typeparam>
        /// <typeparam name="TPartitionType"></typeparam>
        /// <param name="jobDefinition"></param>
        /// <param name="driverId"></param>
        /// <returns></returns>
        protected static IConfiguration CreateGroupCommunicationConfigurations<TMapInput, TMapOutput, TResult, TPartitionType>(IMRUJobDefinition jobDefinition, string driverId)
        {
            return TangFactory.GetTang().NewConfigurationBuilder()
                .BindStringNamedParam<GroupCommConfigurationOptions.DriverId>(driverId)
                .BindStringNamedParam<GroupCommConfigurationOptions.MasterTaskId>(IMRUConstants.UpdateTaskName)
                .BindStringNamedParam<GroupCommConfigurationOptions.GroupName>(IMRUConstants.CommunicationGroupName)
                .BindIntNamedParam<GroupCommConfigurationOptions.FanOut>(
                    IMRUConstants.TreeFanout.ToString(CultureInfo.InvariantCulture)
                        .ToString(CultureInfo.InvariantCulture))
                .BindIntNamedParam<GroupCommConfigurationOptions.NumberOfTasks>(
                    (jobDefinition.NumberOfMappers + 1).ToString(CultureInfo.InvariantCulture))
                .BindImplementation(GenericType<IGroupCommDriver>.Class, GenericType<GroupCommDriver>.Class)
                .Build();
        }

        /// <summary>
        /// Create IMRU Job Definition with IMRU required configurations
        /// </summary>
        /// <param name="numberofMappers"></param>
        /// <param name="chunkSize"></param>
        /// <param name="numIterations"></param>
        /// <param name="dim"></param>
        /// <param name="mapperMemory"></param>
        /// <param name="updateTaskMemory"></param>
        /// <returns></returns>
        protected static IMRUJobDefinition CreateIMRUJobDefinitionBuilder(int numberofMappers, int chunkSize, int numIterations, int dim, int mapperMemory, int updateTaskMemory)
        {
            var updateFunctionConfig =
                TangFactory.GetTang().NewConfigurationBuilder(IMRUUpdateConfiguration<int[], int[], int[]>.ConfigurationModule
                    .Set(IMRUUpdateConfiguration<int[], int[], int[]>.UpdateFunction,
                        GenericType<BroadcastSenderReduceReceiverUpdateFunction>.Class).Build())
                    .BindNamedParameter(typeof(BroadcastReduceConfiguration.NumberOfIterations),
                        numIterations.ToString(CultureInfo.InvariantCulture))
                    .BindNamedParameter(typeof(BroadcastReduceConfiguration.Dimensions),
                        dim.ToString(CultureInfo.InvariantCulture))
                    .BindNamedParameter(typeof(BroadcastReduceConfiguration.NumWorkers),
                        numberofMappers.ToString(CultureInfo.InvariantCulture))
                    .Build();

            var dataConverterConfig1 =
                TangFactory.GetTang()
                    .NewConfigurationBuilder(IMRUPipelineDataConverterConfiguration<int[]>.ConfigurationModule
                        .Set(IMRUPipelineDataConverterConfiguration<int[]>.MapInputPiplelineDataConverter,
                            GenericType<PipelineIntDataConverter>.Class).Build())
                    .BindNamedParameter(typeof(BroadcastReduceConfiguration.ChunkSize),
                        chunkSize.ToString(CultureInfo.InvariantCulture))
                    .Build();

            var dataConverterConfig2 =
                TangFactory.GetTang()
                    .NewConfigurationBuilder(IMRUPipelineDataConverterConfiguration<int[]>.ConfigurationModule
                        .Set(IMRUPipelineDataConverterConfiguration<int[]>.MapInputPiplelineDataConverter,
                            GenericType<PipelineIntDataConverter>.Class).Build())
                    .BindNamedParameter(typeof(BroadcastReduceConfiguration.ChunkSize),
                        chunkSize.ToString(CultureInfo.InvariantCulture))
                    .Build();

            return new IMRUJobDefinitionBuilder()
                .SetMapFunctionConfiguration(IMRUMapConfiguration<int[], int[]>.ConfigurationModule
                    .Set(IMRUMapConfiguration<int[], int[]>.MapFunction,
                        GenericType<BroadcastReceiverReduceSenderMapFunction>.Class)
                    .Build())
                .SetUpdateFunctionConfiguration(updateFunctionConfig)
                .SetMapInputCodecConfiguration(IMRUCodecConfiguration<int[]>.ConfigurationModule
                    .Set(IMRUCodecConfiguration<int[]>.Codec, GenericType<IntArrayStreamingCodec>.Class)
                    .Build())
                .SetUpdateFunctionCodecsConfiguration(IMRUCodecConfiguration<int[]>.ConfigurationModule
                    .Set(IMRUCodecConfiguration<int[]>.Codec, GenericType<IntArrayStreamingCodec>.Class)
                    .Build())
                .SetReduceFunctionConfiguration(IMRUReduceFunctionConfiguration<int[]>.ConfigurationModule
                    .Set(IMRUReduceFunctionConfiguration<int[]>.ReduceFunction,
                        GenericType<IntArraySumReduceFunction>.Class)
                    .Build())
                .SetMapInputPipelineDataConverterConfiguration(dataConverterConfig1)
                .SetMapOutputPipelineDataConverterConfiguration(dataConverterConfig2)
                .SetPartitionedDatasetConfiguration(
                    RandomInputDataConfiguration.ConfigurationModule.Set(
                        RandomInputDataConfiguration.NumberOfPartitions,
                        numberofMappers.ToString()).Build())
                .SetJobName("IMRUBroadcastReduce")
                .SetNumberOfMappers(numberofMappers)
                .SetMapperMemory(mapperMemory)
                .SetUpdateTaskMemory(updateTaskMemory)
                .Build();
        }
    }
}
