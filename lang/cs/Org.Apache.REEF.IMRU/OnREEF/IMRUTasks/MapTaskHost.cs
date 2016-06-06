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

using System;
using System.Text;
using System.Threading;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Common.Tasks.Events;
using Org.Apache.REEF.IMRU.API;
using Org.Apache.REEF.IMRU.OnREEF.Driver;
using Org.Apache.REEF.IMRU.OnREEF.MapInputWithControlMessage;
using Org.Apache.REEF.IMRU.OnREEF.Parameters;
using Org.Apache.REEF.Network.Group.Operators;
using Org.Apache.REEF.Network.Group.Task;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Attributes;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.IMRU.OnREEF.IMRUTasks
{
    /// <summary>
    /// Hosts the IMRU MapTask in a REEF Task.
    /// </summary>
    /// <typeparam name="TMapInput">Map input</typeparam>
    /// <typeparam name="TMapOutput">Map output</typeparam>
    [ThreadSafe]
    internal sealed class MapTaskHost<TMapInput, TMapOutput> : ITask, IObserver<ICloseEvent>
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(MapTaskHost<TMapInput, TMapOutput>));

        private readonly IBroadcastReceiver<MapInputWithControlMessage<TMapInput>> _dataAndMessageReceiver;
        private readonly IReduceSender<TMapOutput> _dataReducer;
        private readonly IMapFunction<TMapInput, TMapOutput> _mapTask;
        private readonly bool _invokeGC;

        /// <summary>
        /// When receiving a close event, this variable is set to 1. At the beginning of each task iteration,
        /// if this variable is set to 1, the task will break from the loop and return from the Call() method.
        /// </summary>
        private long _shouldCloseTask = 0;

        /// <summary>
        /// Before the task is returned, this variable is set to 1.
        /// Close handler will check this variable to decide if it needs to throw an exception.
        /// </summary>
        private long _isTaskStopped = 0;

        /// <summary>
        /// Shows if the object has been disposed.
        /// </summary>
        private long _disposed = 0;

        /// <summary>
        /// Waiting time for the task to close by itself
        /// </summary>
        private readonly int _enforceCloseTimeoutMilliseconds;

        /// <summary>
        /// An event that will wait in close handler until it is either signaled from Call method or timeout.
        /// </summary>
        private readonly ManualResetEventSlim _waitToCloseEvent = new ManualResetEventSlim(false);

        /// <summary>
        /// Group Communication client for the task
        /// </summary>
        private readonly IGroupCommClient _groupCommunicationsClient;

        /// <summary>
        /// </summary>
        /// <param name="mapTask">The MapTask hosted in this REEF Task.</param>
        /// <param name="groupCommunicationsClient">Used to setup the communications.</param>
        /// <param name="enforceCloseTimeoutMilliseconds">Timeout to enforce the task to close if receiving task close event</param>
        /// <param name="invokeGC">Whether to call Garbage Collector after each iteration or not</param>
        [Inject]
        private MapTaskHost(
            IMapFunction<TMapInput, TMapOutput> mapTask,
            IGroupCommClient groupCommunicationsClient,
            [Parameter(typeof(EnforceCloseTimeoutMilliseconds))] int enforceCloseTimeoutMilliseconds,
            [Parameter(typeof(InvokeGC))] bool invokeGC)
        {
            _mapTask = mapTask;
            _groupCommunicationsClient = groupCommunicationsClient;
            var cg = groupCommunicationsClient.GetCommunicationGroup(IMRUConstants.CommunicationGroupName);
            _dataAndMessageReceiver =
                cg.GetBroadcastReceiver<MapInputWithControlMessage<TMapInput>>(IMRUConstants.BroadcastOperatorName);
            _dataReducer = cg.GetReduceSender<TMapOutput>(IMRUConstants.ReduceOperatorName);
            _invokeGC = invokeGC;
            _enforceCloseTimeoutMilliseconds = enforceCloseTimeoutMilliseconds;
        }

        /// <summary>
        /// Performs IMRU iterations on map side
        /// </summary>
        /// <param name="memento"></param>
        /// <returns></returns>
        public byte[] Call(byte[] memento)
        {
            while (Interlocked.Read(ref _shouldCloseTask) == 0)
            {
                if (_invokeGC)
                {
                    Logger.Log(Level.Verbose, "Calling Garbage Collector");
                    GC.Collect();
                    GC.WaitForPendingFinalizers();
                }

                TMapOutput result;

                using (
                    MapInputWithControlMessage<TMapInput> mapInput = _dataAndMessageReceiver.Receive())
                {
                    if (mapInput.ControlMessage == MapControlMessage.Stop)
                    {
                        break;
                    }
                    result = _mapTask.Map(mapInput.Message);
                }
             
                _dataReducer.Send(result);
            }

            Interlocked.Exchange(ref _isTaskStopped, 1);

            if (Interlocked.Read(ref _shouldCloseTask) == 1)
            {
                _waitToCloseEvent.Set();
            }
            return null;
        }

        /// <summary>
        /// Task close handler.
        /// If the closed event is sent from driver, set _shouldCloseTask to 1 so that to inform the Call() to stop at the end of the current iteration.
        /// Then waiting for the signal from Call method. Either it is signaled or after _enforceCloseTimeoutMilliseconds,
        /// checks if the task has been stopped. If not, throw IMRUTaskSystemException to enforce the task to stop.
        /// </summary>
        /// <param name="closeEvent"></param>
        public void OnNext(ICloseEvent closeEvent)
        {
            var msg = Encoding.UTF8.GetString(closeEvent.Value.Value);
            if (closeEvent.Value.IsPresent() && msg.Equals(TaskManager.CloseTaskByDriver))
            {
                Logger.Log(Level.Info, "The task received close event with message: {0}.", msg);
                Interlocked.Exchange(ref _shouldCloseTask, 1);

                _waitToCloseEvent.Wait(TimeSpan.FromMilliseconds(_enforceCloseTimeoutMilliseconds));

                if (Interlocked.Read(ref _isTaskStopped) == 0)
                {
                    throw new IMRUTaskSystemException(TaskManager.TaskKilledByDriver);
                }
            }
        }

        /// <summary>
        /// Dispose function
        /// </summary>
        public void Dispose()
        {
            if (Interlocked.Read(ref _disposed) == 0)
            {
                _groupCommunicationsClient.Dispose();
                Interlocked.Exchange(ref _disposed, 1);
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