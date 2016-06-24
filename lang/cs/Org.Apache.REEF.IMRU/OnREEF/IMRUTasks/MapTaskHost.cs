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
using System.IO;
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
        /// Shows if the object has been disposed.
        /// </summary>
        private int _disposed = 0;

        /// <summary>
        /// Group Communication client for the task
        /// </summary>
        private readonly IGroupCommClient _groupCommunicationsClient;

        /// <summary>
        /// Task close Coordinator to handle the work when receiving task close event
        /// </summary>
        private readonly TaskCloseCoordinator _taskCloseCoordinator;

        /// <summary>
        /// The cancellation token to control the group communication operation cancellation
        /// </summary>
        private readonly CancellationTokenSource _cancellationSource;

        /// <summary>
        /// </summary>
        /// <param name="mapTask">The MapTask hosted in this REEF Task.</param>
        /// <param name="groupCommunicationsClient">Used to setup the communications.</param>
        /// <param name="taskCloseCoordinator">Task close Coordinator</param>
        /// <param name="invokeGC">Whether to call Garbage Collector after each iteration or not</param>
        [Inject]
        private MapTaskHost(
            IMapFunction<TMapInput, TMapOutput> mapTask,
            IGroupCommClient groupCommunicationsClient,
            TaskCloseCoordinator taskCloseCoordinator,
            [Parameter(typeof(InvokeGC))] bool invokeGC)
        {
            _mapTask = mapTask;
            _groupCommunicationsClient = groupCommunicationsClient;
            var cg = groupCommunicationsClient.GetCommunicationGroup(IMRUConstants.CommunicationGroupName);
            _dataAndMessageReceiver =
                cg.GetBroadcastReceiver<MapInputWithControlMessage<TMapInput>>(IMRUConstants.BroadcastOperatorName);
            _dataReducer = cg.GetReduceSender<TMapOutput>(IMRUConstants.ReduceOperatorName);
            _invokeGC = invokeGC;
            _taskCloseCoordinator = taskCloseCoordinator;
            _cancellationSource = new CancellationTokenSource();
        }

        /// <summary>
        /// Performs IMRU iterations on map side
        /// </summary>
        /// <param name="memento"></param>
        /// <returns></returns>
        public byte[] Call(byte[] memento)
        {
            MapControlMessage controlMessage = MapControlMessage.AnotherRound;

            while (!_cancellationSource.IsCancellationRequested && controlMessage != MapControlMessage.Stop)
            {
                if (_invokeGC)
                {
                    Logger.Log(Level.Verbose, "Calling Garbage Collector");
                    GC.Collect();
                    GC.WaitForPendingFinalizers();
                }

                try
                {
                    using (
                    MapInputWithControlMessage<TMapInput> mapInput = _dataAndMessageReceiver.Receive(_cancellationSource))
                    {
                        controlMessage = mapInput.ControlMessage;
                        if (controlMessage != MapControlMessage.Stop)
                        {
                            _dataReducer.Send(_mapTask.Map(mapInput.Message), _cancellationSource);
                        }
                    }
                }
                catch (OperationCanceledException e)
                {
                    Logger.Log(Level.Warning, "Received OperationCanceledException in MapTaskHost with message: {0}.", e.Message);
                    break;
                }
                catch (IOException e)
                {
                    Logger.Log(Level.Error, "Received IOException in MapTaskHost with message: {0}.", e.Message);
                    if (!_cancellationSource.IsCancellationRequested)
                    {
                        throw new IMRUTaskGroupCommunicationException(TaskManager.TaskGroupCommunicationError);
                    }
                    break;
                }
                catch (Exception e)
                {
                    Logger.Log(Level.Error, "Received Exception in MapTaskHos with exception type {0} and stack trace {1}.", e.GetType(), e.StackTrace);
                    throw e;
                }
            }

            _taskCloseCoordinator.SignalTaskStopped();
            Logger.Log(Level.Verbose, "MapTaskHost returned with cancellation token:{0}.", _cancellationSource.IsCancellationRequested);
            return null;
        }

        /// <summary>
        /// Task close handler. Calls TaskCloseCoordinator to handle the event.
        /// </summary>
        /// <param name="closeEvent"></param>
        public void OnNext(ICloseEvent closeEvent)
        {
            _taskCloseCoordinator.HandleEvent(closeEvent, _cancellationSource);
        }

        /// <summary>
        /// Dispose function. Dispose IGroupCommunicationsClient.
        /// </summary>
        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) == 0)
            {
                _groupCommunicationsClient.Dispose();
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