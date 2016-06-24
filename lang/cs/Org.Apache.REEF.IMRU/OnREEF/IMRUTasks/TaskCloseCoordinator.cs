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
using Org.Apache.REEF.Common.Tasks.Events;
using Org.Apache.REEF.IMRU.OnREEF.Driver;
using Org.Apache.REEF.IMRU.OnREEF.Parameters;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Attributes;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.IMRU.OnREEF.IMRUTasks
{
    /// <summary>
    /// This class provides a method to handle Task close event. It is called from TaskCloseEventHandler. 
    /// </summary>
    [ThreadSafe]
    internal sealed class TaskCloseCoordinator
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(TaskCloseCoordinator));

        /// <summary>
        /// Waiting time for the task to close by itself
        /// </summary>
        private readonly int _enforceCloseTimeoutMilliseconds;

        /// <summary>
        /// An event that will wait in close handler until it is either signaled from Call method or timeout.
        /// </summary>
        private readonly ManualResetEventSlim _waitToCloseEvent = new ManualResetEventSlim(false);

        /// <summary>
        /// Handle task close event and manage the states, wait/signal when closing the task
        /// </summary>
        /// <param name="enforceCloseTimeoutMilliseconds">Timeout in milliseconds to enforce the task to close if receiving task close event</param>
        [Inject]
        private TaskCloseCoordinator([Parameter(typeof(EnforceCloseTimeoutMilliseconds))] int enforceCloseTimeoutMilliseconds)
        {
            _enforceCloseTimeoutMilliseconds = enforceCloseTimeoutMilliseconds;
        }

        /// <summary>
        /// Handle Task close event.
        /// Cancel the CancellationToken for data reading operation
        /// Then waiting for the signal from Call method. Either it is signaled or after _enforceCloseTimeoutMilliseconds,
        /// If the closed event is sent from driver, checks if the _waitToCloseEvent has been signaled. If not, throw 
        /// IMRUTaskSystemException.
        /// </summary>
        /// <param name="closeEvent"></param>
        /// <param name="cancellationTokenSource"></param>
        internal void HandleEvent(ICloseEvent closeEvent, CancellationTokenSource cancellationTokenSource)
        {
            cancellationTokenSource.Cancel();
            var taskSignaled = _waitToCloseEvent.Wait(TimeSpan.FromMilliseconds(_enforceCloseTimeoutMilliseconds));

            if (closeEvent.Value.IsPresent())
            {
                var msg = Encoding.UTF8.GetString(closeEvent.Value.Value);
                if (msg.Equals(TaskManager.CloseTaskByDriver))
                {
                    Logger.Log(Level.Info, "The task received close event with message: {0}.", msg);

                    if (!taskSignaled)
                    {
                        throw new IMRUTaskSystemException(TaskManager.TaskKilledByDriver);
                    }
                }
            }
            else
            {
                Logger.Log(Level.Warning, "The task received close event with no message.");
            }
        }

        /// <summary>
        /// Called from Task right before the task is returned to signals _waitToCloseEvent. 
        /// </summary>
        internal void SignalTaskStopped()
        {
            _waitToCloseEvent.Set();
        }
    }
}
