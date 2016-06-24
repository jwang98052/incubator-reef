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
using System.Collections.Concurrent;
using System.Threading;
using Org.Apache.REEF.Network.Group.Driver.Impl;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Network.Group.Task.Impl
{
    /// <summary>
    /// Stores all incoming messages sent by a particular Task.
    /// Writable version
    /// </summary>
    /// <typeparam name="T"> Generic type of message</typeparam>
    internal sealed class NodeStruct<T>
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(NodeStruct<T>));
        private readonly BlockingCollection<GroupCommunicationMessage<T>> _messageQueue;

        /// <summary>
        /// Creates a new NodeStruct.
        /// </summary>
        /// <param name="id">The Task identifier</param>
        internal NodeStruct(string id)
        {
            Identifier = id;
            _messageQueue = new BlockingCollection<GroupCommunicationMessage<T>>();
        }

        /// <summary>
        /// Returns the identifier for the Task that sent all
        /// messages in the message queue.
        /// </summary>
        internal string Identifier { get; private set; }

        /// <summary>
        /// Gets the first message in the message queue.
        /// </summary>
        /// <param name="cancellationSource">The cancellation token for the data reading operation cancellation</param>
        /// <returns>The first available message.</returns>
        internal T[] GetData(CancellationTokenSource cancellationSource = null)
        {
            try
            {
                return cancellationSource == null
                    ? _messageQueue.Take().Data
                    : _messageQueue.Take(cancellationSource.Token).Data;
            }
            catch (OperationCanceledException e)
            {
                Logger.Log(Level.Warning, "Received OperationCanceledException in NodeStruct.GetData() with message {0}.", e.Message);
                throw e;
            }
        }

        /// <summary>
        /// Adds an incoming message to the message queue.
        /// </summary>
        /// <param name="gcm">The incoming message</param>
        internal void AddData(GroupCommunicationMessage<T> gcm)
        {
            _messageQueue.Add(gcm);
        }

        /// <summary>
        /// Tells whether there is a message in queue or not.
        /// </summary>
        /// <returns>True if queue is non empty, false otherwise.</returns>
        internal bool HasMessage()
        {
            if (_messageQueue.Count != 0)
            {
                return true;
            }
            else
            {
                return false;
            }
        }
    }
}
