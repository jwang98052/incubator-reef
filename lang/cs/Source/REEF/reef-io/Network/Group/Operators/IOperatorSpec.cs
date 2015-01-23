﻿/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

using System;
using Org.Apache.Reef.Tang.Util;
using Org.Apache.Reef.Wake.Remote;

namespace Org.Apache.Reef.IO.Network.Group.Operators
{
    /// <summary>
    /// The specification used to define Broadcast Operators.
    /// </summary>
    public interface IOperatorSpec<T>
    {
        /// <summary>
        /// Returns the codec used to serialize and deserialize messages.
        /// </summary>
        ICodec<T> Codec { get; }
    }
}