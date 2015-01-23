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

using Org.Apache.Reef.Tang.Annotations;
using Org.Apache.Reef.Tang.Formats;
using Org.Apache.Reef.Tang.Interface;
using Org.Apache.Reef.Tang.Util;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;

[module: SuppressMessage("StyleCop.CSharp.MaintainabilityRules", "SA1401:FieldsMustBePrivate", Justification = "static field, typical usage in configurations")]

namespace Org.Apache.Reef.Services
{
    /// <summary>
    /// Configuration module for services. The configuration created here can be passed alongside a ContextConfiguration
    /// to form a context. Different from bindings made in the ContextConfiguration, those made here will be passed along
    /// to child context.
    /// </summary>
    public class ServiceConfiguration : ConfigurationModuleBuilder
    {
        /// <summary>
        /// A set of services to instantiate. All classes given here will be instantiated in the context, and their references
        /// will be made available to child context and tasks.
        /// </summary>
        [SuppressMessage("Microsoft.Security", "CA2104:Do not declare read only mutable reference types", Justification = "not applicable")]
        public static readonly OptionalParameter<IService> Services = new OptionalParameter<IService>();

        public ServiceConfiguration()
            : base()
        {
        }

        public ServiceConfiguration(string config)
        {
            TangConfig = new AvroConfigurationSerializer().FromString(config);
        }

        public static ConfigurationModule ConfigurationModule
        {
            get
            {
                return new ServiceConfiguration()
                    .BindSetEntry(GenericType<ServicesSet>.Class, Services)
                    .Build();
            }
        }

        public IConfiguration TangConfig { get; private set; }
    }

    public class InjectedServices
    {
        [Inject]
        public InjectedServices([Parameter(typeof(ServicesSet))] ISet<IService> services)
        {
            Services = services;
        }

        public ISet<IService> Services { get; set; }
    }

    [NamedParameter("Set of services", "servicesSet", "")]
    class ServicesSet : Name<ISet<IService>>
    {      
    }
}
