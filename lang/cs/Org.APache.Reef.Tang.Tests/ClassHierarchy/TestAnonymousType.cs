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
using System.Collections.Generic;
using System.IO;
using Org.Apache.Reef.Tang.Examples;
using Org.Apache.Reef.Tang.Implementations;
using Org.Apache.Reef.Tang.Interface;
using Org.Apache.Reef.Tang.Protobuf;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Org.Apache.Reef.Tang.Test.ClassHierarchy
{
    [TestClass]
    public class TestAnonymousType
    {
        const string ClassHierarchyBinFileName = "example.bin";

        [ClassInitialize]
        public static void ClassSetup(TestContext context)
        {
            TangImpl.Reset();
        }

        [TestMethod]
        public void TestAnonymousTypeWithDictionary()
        {
            List<string> appDlls = new List<string>();
            appDlls.Add(typeof(AnonymousType).Assembly.GetName().Name);
            var c = TangFactory.GetTang().GetClassHierarchy(appDlls.ToArray());
            c.GetNode(typeof(AnonymousType).AssemblyQualifiedName);

            IConfiguration conf = TangFactory.GetTang().NewConfigurationBuilder(c).Build();
            IInjector injector = TangFactory.GetTang().NewInjector(conf);
            var obj = injector.GetInstance<AnonymousType>();
            Assert.IsNotNull(obj);

            var cd = Directory.GetCurrentDirectory();
            Console.WriteLine(cd);

            ProtocolBufferClassHierarchy.Serialize(ClassHierarchyBinFileName, c);
            IClassHierarchy ch = ProtocolBufferClassHierarchy.DeSerialize(ClassHierarchyBinFileName);
            ch.GetNode(typeof(AnonymousType).AssemblyQualifiedName);
        }
    }
}
