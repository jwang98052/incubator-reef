/**
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

using System.CodeDom;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Examples;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Formats.AvroConfigurationDataContract;
using Org.Apache.REEF.Tang.Implementations.ClassHierarchy;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Protobuf;
using Org.Apache.REEF.Tang.Types;
using Org.Apache.REEF.Tang.Util;

namespace Org.Apache.REEF.Tang.Tests.ClassHierarchy
{
    [TestClass]
    public class TestNamedParameter
    {
        [TestMethod]
        public void TestNamedParameterWithDefaultValues()
        {
            var ns = TangFactory.GetTang().GetDefaultClassHierarchy();
            INamedParameterNode cls = (INamedParameterNode)ns.GetNode(typeof(NamedParameterWithDefaultValues).AssemblyQualifiedName);
            Assert.IsTrue(cls.GetDocumentation().Equals("NamedParameterWithDefaultValues"));
            Assert.IsTrue(cls.GetShortName().Equals("NamedParameterWithDefaultValues"));
            Assert.IsTrue(cls.GetAlias().Equals("org.apache.REEF.tang.tests.classHierarchy.NamedParameterWithDefaultValues"));
            Assert.IsTrue(cls.GetAliasLanguage().Equals(AvroConfigurationSerializer.Java));
        }

        [TestMethod]
        public void TestNamedParameterWithAlias()
        {
            var ns = TangFactory.GetTang().GetDefaultClassHierarchy();
            INamedParameterNode cls = (INamedParameterNode)ns.GetNode(typeof(NamedParameterWithAlias).AssemblyQualifiedName);
            Assert.IsTrue(cls.GetAlias().Equals("org.apache.REEF.tang.tests.classHierarchy.NamedParameterWithAlias"));
            Assert.IsTrue(cls.GetAliasLanguage().Equals(AvroConfigurationSerializer.Java));
        }

        [TestMethod]
        public void TestNamedParameterWithAliasRoundTrip()
        {
            var ns = TangFactory.GetTang().GetDefaultClassHierarchy();
            INamedParameterNode node1 = (INamedParameterNode)ns.GetNode(typeof(NamedParameterWithAlias).AssemblyQualifiedName);

            var ns1 = new ProtocolBufferClassHierarchy(ProtocolBufferClassHierarchy.Serialize(ns));
            var node2 = ns1.GetNode(typeof(NamedParameterWithAlias).AssemblyQualifiedName);

            Assert.IsTrue(node2 is INamedParameterNode);
            Assert.IsTrue(((INamedParameterNode)node2).GetAliasLanguage().Equals(AvroConfigurationSerializer.Java));
            Assert.IsTrue(((INamedParameterNode)node2).GetFullName().Equals(typeof(NamedParameterWithAlias).AssemblyQualifiedName));
            Assert.IsTrue(((INamedParameterNode)node2).GetAlias().Equals("org.apache.REEF.tang.tests.classHierarchy.NamedParameterWithAlias"));
        }

        [TestMethod]
        public void TestGetNamedparameterValue()
        {
            var b = TangFactory.GetTang().NewConfigurationBuilder()
                .BindNamedParameter<NamedParameterWithAlias, string>(GenericType<NamedParameterWithAlias>.Class, "test")
                .Build();

            var c = b.GetClassHierarchy();
            var i = TangFactory.GetTang().NewInjector(b);
            var o = i.GetInstance<ClassWithNamedParameterWithAlias>();
            var no = i.GetNamedInstance<NamedParameterWithAlias, string>();
            Assert.IsTrue(o.Value.Equals("test"));
        }

        //This test is trying to simulate scenario where Java code creates configuration with NamedParameterWithAlias1
        //and then serialize it into string 
        //C# code then de-serialize the string back to Configuration. 
        //During the de-serialization, GetNode() will be called with NamedParameterWithAlias1.  
        //If it was from Java, alias lookup table would be used to find the corresponding assembly qualified name at C# side 
        //that would be NamedParameterWithAlias2.
        //In this test, since NamedParameterWithAlias1 is also in the class hierarchy, look up table actually is not used. 
        [TestMethod]
        public void TestDeserializationWithAlias()
        {
            var b = TangFactory.GetTang().NewConfigurationBuilder()
                .BindNamedParameter<NamedParameterWithAlias1, string>(GenericType<NamedParameterWithAlias1>.Class, "test1")
                .Build();
            var n1 = b.GetClassHierarchy().GetNode(typeof(NamedParameterWithAlias1).AssemblyQualifiedName);
            Assert.AreEqual(((INamedParameterNode)n1).GetAlias(), "Org.Apache.REEF.Tang.Examples.NamedParameterWithAlias2");

            AvroConfigurationSerializer serializer = new AvroConfigurationSerializer();
            string s = serializer.ToString(b);

            var c = TangFactory.GetTang()
                .GetClassHierarchy(new string[] {typeof(NamedParameterWithAlias2).Assembly.GetName().Name });

            AvroConfiguration avroConf = JsonConvert.DeserializeObject<AvroConfiguration>(s);
            Assert.AreEqual(avroConf.language, "CS");

            var config = serializer.FromString(s, c);
           
            var n2 = config.GetClassHierarchy().GetNode(typeof(NamedParameterWithAlias1).AssemblyQualifiedName);

            //if it came from Java, it would end up to be NamedParameterWithAlias2
            Assert.AreEqual(((INamedParameterNode)n2).GetFullName(), typeof(NamedParameterWithAlias1).AssemblyQualifiedName);
        }

        [TestMethod]
        public void TestDeserializationWithAlias3()
        {
            var c = TangFactory.GetTang()
                .GetClassHierarchy(new string[] { typeof(NamedParameterWithAlias3).Assembly.GetName().Name });

            var n = c.GetNode("org.apache.REEF.NamedParameterWithAlias3", AvroConfigurationSerializer.Java);

            //using alias to get node which is not in the class hierarchy. The corresponding NamedParameter Node is returned
            Assert.AreEqual(n.GetFullName(), typeof(NamedParameterWithAlias3).AssemblyQualifiedName);
        }
    }

    [NamedParameter(Documentation = "NamedParameterWithDefaultValues",
        ShortName = "NamedParameterWithDefaultValues",
        DefaultValue = "default",
        DefaultClass = null,
        DefaultValues = null,
        DefaultClasses = null,
        Alias = "org.apache.REEF.tang.tests.classHierarchy.NamedParameterWithDefaultValues",
        AliasLanguage = AvroConfigurationSerializer.Java
     )]
    public class NamedParameterWithDefaultValues : Name<string> 
    {
    }

    [NamedParameter(alias: "org.apache.REEF.tang.tests.classHierarchy.NamedParameterWithAlias", aliasLanguage: AvroConfigurationSerializer.Java)]
    public class NamedParameterWithAlias : Name<string>
    {
    }

    public class ClassWithNamedParameterWithAlias
    {
        public string Value;

        [Inject]
        private ClassWithNamedParameterWithAlias([Parameter(typeof(NamedParameterWithAlias))] string abc)
        {
            Value = abc;
        }
    }
}
