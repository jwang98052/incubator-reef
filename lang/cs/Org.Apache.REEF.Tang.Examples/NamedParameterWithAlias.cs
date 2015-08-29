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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Formats;

namespace Org.Apache.REEF.Tang.Examples
{
    [NamedParameter(alias: "Org.Apache.REEF.Tang.Examples.NamedParameterWithAlias2", aliasLanguage: AvroConfigurationSerializer.Java)]
    public class NamedParameterWithAlias1 : Name<string>
    {
    }

    [NamedParameter(alias: "Org.Apache.REEF.Tang.Examples.NamedParameterWithAlias1", aliasLanguage: AvroConfigurationSerializer.Cs)]
    public class NamedParameterWithAlias2 : Name<string>
    {
    }

    [NamedParameter(alias: "org.apache.REEF.NamedParameterWithAlias3", aliasLanguage: AvroConfigurationSerializer.Java)]
    public class NamedParameterWithAlias3 : Name<string>
    {
    }

    public class ClassWithNamedParameterWithAlias1
    {
        public string Value;

        [Inject]
        private ClassWithNamedParameterWithAlias1([Parameter(typeof(NamedParameterWithAlias1))] string abc)
        {
            Value = abc;
        }
    }
}
