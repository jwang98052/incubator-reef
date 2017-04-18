/*
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
package org.apache.reef.runtime.yarn.util;

import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Helper class to set yarn configurations.
 */
public final class YarnConfigurationSetUp {
  private static final Logger LOG = Logger.getLogger(YarnConfigurationSetUp.class.getName());
  private YarnConfigurationSetUp() {
  }

  public static void setDefaultFileSystem(final YarnConfiguration yarnConfigration) {
    try {
      //yarnConfigration.set("fs.defaultFS", "adl://ca0koboperf.caboaccountdogfood.net");
      yarnConfigration.set("fs.defaultFS", "adl://kobo05dfaccountadls.caboaccountdogfood.net");
      //yarnConfigration.set("fs.defaultFS", "adl://kobo03tsperfssd01.caboaccountdogfood.net");
      LOG.log(Level.INFO, "$$$$$$YarnConfigurationSetUp after setting setDefaultFileSystem");
    } catch(Exception e) {
      LOG.log(Level.SEVERE, "Exception is set default file system.");
    }
  }
}
