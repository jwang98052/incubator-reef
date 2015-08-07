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
package org.apache.reef.javabridge;

import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.io.naming.Identifiable;
import org.apache.reef.tang.ClassHierarchy;
import org.apache.reef.tang.formats.AvroConfigurationSerializer;

import java.util.logging.Level;
import java.util.logging.Logger;

public final class ActiveContextBridge extends NativeBridge implements Identifiable {
  private static final Logger LOG = Logger.getLogger(ActiveContextBridge.class.getName());

  private final ActiveContext jactiveContext;
  private final AvroConfigurationSerializer serializer;
  private final String contextId;
  private final String evaluatorId;
  private final ClassHierarchy clrClassHierarchy;

  ActiveContextBridge(final ActiveContext activeContext,
                      final ClassHierarchy clrClassHierarchy,
                      final AvroConfigurationSerializer serializer) {
    this.jactiveContext = activeContext;
    this.clrClassHierarchy = clrClassHierarchy;
    this.serializer = serializer;
    this.contextId = activeContext.getId();
    this.evaluatorId = activeContext.getEvaluatorId();
  }

  public void submitTaskString(final String taskConfigurationString) {
    if (taskConfigurationString.isEmpty()) {
      throw new RuntimeException("empty taskConfigurationString provided.");
    }

    jactiveContext.submitTask(taskConfigurationString);

//    final Configuration taskConfiguration;
//    try {
//      taskConfiguration = serializer.fromString(taskConfigurationString, clrClassHierarchy);
//    } catch (final Exception e) {
//      final String message = "Unable to de-serialize CLR  task configurations using class hierarchy.";
//      LOG.log(Level.SEVERE, message, e);
//      throw new RuntimeException(message, e);
//    }
//    jactiveContext.submitTask(taskConfiguration);
  }

  public String getEvaluatorDescriptorSring() {
    final String descriptorString = Utilities.getEvaluatorDescriptorString(jactiveContext.getEvaluatorDescriptor());
    LOG.log(Level.FINE, "active context - serialized evaluator descriptor: " + descriptorString);
    return descriptorString;
  }

  @Override
  public void close() {
    jactiveContext.close();
  }

  @Override
  public String getId() {
    return jactiveContext.getId();
  }
}
