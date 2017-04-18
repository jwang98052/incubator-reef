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
package org.apache.reef.runtime.common.driver.catalog;

import org.apache.reef.annotations.audience.Private;
import org.apache.reef.driver.catalog.NodeDescriptor;
import org.apache.reef.driver.catalog.RackDescriptor;
import org.apache.reef.driver.catalog.ResourceCatalog;
import org.apache.reef.runtime.common.driver.resourcemanager.NodeDescriptorEvent;

import javax.inject.Inject;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

/**
 * A catalog of the resources available to a REEF instance.
 */
@Private
public final class ResourceCatalogImpl implements ResourceCatalog {

  public static final String DEFAULT_RACK = "/default-rack";
  private static final Logger LOG = Logger.getLogger(ResourceCatalog.class.getName());
  private final Map<String, RackDescriptorImpl> racks = new HashMap<>();

  private final Map<String, NodeDescriptorImpl> nodes = new HashMap<>();

  @Inject
  ResourceCatalogImpl() {
    LOG.log(Level.FINE, "Instantiated 'ResourceCatalogImpl'");
  }

  @Override
  public synchronized String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("=== Resource Catalog ===");
    for (final RackDescriptor rack : racks.values()) {
      sb.append("\n" + rack);
    }
    return sb.toString();
  }

  @Override
  public synchronized Collection<NodeDescriptor> getNodes() {
    return Collections.unmodifiableCollection(new ArrayList<NodeDescriptor>(this.nodes.values()));
  }

  @Override
  public synchronized Collection<RackDescriptor> getRacks() {
    return Collections.unmodifiableCollection(new ArrayList<RackDescriptor>(this.racks.values()));
  }

  public synchronized NodeDescriptor getNode(final String id) {
    return this.nodes.get(id);
  }

  public synchronized void handle(final NodeDescriptorEvent node) {
    final String rackName = node.getRackName().orElse(DEFAULT_RACK);

    LOG.log(Level.INFO, "######Catalog new node: id[{0}], rack[{1}], host[{2}], port[{3}], memory[{4}]",
        new Object[]{node.getIdentifier(), rackName, node.getHostName(), node.getPort(),
            node.getMemorySize()}
    );

    if (!this.racks.containsKey(rackName)) {
      final RackDescriptorImpl rack = new RackDescriptorImpl(rackName);
      this.racks.put(rackName, rack);
    }
    final RackDescriptorImpl rack = this.racks.get(rackName);

    try {
      InetAddress addr = InetAddress.getByName(node.getHostName());
      if (addr == null) {
        LOG.log(Level.SEVERE, "####$$InetAddress.getByName returns null");
      } else {
        LOG.log(Level.INFO, "####$$InetAddress.getByName by host {0} returns getAddress: {1}, getHostAddress: {2}.",
                new Object[] {node.getHostName(), addr.getAddress(), addr.getHostAddress()});
      }
    } catch(UnknownHostException e) {
      LOG.log(Level.SEVERE, "####$$UnknownHostException in InetAddress.getByName", e);
    }

/*    if (isIP(node.getHostName())) {
      LOG.log(Level.INFO, "####the host name is IP " + node.getHostName());
    } else {
      LOG.log(Level.INFO, "####the host name is not IP " + node.getHostName());
    }

    try {
      String[] ip = node.getHostName().split("\\.");
      byte d1 = (byte)Integer.parseInt(ip[0]);
      byte d2 = (byte)Integer.parseInt(ip[1]);
      byte d3 = (byte)Integer.parseInt(ip[2]);
      byte d4 = (byte)Integer.parseInt(ip[3]);
      InetAddress addr = InetAddress.getByAddress(new byte[]{d1, d2, d3, d4});
      LOG.log(Level.INFO, "####the addr " + d1 + "-" + d2 + "-" + d3 + "-" + d4);
      LOG.log(Level.INFO, "####InetAddress.getByAddress by host {0} returns getAddress: {1}, getHostAddress: {2}.",
                new Object[]{node.getHostName(), addr.getAddress(), addr.getHostAddress()});
    } catch (UnknownHostException e) {
      LOG.log(Level.SEVERE, "####UnknownHostException in InetAddress.getByAddress", e);
    }*/

    final InetSocketAddress address = new InetSocketAddress(node.getHostName(), node.getPort());
    LOG.log(Level.INFO, "####InetSocketAddress:{0}, hostName: {1}, host: {2}.",
            new Object[]{address.getAddress(), address.getHostName(), address.getHostString()});
    final NodeDescriptorImpl nodeDescriptor = new NodeDescriptorImpl(node.getIdentifier(), address, rack,
        node.getMemorySize());
    LOG.log(Level.INFO, "$$$$nodeDescriptor:" + nodeDescriptor.toString());
    this.nodes.put(nodeDescriptor.getId(), nodeDescriptor);
  }

  public static boolean isIP(final String ipStr) {
    String regex = "\\b((25[0–5]|2[0–4]\\d|[01]?\\d\\d?)(\\.)){3}(25[0–5]|2[0–4]\\d|[01]?\\d\\d?)\\b";
    return Pattern.matches(regex, ipStr);
  }
}
