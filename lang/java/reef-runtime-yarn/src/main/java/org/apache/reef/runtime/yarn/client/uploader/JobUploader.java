/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.reef.runtime.yarn.client.uploader;

import org.apache.commons.net.util.Base64;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.reef.runtime.common.files.REEFFileNames;
import org.apache.reef.runtime.yarn.driver.JobSubmissionDirectoryProvider;
import org.apache.reef.runtime.yarn.util.YarnConfigurationSetUp;

import javax.inject.Inject;
import java.io.IOException;
//import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Helper class to upload the driver files to HDFS.
 */
public final class JobUploader {

  private static final Logger LOG = Logger.getLogger(JobUploader.class.getName());

  private final FileSystem fileSystem;
  private final JobSubmissionDirectoryProvider jobSubmissionDirectoryProvider;

  @Inject
  JobUploader(final YarnConfiguration yarnConfiguration,
              final JobSubmissionDirectoryProvider jobSubmissionDirectoryProvider)
              throws IOException, URISyntaxException {
    this.jobSubmissionDirectoryProvider = jobSubmissionDirectoryProvider;
    final REEFFileNames fileNames = new REEFFileNames();
    final String securityTokenIdentifierFile = fileNames.getSecurityTokenIdentifierFile();
    byte[] identifier = Files.readAllBytes(Paths.get(securityTokenIdentifierFile));
    String encoded = Base64.encodeBase64String(identifier);
    LOG.log(Level.INFO, "$$$$$$securityTokenIdentifier: " + encoded);
    yarnConfiguration.set("fs.adl.creds", encoded);
    LOG.log(Level.INFO, "$$$$$$after setting cred");
    YarnConfigurationSetUp.setDefaultFileSystem(yarnConfiguration);
    //yarnConfiguration.set("fs.defaultFS", "adl://ca0koboperf.caboaccountdogfood.net");
    //yarnConfiguration.set("fs.defaultFS", "adl://kobo05dfaccountadls.caboaccountdogfood.net");
    LOG.log(Level.INFO, "$$$$$$#after setting setDefaultFileSystem");
    this.fileSystem = FileSystem.get(yarnConfiguration);
    LOG.log(Level.INFO, "$$$$$$#after FileSystem.get(yarnConfiguration)");
    //this.fileSystem = FileSystem.get(URI.create("adl://reefadls2.azuredatalakestore.net"), yarnConfiguration);
    //this.fileSystem = FileSystem.get(URI.create("adl://ca0koboperf.caboaccountdogfood.net"), yarnConfiguration);
    //this.fileSystem = FileSystem.get(
    // URI.create("adl://kobo05dfaccountadls.caboaccountdogfood.net"), yarnConfiguration);
    //LOG.log(Level.INFO, "$$$$$$after FileSystem.FileSystem.get(URI.create()");
    LOG.log(Level.INFO, "$$$$$$fileSystem.getScheme {0}: getHomeDirectory: {1}.",
            new Object[] {fileSystem.getScheme(), fileSystem.getHomeDirectory()});
  }

  /**
   * Creates the Job folder on the DFS.
   *
   * @param applicationId
   * @return a reference to the JobFolder that can be used to upload files to it.
   * @throws IOException
   */
  public JobFolder createJobFolderWithApplicationId(final String applicationId) throws IOException {
    final Path jobFolderPath = jobSubmissionDirectoryProvider.getJobSubmissionDirectoryPath(applicationId);
    final String finalJobFolderPath = jobFolderPath.toString();
    LOG.log(Level.FINE, "Final job submission Directory: " + finalJobFolderPath);
    return createJobFolder(finalJobFolderPath);
  }


  /**
   * Convenience override for int ids.
   *
   * @param finalJobFolderPath
   * @return
   * @throws IOException
   */
  public JobFolder createJobFolder(final String finalJobFolderPath) throws IOException {
    LOG.log(Level.FINE, "Final job submission Directory: " + finalJobFolderPath);
//    return new JobFolder(this.fileSystem, new Path(finalJobFolderPath));
    return new JobFolder(this.fileSystem, fileSystem.makeQualified(new Path(finalJobFolderPath)));
  }

  /**
   * Convenience override for int ids.
   *
   * @param applicationId
   * @return
   * @throws IOException
   */
  public JobFolder createJobFolder(final int applicationId) throws IOException {
    return this.createJobFolderWithApplicationId(Integer.toString(applicationId));
  }
}
