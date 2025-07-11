/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.fs.s3.token;

import com.alibaba.fluss.fs.token.Credentials;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicSessionCredentials;
import org.apache.hadoop.fs.s3a.auth.NoAwsCredentialsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Support dynamic session credentials for authenticating with AWS. Please note that users may
 * reference this class name from configuration property fs.s3a.aws.credentials.provider. Therefore,
 * changing the class name would be a backward-incompatible change. This credential provider must
 * not fail in creation because that will break a chain of credential providers.
 */
public class DynamicTemporaryAWSCredentialsProvider implements AWSCredentialsProvider {

    public static final String NAME = DynamicTemporaryAWSCredentialsProvider.class.getName();

    public static final String COMPONENT = "Dynamic session credentials for Fluss";

    private static final Logger LOG =
            LoggerFactory.getLogger(DynamicTemporaryAWSCredentialsProvider.class);

    @Override
    public AWSCredentials getCredentials() {
        Credentials credentials = S3DelegationTokenReceiver.getCredentials();

        if (credentials == null) {
            throw new NoAwsCredentialsException(COMPONENT);
        }
        LOG.debug("Providing session credentials");
        return new BasicSessionCredentials(
                credentials.getAccessKeyId(),
                credentials.getSecretAccessKey(),
                credentials.getSecurityToken());
    }

    @Override
    public void refresh() {
        // Intentionally blank. Credentials are updated by S3DelegationTokenReceiver
    }
}
