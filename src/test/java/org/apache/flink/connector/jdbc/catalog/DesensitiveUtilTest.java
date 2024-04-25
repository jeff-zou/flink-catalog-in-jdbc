/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.jdbc.catalog;

import org.apache.flink.connector.jdbc.catalog.common.DesensitiveUtil;
import org.apache.flink.util.Preconditions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

class DesensitiveUtilTest {

    @Test
    void desensitiveForProperties() {
        Map<String, String> properties = new HashMap<String, String>();
        properties.put("connector", "kafka");
        properties.put(
                "properties.sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"test\"  password=\"test\";");
        DesensitiveUtil.desensitiveForProperties(properties);
        Preconditions.checkArgument(
                properties
                        .get("properties.sasl.jaas.config")
                        .equals(
                                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"test\"  password=\""
                                        + DesensitiveUtil.DESENSITIVE_STRIGN
                                        + "\";"));
    }

    @Test
    void desensitiveForProperties2() {
        Map<String, String> properties = new HashMap<String, String>();
        properties.put("connector", "jdbc");
        properties.put("password", "aac");
        DesensitiveUtil.desensitiveForProperties(properties);
        Preconditions.checkArgument(
                properties.get("password").equals(DesensitiveUtil.DESENSITIVE_STRIGN));
    }

    @Test
    void desensitiveForProperties3() {
        Map<String, String> properties = new HashMap<String, String>();
        properties.put("connector", "jdbc");
        properties.put("password", "*******");
        DesensitiveUtil.desensitiveForProperties(properties);
        Preconditions.checkArgument(
                properties.get("password").equals(DesensitiveUtil.DESENSITIVE_STRIGN));
    }

    @Test
    void desensitiveForProperties4() {
        Map<String, String> properties = new HashMap<String, String>();
        properties.put("connector", "jdbc");
        String password = DesensitiveUtil.desensitiveForProperties(properties);
        Preconditions.checkArgument(password == null);
    }

    @Test
    void sensitiveForProperties() {
        String password = "aaa";
        Map<String, String> properties = new HashMap<String, String>();
        properties.put("connector", "kafka");
        properties.put(
                "properties.sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"test\"  password=\"******\";");
        DesensitiveUtil.sensitiveForProperties(password, properties);
        Preconditions.checkArgument(
                properties
                        .get("properties.sasl.jaas.config")
                        .equals(
                                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"test\"  password=\""
                                        + password
                                        + "\";"));
    }

    @Test
    void sensitiveForProperties2() {
        String password = "aaa";
        Map<String, String> properties = new HashMap<String, String>();

        properties.put("connector", "jdbc");
        properties.put("password", DesensitiveUtil.DESENSITIVE_STRIGN);
        DesensitiveUtil.sensitiveForProperties(password, properties);
        Preconditions.checkArgument(properties.get("password").equals(password));
    }

    @Test
    void sensitiveForProperties3() {
        String password = "aaa";
        Map<String, String> properties = new HashMap<String, String>();

        properties.put("connector", "jdbc");
        DesensitiveUtil.sensitiveForProperties(password, properties);
    }

    @Test
    void sensitiveForProperties4() {
        String password = "aaa";
        Map<String, String> properties = new HashMap<String, String>();
        properties.put("connector", "kafka");

        DesensitiveUtil.sensitiveForProperties(password, properties);
    }
}
