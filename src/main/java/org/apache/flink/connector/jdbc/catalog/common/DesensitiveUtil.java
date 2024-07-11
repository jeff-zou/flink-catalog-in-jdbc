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

package org.apache.flink.connector.jdbc.catalog.common;

import java.util.Map;

public class DesensitiveUtil {

    public static final String DESENSITIVE_STRIGN = "******";

    public static final String SENSITIVE_KEY_PASSWORD = "password";

    public static final String SENSITIVE_KEY_SASL = "properties.sasl.jaas.config";

    public static String desensitiveForProperties(Map<String, String> properties) {
        String password = null;
        if (properties.size() == 0) {
            return password;
        }

        if (properties.get("connector").contains("kafka")
                || properties.get("connector").contains("upsert-kafka")) {
            String sasl = properties.get(SENSITIVE_KEY_SASL);
            if (sasl != null) {
                int passwordStartIndex = sasl.indexOf("password=\"") + 10;
                int passwordEndIndex = sasl.indexOf("\"", passwordStartIndex);
                password = sasl.substring(passwordStartIndex, passwordEndIndex);
                String newSasl =
                        sasl.substring(0, passwordStartIndex)
                                + DESENSITIVE_STRIGN
                                + sasl.substring(passwordEndIndex);
                properties.put(SENSITIVE_KEY_SASL, newSasl);
            }
        } else {
            password = properties.get(SENSITIVE_KEY_PASSWORD);
            if (password != null) {
                properties.put(SENSITIVE_KEY_PASSWORD, DESENSITIVE_STRIGN);
            }
        }
        return DESENSITIVE_STRIGN.equals(password) ? null : password;
    }

    public static boolean sensitiveForProperties(String password, Map<String, String> properties) {
        if (properties.size() == 0) {
            return false;
        }

        if (properties.get("connector").contains("kafka")
                || properties.get("connector").contains("upsert-kafka")) {
            String sasl = properties.get(SENSITIVE_KEY_SASL);
            if (sasl != null) {
                sasl = sasl.replace(DESENSITIVE_STRIGN, password);
                properties.put(SENSITIVE_KEY_SASL, sasl);
                return true;
            }
        } else {
            if (properties.containsKey(SENSITIVE_KEY_PASSWORD)) {
                properties.put(SENSITIVE_KEY_PASSWORD, password);
                return true;
            }
        }
        return false;
    }
}
