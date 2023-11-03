package org.apache.flink.connector.jdbc.catalog.common;

import java.util.Map;

public class DesensitiveUtil {

    public static final String DESENSITIVE_STRIGN = "******";

    public static final String SENSITIVE_KEY_PASSWORD = "password";

    public static final String SENSITIVE_KEY_SASL = "properties.sasl.jaas.config";

    public static String desensitiveForProperties(Map<String, String> properties) {
        String password = null;
        if (properties.get("connector").contains("kafka")) {
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
        return password;
    }

    public static boolean sensitiveForProperties(String password, Map<String, String> properties) {
        if (properties.get("connector").contains("kafka")) {
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
