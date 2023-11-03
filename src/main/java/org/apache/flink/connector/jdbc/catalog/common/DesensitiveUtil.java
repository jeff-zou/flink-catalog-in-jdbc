package org.apache.flink.connector.jdbc.catalog.common;

import java.util.Map;

public class DesensitiveUtil {

    public static final String DESENSITIVE_STRIGN = "******";

    public static String desensitiveForProperties(Map<String, String> properties) {
        String password = null;
        if (properties.get("connector").contains("kafka")) {
            String sasl = properties.get("properties.sasl.jaas.config");
            if (sasl != null) {
                int passwordStartIndex = sasl.indexOf("password=\"") + 10;
                int passwordEndIndex = sasl.indexOf("\"", passwordStartIndex);
                password = sasl.substring(passwordStartIndex, passwordEndIndex);
                String newSasl =
                        sasl.substring(0, passwordStartIndex)
                                + DESENSITIVE_STRIGN
                                + sasl.substring(passwordEndIndex);
                properties.put("properties.sasl.jaas.config", newSasl);
            }
        } else {
            password = properties.get("password");
            if (password != null) {
                properties.put("password", DESENSITIVE_STRIGN);
            }
        }
        return password;
    }

    public static boolean sensitiveForProperties(String password, Map<String, String> properties) {
        if (properties.get("connector").contains("kafka")) {
            String sasl = properties.get("properties.sasl.jaas.config");
            if (sasl != null) {
                sasl = sasl.replace(DESENSITIVE_STRIGN, password);
                properties.put("properties.sasl.jaas.config", sasl);
                return true;
            }
        } else {
            if (properties.containsKey("password")) {
                properties.put("password", password);
                return true;
            }
        }
        return false;
    }
}
