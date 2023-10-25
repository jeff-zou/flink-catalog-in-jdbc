package org.apache.flink.connector.jdbc.catalog;

import static org.junit.jupiter.api.Assertions.*;

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
