package org.apache.flink.connector.jdbc.catalog;

import org.apache.flink.connector.jdbc.catalog.common.EncryptUtil;
import org.apache.flink.util.Preconditions;
import org.junit.jupiter.api.Test;

class EncryptUtilTest {

    @Test
    void encrypt() {
        String secretKey = "*(2AEs*_A{";
        String password = EncryptUtil.encrypt("test", secretKey);
        Preconditions.checkArgument("test".equals(EncryptUtil.decrypt(password, secretKey)));
    }
}
