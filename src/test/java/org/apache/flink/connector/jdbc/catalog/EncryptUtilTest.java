package org.apache.flink.connector.jdbc.catalog;

import org.apache.flink.connector.jdbc.catalog.common.EncryptUtil;
import org.apache.flink.util.Preconditions;
import org.junit.jupiter.api.Test;

class EncryptUtilTest {

    @Test
    void encrypt() {
        String password = "test";
        String encryptedPass = "1B03B303CD51187173754D75C7D1D5C3";
        Preconditions.checkState(
                encryptedPass.equals(EncryptUtil.encrypt("test", "*(2AEs*_AEs3L{O:s*}{")));
    }
}
