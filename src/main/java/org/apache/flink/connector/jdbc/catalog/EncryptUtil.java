package org.apache.flink.connector.jdbc.catalog;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

/** Created by jeff.zou. on 2021/3/27.11:56 */
public class EncryptUtil {

    private static final Logger log = LoggerFactory.getLogger(EncryptUtil.class);

    private static final String SECRET_KEY = "*(2AEs*_AEs3L{O:s*}{";

    /**
     * AES加密字符串.
     *
     * @param content 需要被加密的字符串
     * @return 密文字符串
     */
    public static String encrypt(String content, String secretKey) {
        byte[] bytes = encryptToBytes(content, secretKey == null ? SECRET_KEY : secretKey);
        return parseByte2HexStr(bytes);
    }

    /**
     * AES加密字符串.
     *
     * @param content 需要被加密的字符串
     * @return 密文
     */
    private static byte[] encryptToBytes(String content, String secretKey) {
        try {

            Cipher cipher = Cipher.getInstance("AES"); // 创建密码器
            byte[] byteContent = content.getBytes("utf-8");
            cipher.init(Cipher.ENCRYPT_MODE, initKeyForAES(secretKey)); // 初始化为加密模式的密码器
            byte[] result = cipher.doFinal(byteContent); // 加密
            return result;
        } catch (Exception e) {
            log.error("", e);
        }
        return null;
    }

    /**
     * 解密AES加密过的字符串.
     *
     * @param content AES加密过过的内容
     * @return 明文
     */
    public static String decrypt(String content, String secretKey) {
        byte[] result =
                decryptFromBytes(
                        parseHexStr2Byte(content), secretKey == null ? SECRET_KEY : secretKey);
        return new String(result);
    }

    /**
     * 解密AES加密过的字符串.
     *
     * @param bytes AES加密过过的内容
     * @return 明文
     */
    public static byte[] decryptFromBytes(byte[] bytes, String secretKey) {
        try {
            Cipher cipher = Cipher.getInstance("AES"); // 创建密码器
            cipher.init(Cipher.DECRYPT_MODE, initKeyForAES(secretKey)); // 初始化为解密模式的密码器
            byte[] result = cipher.doFinal(bytes);
            return result; // 明文
        } catch (Exception e) {
            log.error("", e);
        }
        return null;
    }

    /**
     * init .
     *
     * @param key
     * @return
     * @throws NoSuchAlgorithmException
     */
    private static SecretKeySpec initKeyForAES(String key) throws Exception {
        if (null == key || key.length() == 0) {
            throw new NullPointerException("key not is null");
        }
        SecretKeySpec key2 = null;
        SecureRandom random = SecureRandom.getInstance("SHA1PRNG");
        random.setSeed(key.getBytes());
        KeyGenerator kgen = KeyGenerator.getInstance("AES");
        kgen.init(128, random);
        SecretKey secretKey = kgen.generateKey();
        byte[] enCodeFormat = secretKey.getEncoded();
        key2 = new SecretKeySpec(enCodeFormat, "AES");
        return key2;
    }

    /**
     * 将二进制转换成16进制.
     *
     * @param buf
     * @return
     */
    private static String parseByte2HexStr(byte[] buf) {
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < buf.length; i++) {
            String hex = Integer.toHexString(buf[i] & 0xFF);
            if (hex.length() == 1) {
                hex = '0' + hex;
            }
            sb.append(hex.toUpperCase());
        }
        return sb.toString();
    }

    /**
     * 将16进制转换为二进制.
     *
     * @param hexStr
     * @return
     */
    public static byte[] parseHexStr2Byte(String hexStr) {
        if (hexStr.length() < 1) {
            return null;
        }
        byte[] result = new byte[hexStr.length() / 2];
        for (int i = 0; i < hexStr.length() / 2; i++) {
            int high = Integer.parseInt(hexStr.substring(i * 2, i * 2 + 1), 16);
            int low = Integer.parseInt(hexStr.substring(i * 2 + 1, i * 2 + 2), 16);
            result[i] = (byte) (high * 16 + low);
        }
        return result;
    }
}
