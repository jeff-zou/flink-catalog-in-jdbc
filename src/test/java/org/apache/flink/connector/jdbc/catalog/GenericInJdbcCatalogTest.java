package org.apache.flink.connector.jdbc.catalog;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.junit.Test;

/**
 * @Author: Jeff Zou @Date: 2022/8/4 14:04
 */
public class GenericInJdbcCatalogTest {

    private static final String CREATE_CATALOG =
            "create catalog  my_catalog with ("
                    + "'type'='generic_in_jdbc', 'default-database'='my_database', 'username'='test', 'password'='test',"
                    + " 'url'='jdbc:mysql://*****:3306/test?useUnicode=true&characterEncoding=utf8&autoReconnect=true')";

    private static final String CREATE_DATABASE = "create database if not exists my_database";

    private static final String CREATE_TEST_TABLE =
            "CREATE TABLE if not exists `test` (\n"
                    + "  `c1` VARCHAR(2147483647),\n"
                    + "  `id` INT NOT NULL,\n"
                    + "  `stime` TIMESTAMP(3),\n"
                    + " `cost` as id * 10, "
                    + "  WATERMARK FOR `stime` AS `stime` - INTERVAL '10' SECOND,\n"
                    + "  CONSTRAINT `PK_3386` PRIMARY KEY (`id`) NOT ENFORCED\n"
                    + ") "
                    + " comment 'test' "
                    + " partitioned by (c1)"
                    + " WITH (\n"
                    + "  'connector' = 'print'\n"
                    + ") ";

    @Test
    public void createDatabase() throws Exception {
        StreamExecutionEnvironment env = new StreamExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.executeSql(CREATE_CATALOG);
        tEnv.executeSql("use catalog my_catalog");
        TableResult tableResult = tEnv.executeSql(CREATE_DATABASE);
        //        tEnv.executeSql("show create my_database");
    }

    @Test
    public void dropDatabase() throws Exception {
        StreamExecutionEnvironment env = new StreamExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.executeSql(CREATE_CATALOG);
        tEnv.executeSql("use catalog my_catalog");
        TableResult tableResult = tEnv.executeSql("drop database if exists my_database");
        //        tEnv.executeSql("show create my_database");
    }

    @Test
    public void createTable() throws Exception {
        StreamExecutionEnvironment env = new StreamExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        tEnv.executeSql(CREATE_CATALOG);
        tEnv.executeSql("use catalog my_catalog");
        tEnv.executeSql("use my_database");
        TableResult tableResult = tEnv.executeSql(CREATE_TEST_TABLE); // RexNodeExpression
        tEnv.executeSql("show create table test").print();
        /*  tEnv.executeSql(
        "create table test (c1 varchar, stime timestamp(3), watermark for stime as stime ) with ('connector' = 'print')");*/
    }
}
