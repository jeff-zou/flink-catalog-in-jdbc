package org.apache.flink.connector.jdbc.catalog;

import org.apache.flink.connector.jdbc.catalog.common.EncryptUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Preconditions;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * @Author: Jeff Zou @Date: 2023/10/25 14:04
 */
public class GenericInJdbcCatalogTest {

    private static final String DB_USERNAME = "username";
    private static final String DB_PASSWORD = "password";
    private static final String DB_URL = "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1";

    private static final String CREATE_CATALOG =
            "create catalog  my_catalog with ("
                    + "'type'='generic_in_jdbc', 'default-database'='default', 'username'='"
                    + DB_USERNAME
                    + "', 'password'='"
                    + DB_PASSWORD
                    + "','secret.key'='test',"
                    + " 'url'='"
                    + DB_URL
                    + "')";

    private static final String CREATE_DATABASE = "create database if not exists my_database";

    private static final String CREATE_TEST_TABLE =
            "CREATE TABLE if not exists `test` ("
                    + "  `c1` VARCHAR(2147483647),"
                    + "  `id` INT NOT NULL,"
                    + "  `stime` TIMESTAMP(3),"
                    + " `cost` as id * 10, "
                    + "  WATERMARK FOR `stime` AS `stime` - INTERVAL '10' SECOND"
                    + ") "
                    + " comment 'test' "
                    + " partitioned by (c1)"
                    + " WITH ("
                    + "  'properties.sasl.jaas.config' = 'org.apache.kafka.common.security.plain.PlainLoginModule required username=\"test\"  password=\"test\";',"
                    + "  'connector' = 'kafka',"
                    + "  'topic' = 'test',"
                    + "  'properties.bootstrap.servers'='127.0.0.1:9093',"
                    + "  'format'='json',"
                    + "  'properties.group.id' = 'test',"
                    + "  'properties.acks' = '0',"
                    + "  'properties.value.serializer' = 'org.apache.kafka.common.serialization.ByteArraySerializer',"
                    + "  'properties.enable.auto.commit' = 'true'"
                    + ") ";

    private static final String ENCRYPTED_TEST_TABLE =
            "CREATE TABLE my_database.test (    `c1` VARCHAR(2147483647),   `id` INT NOT NULL,   `stime` TIMESTAMP(3),   `cost` AS `id` * 10,   WATERMARK FOR `stime` AS `stime` - INTERVAL '10' SECOND ) COMMENT 'test'  PARTITIONED BY (`c1`) WITH (   'properties.sasl.jaas.config' = 'org.apache.kafka.common.security.plain.PlainLoginModule required username=\"test\"  password=\"******\";',   'properties.bootstrap.servers' = '127.0.0.1:9093',   'connector' = 'kafka',   'format' = 'json',   'topic' = 'test',   'properties.group.id' = 'test',   'properties.acks' = '0',   'properties.value.serializer' = 'org.apache.kafka.common.serialization.ByteArraySerializer',   'properties.enable.auto.commit' = 'true' ) ";

    @BeforeClass
    public static void initDatabase() throws Exception {

        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USERNAME, DB_PASSWORD)) {
            Statement statement = conn.createStatement();
            statement.executeUpdate(
                    "CREATE TABLE `flink_catalog_databases` (\n"
                            + "  `comment` varchar(100) DEFAULT NULL COMMENT '1',\n"
                            + "  `properties` varchar(100) DEFAULT NULL,\n"
                            + "  `database_name` varchar(100) NOT NULL,\n"
                            + "  `catalog_name` varchar(100) NOT NULL,\n"
                            + "  PRIMARY KEY (`database_name`,`catalog_name`)\n"
                            + ")");
            statement.executeUpdate(
                    "CREATE TABLE `flink_catalog_tables` (\n"
                            + "  `script` varchar(5000) DEFAULT NULL COMMENT '1',\n"
                            + "  `object_name` varchar(100) NOT NULL,\n"
                            + "  `database_name` varchar(100) NOT NULL,\n"
                            + "  `kind` varchar(20) DEFAULT NULL,\n"
                            + "  `comment` varchar(200) DEFAULT NULL,\n"
                            + "  `password` varchar(200) DEFAULT NULL,\n"
                            + "  `catalog_name` varchar(100) NOT NULL,\n"
                            + "  PRIMARY KEY (`catalog_name`,`database_name`,`object_name`)\n"
                            + ") ");
            statement.executeUpdate(
                    "CREATE TABLE `flink_catalog_functions` (\n"
                            + "  `database_name` varchar(100) NOT NULL,\n"
                            + "  `object_name` varchar(100) NOT NULL,\n"
                            + "  `class_name` varchar(200) DEFAULT NULL COMMENT '1',\n"
                            + "  `function_language` varchar(20) DEFAULT NULL,\n"
                            + "  `comment` varchar(500) DEFAULT NULL,\n"
                            + "  `catalog_name` varchar(100) NOT NULL,\n"
                            + "  PRIMARY KEY (`database_name`,`object_name`,`catalog_name`)\n"
                            + ") ");

            statement.executeUpdate(
                    "CREATE TABLE `flink_catalog_columns` (\n"
                            + "  `database_name` varchar(100) NOT NULL,\n"
                            + "  `object_name` varchar(100)  NOT NULL,\n"
                            + "  `column_name` varchar(50)  NOT NULL,\n"
                            + "  `column_type` varchar(100) DEFAULT NULL,\n"
                            + "  `column_comment` varchar(200)  DEFAULT NULL,\n"
                            + "  `catalog_name` varchar(100) NOT NULL,\n"
                            + "  PRIMARY KEY (`database_name`,`object_name`,`catalog_name`,`column_name`)\n"
                            + ") ");
            statement.close();
        }
    }

    @Test
    public void createAnddropDatabase() throws Exception {
        StreamExecutionEnvironment env = new StreamExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.executeSql(CREATE_CATALOG);
        tEnv.executeSql("use catalog my_catalog");
        tEnv.executeSql(CREATE_DATABASE);

        try (Connection connection =
                DriverManager.getConnection(DB_URL, DB_USERNAME, DB_PASSWORD)) {
            Statement statement = connection.createStatement();
            ResultSet resultSet =
                    statement.executeQuery(
                            "select count(1) from flink_catalog_databases  where catalog_name = 'my_catalog' and  database_name='my_database' ");

            while (resultSet.next()) {
                Preconditions.checkArgument(resultSet.getInt(1) == 1);
            }
            resultSet.close();

            tEnv.executeSql("use my_database");
            tEnv.executeSql("drop database if exists my_database");

            resultSet =
                    statement.executeQuery(
                            "select count(1) from flink_catalog_databases  where catalog_name = 'my_catalog' and  database_name='my_database' ");

            while (resultSet.next()) {
                Preconditions.checkArgument(resultSet.getInt(1) == 0);
            }
            resultSet.close();
        }
    }

    @After
    public void clean() {

        StreamExecutionEnvironment env = new StreamExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.executeSql(CREATE_CATALOG);
        tEnv.executeSql("use catalog my_catalog");
        tEnv.executeSql("drop database if exists my_database cascade");
        System.out.println("clean ***********************************");
    }

    @Test
    public void createAndDropTable() throws Exception {
        StreamExecutionEnvironment env = new StreamExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        tEnv.executeSql(CREATE_CATALOG);
        tEnv.executeSql("use catalog my_catalog");
        tEnv.executeSql(CREATE_DATABASE);
        tEnv.executeSql("use my_database");
        tEnv.executeSql(CREATE_TEST_TABLE);
        TableResult tableResult = tEnv.executeSql("show create table test");
        System.out.println("创建表成功：");
        tableResult.print();

        try (Connection connection =
                DriverManager.getConnection(DB_URL, DB_USERNAME, DB_PASSWORD)) {
            Statement statement = connection.createStatement();
            ResultSet resultSet =
                    statement.executeQuery(
                            "select script,password from flink_catalog_tables  where catalog_name = 'my_catalog' and  database_name='my_database' and object_name = 'test'");

            int count = 0;
            while (resultSet.next()) {
                String encryptPassword = resultSet.getString(2);

                String script =
                        resultSet.getString(1).replaceAll("\\r", " ").replaceAll("\\n", " ");
                Preconditions.checkArgument(ENCRYPTED_TEST_TABLE.equals(script));

                Preconditions.checkArgument(
                        encryptPassword.equals(EncryptUtil.encrypt("test", "test")));
                count++;
            }
            resultSet.close();

            Preconditions.checkState(count == 1);

            resultSet =
                    statement.executeQuery(
                            "select count(1) from flink_catalog_columns where catalog_name = 'my_catalog' and  database_name='my_database' and object_name = 'test'");

            while (resultSet.next()) {
                Preconditions.checkArgument(resultSet.getInt(1) == 4);
            }

            tEnv.executeSql("drop table test");

            resultSet =
                    statement.executeQuery(
                            "select count(1) from flink_catalog_columns where catalog_name = 'my_catalog' and  database_name='my_database' and object_name = 'test'");

            while (resultSet.next()) {
                Preconditions.checkArgument(resultSet.getInt(1) == 0);
            }
            resultSet.close();

            resultSet =
                    statement.executeQuery(
                            "select count(1) from flink_catalog_tables  where catalog_name = 'my_catalog' and  database_name='my_database' and object_name = 'test'");

            while (resultSet.next()) {
                Preconditions.checkArgument(resultSet.getInt(1) == 0);
            }
            resultSet.close();

            System.out.println("删除表成功");
        }
    }

    @Test
    public void createAnddropFunction() throws Exception {
        StreamExecutionEnvironment env = new StreamExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.executeSql(CREATE_CATALOG);
        tEnv.executeSql("use catalog my_catalog");
        tEnv.executeSql(CREATE_DATABASE);
        tEnv.executeSql("use my_database");
        tEnv.executeSql("create function test as 'com.xsj.realtime.udx.Test'");
        try (Connection connection =
                DriverManager.getConnection(DB_URL, DB_USERNAME, DB_PASSWORD)) {
            Statement statement = connection.createStatement();
            ResultSet resultSet =
                    statement.executeQuery(
                            "select count(1) from flink_catalog_functions  where catalog_name = 'my_catalog' and  database_name='my_database' and object_name='test' and class_name='com.xsj.realtime.udx.Test'");

            while (resultSet.next()) {
                Preconditions.checkArgument(resultSet.getInt(1) == 1);
            }
            resultSet.close();

            tEnv.executeSql("drop function if exists test");

            resultSet =
                    statement.executeQuery(
                            "select count(1) from flink_catalog_functions  where catalog_name = 'my_catalog' and  database_name='my_database' and object_name='test' and class_name='com.xsj.realtime.udx.Test'");

            while (resultSet.next()) {
                Preconditions.checkArgument(resultSet.getInt(1) == 0);
            }
            resultSet.close();
        }
    }

    @Test
    public void createAndDropView() throws Exception {
        StreamExecutionEnvironment env = new StreamExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        tEnv.executeSql(CREATE_CATALOG);
        tEnv.executeSql("use catalog my_catalog");
        tEnv.executeSql(CREATE_DATABASE);
        tEnv.executeSql("use my_database");
        tEnv.executeSql(CREATE_TEST_TABLE);
        System.out.println("创建表成功：");

        String createView =
                "CREATE VIEW my_database.test_view(`c1`, `id`) as  SELECT `test`.`c1`, `test`.`id`  FROM `my_catalog`.`my_database`.`test`  WHERE `test`.`c1` = '2'";
        tEnv.executeSql("create view test_view as select c1, id from test where c1 = '2'");
        System.out.println("创建视图成功：");
        try (Connection connection =
                DriverManager.getConnection(DB_URL, DB_USERNAME, DB_PASSWORD)) {
            Statement statement = connection.createStatement();
            ResultSet resultSet =
                    statement.executeQuery(
                            "select script,password from flink_catalog_tables  where catalog_name = 'my_catalog' and  database_name='my_database' and object_name = 'test_view'");

            while (resultSet.next()) {

                String script =
                        resultSet.getString(1).replaceAll("\\r", " ").replaceAll("\\n", " ");
                Preconditions.checkArgument(createView.equals(script));
            }
            resultSet.close();

            resultSet =
                    statement.executeQuery(
                            "select count(1) from flink_catalog_columns where catalog_name = 'my_catalog' and  database_name='my_database' and object_name = 'test_view'");

            while (resultSet.next()) {
                Preconditions.checkArgument(resultSet.getInt(1) == 2);
            }

            tEnv.executeSql("drop view test_view");

            resultSet =
                    statement.executeQuery(
                            "select count(1) from flink_catalog_columns where catalog_name = 'my_catalog' and  database_name='my_database' and object_name = 'test_view'");

            while (resultSet.next()) {
                Preconditions.checkArgument(resultSet.getInt(1) == 0);
            }
            resultSet.close();

            resultSet =
                    statement.executeQuery(
                            "select count(1) from flink_catalog_tables  where catalog_name = 'my_catalog' and  database_name='my_database' and object_name = 'test_view'");

            while (resultSet.next()) {
                Preconditions.checkArgument(resultSet.getInt(1) == 0);
            }
            resultSet.close();

            System.out.println("删除视图成功");
        }
    }

    @Test
    public void testLoadViewAndTable() throws Exception {
        StreamExecutionEnvironment env = new StreamExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        tEnv.executeSql(CREATE_CATALOG);
        tEnv.executeSql("use catalog my_catalog");
        tEnv.executeSql(CREATE_DATABASE);
        tEnv.executeSql("use my_database");
        tEnv.executeSql(CREATE_TEST_TABLE);
        System.out.println("创建表成功：");

        String createView =
                "CREATE VIEW my_database.test_view(`c1`, `id`) as  SELECT `test`.`c1`, `test`.`id`  FROM `my_catalog`.`my_database`.`test`  WHERE `test`.`c1` = '2'";
        tEnv.executeSql("create view test_view as select c1, id from test where c1 = '2'");
        System.out.println("创建视图成功：");
        try (Connection connection =
                DriverManager.getConnection(DB_URL, DB_USERNAME, DB_PASSWORD)) {
            Statement statement = connection.createStatement();
            ResultSet resultSet =
                    statement.executeQuery(
                            "select script,password from flink_catalog_tables  where catalog_name = 'my_catalog' and  database_name='my_database' and object_name = 'test_view'");

            while (resultSet.next()) {

                String script =
                        resultSet.getString(1).replaceAll("\\r", " ").replaceAll("\\n", " ");
                Preconditions.checkArgument(createView.equals(script));
            }
            resultSet.close();

            resultSet =
                    statement.executeQuery(
                            "select count(1) from flink_catalog_columns where catalog_name = 'my_catalog' and  database_name='my_database' and object_name = 'test_view'");

            while (resultSet.next()) {
                Preconditions.checkArgument(resultSet.getInt(1) == 2);
            }
            resultSet.close();
        }
        env.close();

        env = StreamExecutionEnvironment.createLocalEnvironment();
        tEnv = StreamTableEnvironment.create(env);
        tEnv.executeSql(CREATE_CATALOG);
    }
}
