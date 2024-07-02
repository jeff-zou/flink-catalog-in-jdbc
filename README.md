### Instructions for use：

After executing mvn package on the command line, import the generated package
flink-catalog-in-jdbc-2.0.1.jar into flink(support flink 1.16) lib, no other settings are required.

Development environment engineering direct reference:

```
<dependency>
    <groupId>io.github.jeff-zou</groupId>
    <artifactId>flink-catalog-in-jdbc</artifactId>
    <version>2.0.1</version>
</dependency>
```

# create tables in your database(support jdbc)

```

CREATE TABLE `flink_catalog_databases` (
   `comment` varchar(100) DEFAULT NULL COMMENT '1',
   `properties` varchar(100) DEFAULT NULL,
   `database_name` varchar(100) NOT NULL,
   `catalog_name` varchar(100) NOT NULL,
   PRIMARY KEY (`catalog_name`, `database_name`)
) ;

CREATE TABLE `flink_catalog_tables` (
    `script` varchar(5000) DEFAULT NULL COMMENT '1',
    `object_name` varchar(100) NOT NULL,
    `database_name` varchar(100) NOT NULL,
    `kind` varchar(20) DEFAULT NULL,
    `comment` varchar(200) DEFAULT NULL,
    `password` varchar(200) DEFAULT NULL,
    `catalog_name` varchar(100) NOT NULL,
    `schema_properties` varchar(5000) NOT NULL,
    PRIMARY KEY (`catalog_name`,`database_name`,`object_name`)
) ;

CREATE TABLE `flink_catalog_functions` (
   `database_name` varchar(100) NOT NULL,
   `object_name` varchar(100) NOT NULL,
   `class_name` varchar(200) DEFAULT NULL COMMENT '1',
   `function_language` varchar(20) DEFAULT NULL,
   `comment` varchar(500) DEFAULT NULL,
   `catalog_name` varchar(100) NOT NULL,
    PRIMARY KEY (`catalog_name`, `database_name`,`object_name`)
) ;

CREATE TABLE `flink_catalog_columns` (
    `database_name` varchar(100) NOT NULL,
    `object_name` varchar(100) NOT NULL,
    `column_name` varchar(50) NOT NULL,
    `column_type` varchar(100) DEFAULT NULL,
    `column_comment` varchar(200) DEFAULT NULL,
    `catalog_name` varchar(100) NOT NULL,
    PRIMARY KEY (`catalog_name`,`database_name`,`object_name`,`column_name`)
) ;
```

### usage
```
create catalog  my_catalog with ( 'type'='generic_in_jdbc', 'default-database'='test', 'username'='test', 'password'='****','secret.key'='****'
'url'='jdbc:mysql://*****:3306/test_database?useUnicode=true&characterEncoding=utf8&autoReconnect=true');

use catalog my_catalog;
 
create database if not exists my_database;

use  my_database;

CREATE TABLE if not exists `test` (
                      `c1` VARCHAR(2147483647),
                      `id` INT NOT NULL,
                      `stime` TIMESTAMP(3),
                     `cost` as id * 10, "
                      WATERMARK FOR `stime` AS `stime` - INTERVAL '10' SECOND,
                      CONSTRAINT `PK_3386` PRIMARY KEY (`id`) NOT ENFORCED
                    ) "
                     comment 'test' "
                     partitioned by (c1)"
                     WITH (
                      'connector' = 'print'
                    ) ";
                    
 show create table test;                   
```
Parameter Description</br>


| Field                    | Default                       | Type   | Description                                                                                                                                                   |
|--------------------------|-------------------------------|--------|---------------------------------------------------------------------------------------------------------------------------------------------------------------|
| username                 |                               | String | the username of DB                                                                                                                                            |
| password                 |                               | String | the password of DB                                                                                                                                            |
| secret.key               |                               | String | The key used for encrypting the password which in the metadata, it should not be modified after the data is saved, otherwise the metadata cannot be restored. |
| url                      |                               | String | the url of DB                                                                                                                                                 |
| jdbc.driver.class        | com.mysql.cj.jdbc.Driver      | String | the driver class name of jdbc                                                                                                                                 |
| connection.test.query    | SELECT 1                      | String | the test query for connection                                                                                                                                 |
| pool.max.size            | 2                             | int    | max  size of pool                                                                                                                                             |
| pool.min.idle            | 1                             | int    | min idle of connection                                                                                                                                        |
| max.life.time            | 1800000                       | int    | set maximum lifetime for the connection (milliseconds)                                                                                                        |
| connect.time.out         | 30000                         | int    | set connection timeout (milliseconds)                                                                                                                         |


