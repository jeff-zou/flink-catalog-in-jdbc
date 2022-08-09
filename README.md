### Instructions for use：

After executing mvn package -DskipTests on the command line, import the generated package
flink-catalog-in-jdbc-1.0.jar into flink lib, no other settings are required.

Development environment engineering direct reference:

```
<dependency>
    <groupId>io.github.jeff-zou</groupId>
    <artifactId>flink-catalog-in-jdbc</artifactId>
    <version>1.1</version>
</dependency>
```

# create tables in your database(support jdbc)

```

CREATE TABLE `flink_catalog_databases` (
  `comment` varchar(100) DEFAULT NULL COMMENT '1',
  `properties` varchar(100) DEFAULT NULL,
  `database_name` varchar(100) DEFAULT NULL,
  UNIQUE KEY `flink_catalog_databases_un` (`database_name`)
) ;

CREATE TABLE `flink_catalog_tables` (
  `script` varchar(5000) DEFAULT NULL COMMENT '1',
  `object_name` varchar(100) DEFAULT NULL,
  `database_name` varchar(100) DEFAULT NULL,
  `kind` varchar(20) DEFAULT NULL,
  `comment` varchar(200) DEFAULT NULL,
  UNIQUE KEY `flink_catalog_databases_un` (`database_name`,`object_name`)
);

CREATE TABLE `flink_catalog_functions` (
  `database_name` varchar(100) DEFAULT NULL,
  `object_name` varchar(100) DEFAULT NULL,
  `class_name` varchar(200) DEFAULT NULL COMMENT '1',
  `function_language` varchar(20) DEFAULT NULL,
  UNIQUE KEY `flink_catalog_functions_un` (`database_name`,`object_name`)
) ;

CREATE TABLE `flink_catalog_columns` (
  `database_name` varchar(100) COLLATE utf8_unicode_ci DEFAULT NULL,
  `object_name` varchar(100) COLLATE utf8_unicode_ci DEFAULT NULL,
  `column_name` varchar(50) COLLATE utf8_unicode_ci DEFAULT NULL,
  `column_type` varchar(100) COLLATE utf8_unicode_ci DEFAULT NULL,
  `column_comment` varchar(200) COLLATE utf8_unicode_ci DEFAULT NULL,
  UNIQUE KEY `flink_catalog_columns_un` (`database_name`,`object_name`,`column_name`)
)
```

### usage
```
create catalog  my_catalog with ( 'type'='generic_in_jdbc', 'default-database'='test', 'username'='test', 'password'='****',
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