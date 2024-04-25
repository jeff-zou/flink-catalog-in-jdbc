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