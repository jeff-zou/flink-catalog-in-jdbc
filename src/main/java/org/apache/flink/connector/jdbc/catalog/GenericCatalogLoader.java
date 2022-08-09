package org.apache.flink.connector.jdbc.catalog;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.operations.ddl.CreateTableOperation;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
/**
 * @Author: Jeff Zou @Date: 2022/8/5 11:01
 */
public class GenericCatalogLoader {

    private FlinkParser flinkParser;

    public void load(
            String url,
            String username,
            String pwd,
            GenericInJdbcCatalog genericInJdbcCatalog,
            ObjectMapper objectMapper)
            throws Exception {
        flinkParser = new FlinkParser();
        try (Connection conn = DriverManager.getConnection(url, username, pwd)) {
            // create databases in memory
            PreparedStatement pstmt =
                    conn.prepareStatement(
                            "select database_name, comment, properties from  flink_catalog_databases");
            ResultSet resultSet = pstmt.executeQuery();
            while (resultSet.next()) {
                Map<String, String> properties =
                        (Map<String, String>)
                                objectMapper.readValue(resultSet.getString(3), Map.class);
                CatalogDatabase catalogDatabase =
                        new CatalogDatabaseImpl(properties, resultSet.getString(2));
                genericInJdbcCatalog.loadDatabase(resultSet.getString(1), catalogDatabase, true);
            }
            resultSet.close();

            // create table in memory
            pstmt =
                    conn.prepareStatement(
                            "select database_name, object_name, kind, script  from  flink_catalog_tables");
            resultSet = pstmt.executeQuery();
            while (resultSet.next()) {
                CreateTableOperation createTableOperation =
                        (CreateTableOperation) flinkParser.parse(resultSet.getString(4));
                flinkParser.creatTable(createTableOperation);
                ObjectPath objectPath =
                        new ObjectPath(resultSet.getString(1), resultSet.getString(2));
                genericInJdbcCatalog.loadTable(
                        objectPath, flinkParser.getTable(objectPath.getObjectName()), false);
            }
            resultSet.close();

            // craete function in memory
            /*    pstmt =
                    conn.prepareStatement(
                            "select database_name, object_name, kind, script  from  flink_catalog_tables");
            resultSet = pstmt.executeQuery();
            while (resultSet.next()) {
                CreateTableOperation createTableOperation =
                        (CreateTableOperation) flinkParser.parse(resultSet.getString(4));
                flinkParser.creatTable(createTableOperation);
                ObjectPath objectPath =
                        new ObjectPath(resultSet.getString(1), resultSet.getString(2));
                genericInJdbcCatalog.loadTable(
                        objectPath, flinkParser.getTable(objectPath.getObjectName()), false);
            }*/
            resultSet.close();
        } catch (SQLException e) {
            throw new ValidationException(
                    String.format("Failed connecting to %s via JDBC.", url), e);
        }
    }
}
