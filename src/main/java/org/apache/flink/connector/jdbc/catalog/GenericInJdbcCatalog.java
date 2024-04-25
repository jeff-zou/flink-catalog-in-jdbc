/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.jdbc.catalog;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

import org.apache.flink.connector.jdbc.catalog.common.DesensitiveUtil;
import org.apache.flink.connector.jdbc.catalog.common.EncryptUtil;
import org.apache.flink.connector.jdbc.catalog.common.FlinkParser;
import org.apache.flink.connector.jdbc.catalog.common.ShowCreateUtils;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.*;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotEmptyException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.FunctionAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.FunctionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionAlreadyExistsException;
import org.apache.flink.table.catalog.exceptions.PartitionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionSpecInvalidException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotPartitionedException;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.functions.FunctionIdentifier;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.ddl.CreateTableOperation;
import org.apache.flink.table.operations.ddl.CreateViewOperation;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/** A generic catalog implementation that holds all meta objects in jdbc. */
public class GenericInJdbcCatalog extends GenericInMemoryCatalog {

    private static final Logger LOG = LoggerFactory.getLogger(GenericInJdbcCatalog.class);

    public static final String CATALOG_LOAD_TARGET_DATABASE_SEPERATOR = ",";

    private final String username;
    private final String pwd;
    private final String url;
    private ObjectMapper objectMapper;
    private FlinkParser flinkParser;
    private boolean hasLoaded = false;

    private final String catalogName;

    private final String secretKey;

    private final String targetDatabases;

    public GenericInJdbcCatalog(
            String catalogName,
            String defaultDatabase,
            String username,
            String pwd,
            String url,
            String secretKey,
            String targetDatabases) {
        super(catalogName, defaultDatabase);

        checkArgument(!StringUtils.isNullOrWhitespaceOnly(username));
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(pwd));
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(url));

        validateJdbcUrl(url);
        this.catalogName = catalogName;
        this.username = username;
        this.pwd = pwd;
        this.url = url;
        this.secretKey = secretKey;
        this.targetDatabases = targetDatabases;
        objectMapper = new ObjectMapper();
    }

    private void validateJdbcUrl(String url) {
        String[] parts = url.trim().split("\\/+");
        // Preconditions.checkArgument(parts.length == 3);
    }

    @Override
    public void open() throws CatalogException {
        if (hasLoaded) {
            return;
        }

        try {
            load(targetDatabases, objectMapper);
        } catch (Exception e1) {
            throw new ValidationException(
                    String.format("Failed load catalog data from jdbc %s .", this.url), e1);
        }

        super.open();
        LOG.info("Catalog {} established connection to {}", getName(), url);
        hasLoaded = true;
    }

    @Override
    public void close() throws CatalogException {
        super.close();
        LOG.info("Catalog {} closing", getName());
    }

    // ------ databases ------
    @Override
    public void createDatabase(
            String databaseName, CatalogDatabase database, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException, CatalogException {
        boolean exists = super.databaseExists(databaseName);
        if (exists && !ignoreIfExists) {
            throw new DatabaseAlreadyExistException(getName(), databaseName);
        } else if (exists) {
            return;
        }

        try (Connection conn = DriverManager.getConnection(url, username, pwd)) {
            PreparedStatement pstmt =
                    conn.prepareStatement(
                            "insert into flink_catalog_databases (database_name, comment, properties,catalog_name ) values (?, ?, ?, ?)");
            pstmt.setString(1, databaseName);
            pstmt.setString(2, database.getComment());
            pstmt.setString(3, objectMapper.writeValueAsString(database.getProperties()));
            pstmt.setString(4, catalogName);
            pstmt.execute();
            super.createDatabase(databaseName, database, ignoreIfExists);
        } catch (SQLException e) {
            throw new ValidationException(String.format("Failed create database %s.", database), e);
        } catch (JsonProcessingException e1) {
            throw new ValidationException(String.format("Failed format map to json."), e1);
        }
    }

    @Override
    public void dropDatabase(String databaseName, boolean ignoreIfNotExists, boolean cascade)
            throws DatabaseNotExistException, DatabaseNotEmptyException, CatalogException {
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(databaseName));
        if (!ignoreIfNotExists && !super.databaseExists(databaseName)) {
            throw new DatabaseNotExistException(getName(), databaseName);
        } else if (!super.databaseExists(databaseName)) {
            return;
        }

        if (!cascade) {
            if (isEmptyDatabase(databaseName)) {
                try (Connection conn = DriverManager.getConnection(url, username, pwd)) {
                    PreparedStatement pstmt =
                            conn.prepareStatement(
                                    "delete from  flink_catalog_databases where database_name = ? and catalog_name=?");
                    pstmt.setString(1, databaseName);
                    pstmt.setString(2, catalogName);
                    pstmt.execute();
                    super.dropDatabase(databaseName, ignoreIfNotExists, cascade);
                } catch (Exception e) {
                    throw new ValidationException(
                            String.format("Failed drop database %s.", databaseName), e);
                }
            } else {
                throw new DatabaseNotEmptyException(getName(), databaseName);
            }
        } else {
            dropDatabaseCascade(databaseName);
        }
    }

    private void dropDatabaseCascade(String databaseName) {
        try (Connection conn = DriverManager.getConnection(url, username, pwd)) {
            PreparedStatement pstmt =
                    conn.prepareStatement(
                            "delete from  flink_catalog_databases where database_name = ? and catalog_name=?");
            pstmt.setString(1, databaseName);
            pstmt.setString(2, catalogName);
            pstmt.execute();
            super.dropDatabase(databaseName, true, true);
            pstmt =
                    conn.prepareStatement(
                            "delete from  flink_catalog_tables where database_name = ? and catalog_name=?");
            pstmt.setString(1, databaseName);
            pstmt.setString(2, catalogName);
            pstmt.execute();

            pstmt =
                    conn.prepareStatement(
                            "delete from  flink_catalog_functions where database_name = ? and catalog_name=?");
            pstmt.setString(1, databaseName);
            pstmt.setString(2, catalogName);
            pstmt.execute();
        } catch (Exception e) {
            throw new ValidationException(
                    String.format("Failed drop database %s.", databaseName), e);
        }
    }

    private boolean isEmptyDatabase(String databaseName) throws DatabaseNotExistException {
        return listTables(databaseName).isEmpty() && listFunctions(databaseName).isEmpty();
    }

    @Override
    public void alterDatabase(
            String databaseName, CatalogDatabase newDatabase, boolean ignoreIfNotExists)
            throws DatabaseNotExistException, CatalogException {
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(databaseName));
        checkNotNull(newDatabase);

        CatalogDatabase existingDatabase = super.getDatabase(databaseName);

        if (existingDatabase != null) {
            try (Connection conn = DriverManager.getConnection(url, username, pwd)) {
                PreparedStatement pstmt =
                        conn.prepareStatement(
                                "insert into flink_catalog_databases (database_name, comment, properties, catalog_name ) values (?, ?, ?, ?)");
                pstmt.setString(1, databaseName);
                pstmt.setString(2, newDatabase.getComment());
                pstmt.setString(3, objectMapper.writeValueAsString(newDatabase.getProperties()));
                pstmt.setString(4, catalogName);
                pstmt.execute();
                super.alterDatabase(databaseName, newDatabase, ignoreIfNotExists);
            } catch (SQLException e) {
                throw new ValidationException(
                        String.format("Failed drop database %s.", databaseName), e);
            } catch (JsonProcessingException e1) {
                throw new ValidationException(String.format("Failed format map to json."), e1);
            }
        } else if (!ignoreIfNotExists) {
            throw new DatabaseNotExistException(getName(), databaseName);
        }
    }

    // ------ tables and views ------
    @Override
    public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException {
        checkNotNull(tablePath);
        checkNotNull(table);

        if (!databaseExists(tablePath.getDatabaseName())) {
            throw new DatabaseNotExistException(getName(), tablePath.getDatabaseName());
        }

        if (tableExists(tablePath)) {
            if (!ignoreIfExists) {
                throw new TableAlreadyExistException(getName(), tablePath);
            }
        } else {
            try (Connection conn = DriverManager.getConnection(url, username, pwd)) {
                conn.setAutoCommit(false);
                String password = DesensitiveUtil.desensitiveForProperties(table.getOptions());
                PreparedStatement pstmt =
                        conn.prepareStatement(
                                "insert into flink_catalog_tables (database_name, object_name, kind, script, comment, `password`, catalog_name) values (?, ?, ?, ?, ?, ?, ?)");
                pstmt.setString(1, tablePath.getDatabaseName());
                pstmt.setString(2, tablePath.getObjectName());
                pstmt.setString(3, table.getTableKind().name());
                pstmt.setString(
                        4,
                        table.getTableKind() == CatalogBaseTable.TableKind.TABLE
                                ? ShowCreateUtils.buildShowCreateTableRow(
                                        (ResolvedCatalogTable) table,
                                        tablePath.getDatabaseName(),
                                        tablePath.getObjectName(),
                                        false)
                                : ShowCreateUtils.buildShowCreateViewRow(
                                        (ResolvedCatalogView) table,
                                        tablePath.getDatabaseName(),
                                        tablePath.getObjectName(),
                                        false));
                pstmt.setString(5, table.getComment());
                pstmt.setString(
                        6, password == null ? null : EncryptUtil.encrypt(password, secretKey));
                pstmt.setString(7, catalogName);
                pstmt.execute();
                batchSaveColumns(
                        tablePath,
                        ((ResolvedCatalogBaseTable<?>) table).getResolvedSchema(),
                        pstmt,
                        conn);
                conn.commit();

                super.createTable(tablePath, table, ignoreIfExists);

            } catch (SQLException e) {
                throw new ValidationException(
                        String.format("Failed drop database %s.", tablePath.getDatabaseName()), e);
            }
        }
    }

    private void batchSaveColumns(
            ObjectPath tablePath,
            ResolvedSchema resolvedSchema,
            PreparedStatement pstmt,
            Connection conn)
            throws SQLException {
        pstmt =
                conn.prepareStatement(
                        "insert into flink_catalog_columns (database_name, object_name, column_name , column_type, column_comment, catalog_name) values (?, ?, ?, ?, ?, ? )");

        List<Column> columns = resolvedSchema.getColumns();

        for (Column column : columns) {
            pstmt.setString(1, tablePath.getDatabaseName());
            pstmt.setString(2, tablePath.getObjectName());
            pstmt.setString(3, column.getName());
            pstmt.setString(4, column.getDataType().toString());
            pstmt.setString(5, column.getComment().orElse(""));
            pstmt.setString(6, catalogName);
            pstmt.addBatch();
        }
        pstmt.executeBatch();
    }

    @Override
    public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        checkNotNull(tablePath);
        if (tableExists(tablePath)) {
            try (Connection conn = DriverManager.getConnection(url, username, pwd)) {
                conn.setAutoCommit(false);
                PreparedStatement pstmt =
                        conn.prepareStatement(
                                "delete from flink_catalog_tables where database_name = ? and  object_name = ? and catalog_name = ?");
                pstmt.setString(1, tablePath.getDatabaseName());
                pstmt.setString(2, tablePath.getObjectName());
                pstmt.setString(3, catalogName);
                pstmt.execute();

                pstmt =
                        conn.prepareStatement(
                                "delete from flink_catalog_columns where database_name = ? and  object_name = ? and catalog_name = ?");
                pstmt.setString(1, tablePath.getDatabaseName());
                pstmt.setString(2, tablePath.getObjectName());
                pstmt.setString(3, catalogName);
                pstmt.execute();
                conn.commit();

                super.dropTable(tablePath, ignoreIfNotExists);
            } catch (SQLException e) {
                throw new ValidationException(
                        String.format(
                                "Failed drop table for %s.%s",
                                tablePath.getDatabaseName(), tablePath.getObjectName()),
                        e);
            }
        } else if (!ignoreIfNotExists) {
            throw new TableNotExistException(getName(), tablePath);
        }
    }

    @Override
    public void renameTable(ObjectPath tablePath, String newTableName, boolean ignoreIfNotExists)
            throws TableNotExistException, TableAlreadyExistException, CatalogException {
        checkNotNull(tablePath);
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(newTableName));
        CatalogBaseTable catalogBaseTable = getTable(tablePath);
        if (catalogBaseTable != null) {
            ObjectPath newPath = new ObjectPath(tablePath.getDatabaseName(), newTableName);
            if (tableExists(newPath)) {
                throw new TableAlreadyExistException(getName(), newPath);
            } else {
                try (Connection conn = DriverManager.getConnection(url, username, pwd)) {
                    conn.setAutoCommit(false);
                    PreparedStatement pstmt =
                            conn.prepareStatement(
                                    "update flink_catalog_tables set object_name=?, script = ? where database_name = ? and  object_name = ? and catalog_name = ?");
                    pstmt.setString(1, newTableName);
                    ResolvedCatalogTable catalogTable = (ResolvedCatalogTable) catalogBaseTable;
                    pstmt.setString(
                            2,
                            catalogTable.getTableKind() == CatalogBaseTable.TableKind.TABLE
                                    ? ShowCreateUtils.buildShowCreateTableRow(
                                            catalogTable,
                                            tablePath.getDatabaseName(),
                                            newTableName,
                                            false)
                                    : ShowCreateUtils.buildShowCreateViewRow(
                                            catalogTable,
                                            tablePath.getDatabaseName(),
                                            newTableName,
                                            false));
                    pstmt.setString(3, tablePath.getDatabaseName());
                    pstmt.setString(4, tablePath.getObjectName());
                    pstmt.setString(5, catalogName);
                    pstmt.execute();

                    pstmt =
                            conn.prepareStatement(
                                    "update flink_catalog_columns set object_name=? where database_name = ? and  object_name = ? and catalog_name = ?");
                    pstmt.setString(1, newTableName);
                    pstmt.setString(2, tablePath.getDatabaseName());
                    pstmt.setString(3, tablePath.getObjectName());
                    pstmt.setString(4, catalogName);
                    pstmt.execute();
                    conn.commit();

                    super.renameTable(tablePath, newTableName, ignoreIfNotExists);
                } catch (SQLException e) {
                    throw new ValidationException(
                            String.format(
                                    "Failed rename table for %s.%s to %s",
                                    tablePath.getDatabaseName(),
                                    tablePath.getObjectName(),
                                    newTableName),
                            e);
                }
            }
        } else if (!ignoreIfNotExists) {
            throw new TableNotExistException(getName(), tablePath);
        }
    }

    @Override
    public void alterTable(
            ObjectPath tablePath, CatalogBaseTable newTable, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        checkNotNull(tablePath);
        checkNotNull(newTable);

        CatalogBaseTable existingTable = getTable(tablePath);

        if (existingTable != null) {
            if (existingTable.getTableKind() != newTable.getTableKind()) {
                throw new CatalogException(
                        String.format(
                                "Table types don't match. Existing table is '%s' and new table is '%s'.",
                                existingTable.getTableKind(), newTable.getTableKind()));
            }

            try (Connection conn = DriverManager.getConnection(url, username, pwd)) {
                PreparedStatement pstmt =
                        conn.prepareStatement(
                                "update flink_catalog_tables set script = ? where database_name = ? and object_name= ? and catalog_name = ?");
                pstmt.setString(2, tablePath.getDatabaseName());
                pstmt.setString(3, tablePath.getObjectName());
                ResolvedCatalogTable catalogTable = (ResolvedCatalogTable) newTable;
                pstmt.setString(
                        1,
                        newTable.getTableKind() == CatalogBaseTable.TableKind.TABLE
                                ? ShowCreateUtils.buildShowCreateTableRow(
                                        catalogTable,
                                        tablePath.getDatabaseName(),
                                        tablePath.getObjectName(),
                                        false)
                                : ShowCreateUtils.buildShowCreateViewRow(
                                        catalogTable,
                                        tablePath.getDatabaseName(),
                                        tablePath.getObjectName(),
                                        false));
                pstmt.setString(4, catalogName);
                pstmt.execute();
                super.alterTable(tablePath, newTable, ignoreIfNotExists);
            } catch (SQLException e) {
                throw new ValidationException(
                        String.format(
                                "Failed alter table %s.%s",
                                tablePath.getDatabaseName(), tablePath.getObjectName()),
                        e);
            }
        } else if (!ignoreIfNotExists) {
            throw new TableNotExistException(getName(), tablePath);
        }
    }

    // ------ partitions ------

    @Override
    public void createPartition(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogPartition partition,
            boolean ignoreIfExists)
            throws TableNotExistException,
                    TableNotPartitionedException,
                    PartitionSpecInvalidException,
                    PartitionAlreadyExistsException,
                    CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropPartition(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec, boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterPartition(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogPartition newPartition,
            boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    // ------ functions ------
    @Override
    public void createFunction(ObjectPath path, CatalogFunction function, boolean ignoreIfExists)
            throws FunctionAlreadyExistException, DatabaseNotExistException, CatalogException {
        Preconditions.checkNotNull(path);
        Preconditions.checkNotNull(function);
        ObjectPath functionPath = this.normalize(path);
        if (!this.databaseExists(functionPath.getDatabaseName())) {
            throw new DatabaseNotExistException(this.getName(), functionPath.getDatabaseName());
        } else {
            if (this.functionExists(functionPath)) {
                if (!ignoreIfExists) {
                    throw new FunctionAlreadyExistException(this.getName(), functionPath);
                }
            } else {
                try (Connection conn = DriverManager.getConnection(url, username, pwd)) {
                    PreparedStatement pstmt =
                            conn.prepareStatement(
                                    "insert into flink_catalog_functions (database_name, object_name,  class_name, function_language, catalog_name ) values (?, ?, ?, ?, ?)");
                    pstmt.setString(1, path.getDatabaseName());
                    pstmt.setString(2, path.getObjectName());
                    pstmt.setString(3, function.getClassName());
                    pstmt.setString(4, function.getFunctionLanguage().name());
                    pstmt.setString(5, catalogName);
                    pstmt.execute();
                    super.createFunction(path, function, ignoreIfExists);
                } catch (SQLException e) {
                    throw new ValidationException(
                            String.format(
                                    "Failed create function %s.%s",
                                    path.getDatabaseName(), path.getObjectName()),
                            e);
                }
            }
        }
    }

    @Override
    public void alterFunction(
            ObjectPath path, CatalogFunction newFunction, boolean ignoreIfNotExists)
            throws FunctionNotExistException, CatalogException {
        checkNotNull(path);
        checkNotNull(newFunction);

        ObjectPath functionPath = normalize(path);

        CatalogFunction existingFunction = getFunction(functionPath);

        if (existingFunction != null) {
            if (existingFunction.getClass() != newFunction.getClass()) {
                throw new CatalogException(
                        String.format(
                                "Function types don't match. Existing function is '%s' and new function is '%s'.",
                                existingFunction.getClass().getName(),
                                newFunction.getClass().getName()));
            }

            try (Connection conn = DriverManager.getConnection(url, username, pwd)) {
                PreparedStatement pstmt =
                        conn.prepareStatement(
                                "update flink_catalog_functions set class_name = ?, function_language =? where database_name = ? and object_name = ? and catalog_name = ?");
                pstmt.setString(1, newFunction.getClassName());
                pstmt.setString(2, newFunction.getFunctionLanguage().name());
                pstmt.setString(3, path.getDatabaseName());
                pstmt.setString(4, path.getObjectName());
                pstmt.setString(5, catalogName);
                pstmt.execute();
                super.alterFunction(path, newFunction, ignoreIfNotExists);
            } catch (SQLException e) {
                throw new ValidationException(
                        String.format(
                                "Failed alter fuction %s.%s",
                                path.getDatabaseName(), path.getObjectName()),
                        e);
            }
        } else if (!ignoreIfNotExists) {
            throw new FunctionNotExistException(getName(), functionPath);
        }
    }

    @Override
    public void dropFunction(ObjectPath path, boolean ignoreIfNotExists)
            throws FunctionNotExistException, CatalogException {
        checkNotNull(path);

        ObjectPath functionPath = normalize(path);

        if (functionExists(functionPath)) {
            try (Connection conn = DriverManager.getConnection(url, username, pwd)) {
                PreparedStatement pstmt =
                        conn.prepareStatement(
                                "delete from flink_catalog_functions where database_name = ? and object_name =? and catalog_name = ?");
                pstmt.setString(1, path.getDatabaseName());
                pstmt.setString(2, path.getObjectName());
                pstmt.setString(3, catalogName);
                pstmt.execute();
                super.dropFunction(path, ignoreIfNotExists);
            } catch (Exception e) {
                throw new ValidationException(
                        String.format(
                                "Failed drop table %s.%s",
                                path.getDatabaseName(), path.getObjectName()),
                        e);
            }
        } else if (!ignoreIfNotExists) {
            throw new FunctionNotExistException(getName(), functionPath);
        }
    }

    private ObjectPath normalize(ObjectPath path) {
        return new ObjectPath(
                path.getDatabaseName(), FunctionIdentifier.normalizeName(path.getObjectName()));
    }

    // ------ stats ------
    @Override
    public void alterTableStatistics(
            ObjectPath tablePath, CatalogTableStatistics tableStatistics, boolean ignoreIfNotExists)
            throws TableNotExistException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterTableColumnStatistics(
            ObjectPath tablePath,
            CatalogColumnStatistics columnStatistics,
            boolean ignoreIfNotExists)
            throws TableNotExistException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterPartitionStatistics(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogTableStatistics partitionStatistics,
            boolean ignoreIfNotExists)
            throws PartitionNotExistException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterPartitionColumnStatistics(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogColumnStatistics columnStatistics,
            boolean ignoreIfNotExists)
            throws PartitionNotExistException {
        throw new UnsupportedOperationException();
    }

    private void load(String targetDatabases, ObjectMapper objectMapper) throws Exception {
        String databaseCondition = " ";
        if (!StringUtils.isNullOrWhitespaceOnly(targetDatabases)) {
            StringBuilder stringBuilder = new StringBuilder(" and database_name in (");
            String[] dbs = targetDatabases.split(CATALOG_LOAD_TARGET_DATABASE_SEPERATOR);
            for (int i = 0; i < dbs.length; i++) {
                stringBuilder.append("'").append(dbs[i].trim()).append("'");
                if (i != dbs.length - 1) {
                    stringBuilder.append(CATALOG_LOAD_TARGET_DATABASE_SEPERATOR);
                }
            }
            stringBuilder.append(")");

            databaseCondition = stringBuilder.toString();
        }

        flinkParser = new FlinkParser(catalogName);
        try (Connection conn = DriverManager.getConnection(url, username, pwd)) {
            // create databases in memory
            PreparedStatement pstmt =
                    conn.prepareStatement(
                            "select database_name, comment, properties from  flink_catalog_databases where catalog_name = ? "
                                    + databaseCondition);
            pstmt.setString(1, catalogName);
            ResultSet resultSet = pstmt.executeQuery();
            while (resultSet.next()) {
                Map<String, String> properties =
                        (Map<String, String>)
                                objectMapper.readValue(resultSet.getString(3), Map.class);
                CatalogDatabase catalogDatabase =
                        new CatalogDatabaseImpl(properties, resultSet.getString(2));
                flinkParser.createDatabase(resultSet.getString(1), catalogDatabase);
                super.createDatabase(resultSet.getString(1), catalogDatabase, true);
            }
            resultSet.close();

            // create table and view in memory
            pstmt =
                    conn.prepareStatement(
                            "select database_name, object_name, kind, script, password  from  flink_catalog_tables where catalog_name = ? "
                                    + databaseCondition
                                    + " order by kind ");
            pstmt.setString(1, catalogName);
            resultSet = pstmt.executeQuery();
            while (resultSet.next()) {
                Operation operation = flinkParser.parse(resultSet.getString(4));
                if (operation instanceof CreateViewOperation) {
                    CreateViewOperation createViewOperation = (CreateViewOperation) operation;
                    flinkParser.creatView(createViewOperation);
                } else {
                    CreateTableOperation createTableOperation = (CreateTableOperation) operation;

                    if (!StringUtils.isNullOrWhitespaceOnly(resultSet.getString(5))) {
                        String password = EncryptUtil.decrypt(resultSet.getString(5), secretKey);
                        DesensitiveUtil.sensitiveForProperties(
                                password, createTableOperation.getCatalogTable().getOptions());
                    }

                    flinkParser.creatTable(createTableOperation);
                }

                ObjectPath objectPath =
                        new ObjectPath(resultSet.getString(1), resultSet.getString(2));
                super.createTable(
                        objectPath,
                        flinkParser.getTable(
                                objectPath.getDatabaseName(), objectPath.getObjectName()),
                        false);
            }
            resultSet.close();

            // craete function in memory
            pstmt =
                    conn.prepareStatement(
                            "select database_name, object_name,  class_name, function_language from flink_catalog_functions where catalog_name = ?"
                                    + databaseCondition);
            pstmt.setString(1, catalogName);
            resultSet = pstmt.executeQuery();
            while (resultSet.next()) {
                CatalogFunction catalogFunction =
                        new CatalogFunctionImpl(
                                resultSet.getString(3),
                                FunctionLanguage.valueOf(resultSet.getString(4)));
                ObjectPath objectPath =
                        new ObjectPath(resultSet.getString(1), resultSet.getString(2));
                super.createFunction(objectPath, catalogFunction, false);
            }
            resultSet.close();
        } catch (SQLException e) {
            throw new ValidationException(
                    String.format("Failed connecting to %s via JDBC.", url), e);
        }
    }
}
