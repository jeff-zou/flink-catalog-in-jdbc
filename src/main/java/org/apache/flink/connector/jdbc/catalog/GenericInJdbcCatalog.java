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

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import org.apache.flink.connector.jdbc.catalog.common.CatalogViewSerialize;
import org.apache.flink.connector.jdbc.catalog.common.DesensitiveUtil;
import org.apache.flink.connector.jdbc.catalog.common.EncryptUtil;
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
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.factories.Factory;
import org.apache.flink.table.factories.FunctionDefinitionFactory;
import org.apache.flink.table.factories.TableFactory;
import org.apache.flink.table.functions.FunctionIdentifier;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.*;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.StringUtils.isNullOrWhitespaceOnly;

/** A generic catalog implementation that holds all meta objects in jdbc with no cache. */
public class GenericInJdbcCatalog extends AbstractCatalog {

    private static final Logger LOG = LoggerFactory.getLogger(GenericInJdbcCatalog.class);

    private ObjectMapper objectMapper;
    private static final String DEFAULT_DATABASE = "default";

    private final String secretKey;

    private CatalogViewSerialize catalogViewSerialize;

    private final HikariConfig hikariConfig;

    private HikariDataSource dataSource;

    public GenericInJdbcCatalog(
            String catalogName,
            String defaultDatabase,
            String secretKey,
            HikariConfig hikariConfig) {
        super(catalogName, defaultDatabase);
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(hikariConfig.getUsername()));
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(hikariConfig.getPassword()));
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(hikariConfig.getJdbcUrl()));
        this.hikariConfig = hikariConfig;
        this.secretKey = secretKey;
    }

    @Override
    public void open() throws CatalogException {
        if (dataSource == null) {
            dataSource = new HikariDataSource(hikariConfig);
        }

        if (!DEFAULT_DATABASE.equals(getDefaultDatabase())
                && !databaseExists(getDefaultDatabase())) {
            throw new CatalogException(
                    String.format(
                            "Configured default database %s doesn't exist in catalog %s.",
                            getDefaultDatabase(), getName()));
        }
        objectMapper = new ObjectMapper();
        try {
            catalogViewSerialize = new CatalogViewSerialize();
        } catch (NoSuchMethodException e) {
            throw new CatalogException("Failed init CatalogViewSerialize .", e);
        }
    }

    @Override
    public void close() throws CatalogException {
        if (dataSource != null) {
            dataSource.close();
            dataSource = null;
            LOG.info("Close connection to jdbc metastore");
        }
    }

    // ------ databases ------
    @Override
    public void createDatabase(
            String databaseName, CatalogDatabase database, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException, CatalogException {

        checkArgument(
                !isNullOrWhitespaceOnly(databaseName), "databaseName cannot be null or empty");
        checkNotNull(database, "database cannot be null");

        boolean exists = databaseExists(databaseName);
        if (exists && !ignoreIfExists) {
            throw new DatabaseAlreadyExistException(getName(), databaseName);
        } else if (exists) {
            return;
        }

        try (Connection conn = dataSource.getConnection();
                PreparedStatement pstmt =
                        conn.prepareStatement(
                                "insert into flink_catalog_databases (database_name, comment, properties,catalog_name ) values (?, ?, ?, ?)")) {
            pstmt.setString(1, databaseName);
            pstmt.setString(2, database.getComment());
            pstmt.setString(3, objectMapper.writeValueAsString(database.getProperties()));
            pstmt.setString(4, getName());
            pstmt.execute();
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
        if (!ignoreIfNotExists && !databaseExists(databaseName)) {
            throw new DatabaseNotExistException(getName(), databaseName);
        } else if (!databaseExists(databaseName)) {
            return;
        }

        if (!cascade) {
            if (isEmptyDatabase(databaseName)) {
                try (Connection conn = dataSource.getConnection();
                        PreparedStatement pstmt =
                                conn.prepareStatement(
                                        "delete from  flink_catalog_databases where database_name = ? and catalog_name=?")) {
                    pstmt.setString(1, databaseName);
                    pstmt.setString(2, getName());
                    pstmt.execute();
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

        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            PreparedStatement pstmt =
                    conn.prepareStatement(
                            "delete from  flink_catalog_databases where database_name = ? and catalog_name=?");
            pstmt.setString(1, databaseName);
            pstmt.setString(2, getName());
            pstmt.execute();
            pstmt =
                    conn.prepareStatement(
                            "delete from  flink_catalog_tables where database_name = ? and catalog_name=?");
            pstmt.setString(1, databaseName);
            pstmt.setString(2, getName());
            pstmt.execute();

            pstmt =
                    conn.prepareStatement(
                            "delete from  flink_catalog_functions where database_name = ? and catalog_name=?");
            pstmt.setString(1, databaseName);
            pstmt.setString(2, getName());
            pstmt.execute();

            pstmt =
                    conn.prepareStatement(
                            "delete from  flink_catalog_columns where database_name = ? and catalog_name=?");
            pstmt.setString(1, databaseName);
            pstmt.setString(2, getName());
            pstmt.execute();
            conn.commit();
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

        CatalogDatabase existingDatabase = getDatabase(databaseName);

        if (existingDatabase != null) {
            try (Connection conn = dataSource.getConnection();
                    PreparedStatement pstmt =
                            conn.prepareStatement(
                                    "update flink_catalog_databases set comment = ? , properties =?  where database_name = ? and catalog_name = ?")) {
                pstmt.setString(1, newDatabase.getComment());
                pstmt.setString(2, objectMapper.writeValueAsString(newDatabase.getProperties()));
                pstmt.setString(3, databaseName);
                pstmt.setString(4, getName());
                pstmt.execute();
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
            try (Connection conn = dataSource.getConnection()) {
                conn.setAutoCommit(false);
                String password = DesensitiveUtil.desensitiveForProperties(table.getOptions());
                Map<String, String> schemaAndProperties = null;
                if (table instanceof ResolvedCatalogView) {
                    schemaAndProperties =
                            catalogViewSerialize.serializeCatalogView((ResolvedCatalogView) table);
                } else {
                    schemaAndProperties =
                            CatalogPropertiesUtil.serializeCatalogTable(
                                    (ResolvedCatalogTable) table);
                }

                PreparedStatement pstmt =
                        conn.prepareStatement(
                                "insert into flink_catalog_tables (database_name, object_name, kind, script, comment, `password`, catalog_name, schema_properties) values (?, ?, ?, ?, ?, ?, ?, ?)");
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
                pstmt.setString(7, getName());
                pstmt.setString(8, objectMapper.writeValueAsString(schemaAndProperties));
                pstmt.execute();
                batchSaveColumns(
                        tablePath,
                        ((ResolvedCatalogBaseTable<?>) table).getResolvedSchema(),
                        pstmt,
                        conn);
                conn.commit();
            } catch (SQLException e) {
                throw new CatalogException(
                        String.format("Failed to create table %s", tablePath.getFullName()), e);
            } catch (JsonProcessingException e) {
                throw new CatalogException("Failed to parse properties to json", e);
            } catch (InvocationTargetException | IllegalAccessException e) {
                throw new CatalogException("Failed to serialize view to properties", e);
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
            pstmt.setString(6, getName());
            pstmt.addBatch();
        }
        pstmt.executeBatch();
    }

    @Override
    public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        checkNotNull(tablePath);
        if (tableExists(tablePath)) {
            try (Connection conn = dataSource.getConnection()) {
                conn.setAutoCommit(false);
                PreparedStatement pstmt =
                        conn.prepareStatement(
                                "delete from flink_catalog_tables where database_name = ? and  object_name = ? and catalog_name = ?");
                pstmt.setString(1, tablePath.getDatabaseName());
                pstmt.setString(2, tablePath.getObjectName());
                pstmt.setString(3, getName());
                pstmt.execute();

                pstmt =
                        conn.prepareStatement(
                                "delete from flink_catalog_columns where database_name = ? and  object_name = ? and catalog_name = ?");
                pstmt.setString(1, tablePath.getDatabaseName());
                pstmt.setString(2, tablePath.getObjectName());
                pstmt.setString(3, getName());
                pstmt.execute();
                conn.commit();
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
                try (Connection conn = dataSource.getConnection()) {
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
                    pstmt.setString(5, getName());
                    pstmt.execute();

                    pstmt =
                            conn.prepareStatement(
                                    "update flink_catalog_columns set object_name=? where database_name = ? and  object_name = ? and catalog_name = ?");
                    pstmt.setString(1, newTableName);
                    pstmt.setString(2, tablePath.getDatabaseName());
                    pstmt.setString(3, tablePath.getObjectName());
                    pstmt.setString(4, getName());
                    pstmt.execute();
                    conn.commit();
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

            try (Connection conn = dataSource.getConnection();
                    PreparedStatement pstmt =
                            conn.prepareStatement(
                                    "update flink_catalog_tables set script = ? where database_name = ? and object_name= ? and catalog_name = ?")) {
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
                pstmt.setString(4, getName());
                pstmt.execute();
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
                try (Connection conn = dataSource.getConnection();
                        PreparedStatement pstmt =
                                conn.prepareStatement(
                                        "insert into flink_catalog_functions (database_name, object_name,  class_name, function_language, catalog_name ) values (?, ?, ?, ?, ?)")) {
                    pstmt.setString(1, path.getDatabaseName());
                    pstmt.setString(2, path.getObjectName());
                    pstmt.setString(3, function.getClassName());
                    pstmt.setString(4, function.getFunctionLanguage().name());
                    pstmt.setString(5, getName());
                    pstmt.execute();
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

            try (Connection conn = dataSource.getConnection();
                    PreparedStatement pstmt =
                            conn.prepareStatement(
                                    "update flink_catalog_functions set class_name = ?, function_language =? where database_name = ? and object_name = ? and catalog_name = ?")) {
                pstmt.setString(1, newFunction.getClassName());
                pstmt.setString(2, newFunction.getFunctionLanguage().name());
                pstmt.setString(3, path.getDatabaseName());
                pstmt.setString(4, path.getObjectName());
                pstmt.setString(5, getName());
                pstmt.execute();
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
            try (Connection conn = dataSource.getConnection();
                    PreparedStatement pstmt =
                            conn.prepareStatement(
                                    "delete from flink_catalog_functions where database_name = ? and object_name =? and catalog_name = ?")) {
                pstmt.setString(1, path.getDatabaseName());
                pstmt.setString(2, path.getObjectName());
                pstmt.setString(3, getName());
                pstmt.execute();
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

    @Override
    public Optional<Factory> getFactory() {
        return super.getFactory();
    }

    @Override
    public Optional<TableFactory> getTableFactory() {
        return super.getTableFactory();
    }

    @Override
    public Optional<FunctionDefinitionFactory> getFunctionDefinitionFactory() {
        return super.getFunctionDefinitionFactory();
    }

    @Override
    public void dropDatabase(String name, boolean ignoreIfNotExists)
            throws DatabaseNotExistException, DatabaseNotEmptyException, CatalogException {
        dropDatabase(name, ignoreIfNotExists, true);
    }

    @Override
    public boolean supportsManagedTable() {
        return super.supportsManagedTable();
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        try (Connection conn = dataSource.getConnection();
                PreparedStatement pstmt =
                        conn.prepareStatement(
                                "select database_name from flink_catalog_databases where catalog_name = ?")) {
            pstmt.setString(1, getName());
            ResultSet resultSet = pstmt.executeQuery();
            List<String> result = new ArrayList<>();
            while (resultSet.next()) {
                result.add(resultSet.getString(1));
            }
            return result;
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed to list databases in catalog %s", getName()), e);
        }
    }

    @Override
    public CatalogDatabase getDatabase(String databaseName)
            throws DatabaseNotExistException, CatalogException {
        try (Connection conn = dataSource.getConnection();
                PreparedStatement pstmt =
                        conn.prepareStatement(
                                "select comment, properties from flink_catalog_databases where database_name = ? and catalog_name = ?")) {
            pstmt.setString(1, databaseName);
            pstmt.setString(2, getName());
            ResultSet resultSet = pstmt.executeQuery();
            while (resultSet.next()) {
                Map<String, String> properties = null;
                properties = objectMapper.readValue(resultSet.getString(2), Map.class);
                return new CatalogDatabaseImpl(properties, resultSet.getString(1));
            }

        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed to get database %s ", databaseName), e);
        }
        throw new DatabaseNotExistException(getName(), databaseName);
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        try (Connection conn = dataSource.getConnection();
                PreparedStatement pstmt =
                        conn.prepareStatement(
                                "select database_name from flink_catalog_databases where database_name = ? and catalog_name = ?")) {
            pstmt.setString(1, databaseName);
            pstmt.setString(2, getName());
            ResultSet resultSet = pstmt.executeQuery();
            if (resultSet.next()) {
                return true;
            } else {
                return false;
            }
        } catch (Exception e) {
            throw new CatalogException(
                    String.format(
                            "Failed to determine whether database %s exists or not", databaseName),
                    e);
        }
    }

    @Override
    public List<String> listTables(String databaseName)
            throws DatabaseNotExistException, CatalogException {
        checkArgument(
                !isNullOrWhitespaceOnly(databaseName), "databaseName cannot be null or empty");

        try (Connection conn = dataSource.getConnection();
                PreparedStatement pstmt =
                        conn.prepareStatement(
                                "select object_name from flink_catalog_tables where database_name = ? and catalog_name = ?")) {
            pstmt.setString(1, databaseName);
            pstmt.setString(2, getName());
            ResultSet resultSet = pstmt.executeQuery();
            List<String> result = new ArrayList<>();
            while (resultSet.next()) {
                result.add(resultSet.getString(1));
            }
            return result;
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed to list tables in database %s", databaseName), e);
        }
    }

    @Override
    public List<String> listViews(String databaseName)
            throws DatabaseNotExistException, CatalogException {
        checkArgument(
                !isNullOrWhitespaceOnly(databaseName), "databaseName cannot be null or empty");

        try (Connection conn = dataSource.getConnection();
                PreparedStatement pstmt =
                        conn.prepareStatement(
                                "select object_name from flink_catalog_tables where database_name = ? and catalog_name = ? and kind = ?")) {
            pstmt.setString(1, databaseName);
            pstmt.setString(2, getName());
            pstmt.setString(3, CatalogBaseTable.TableKind.VIEW.name());
            ResultSet resultSet = pstmt.executeQuery();
            List<String> result = new ArrayList<>();
            while (resultSet.next()) {
                result.add(resultSet.getString(1));
            }
            return result;
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed to list views in database %s", databaseName), e);
        }
    }

    @Override
    public CatalogBaseTable getTable(ObjectPath tablePath)
            throws TableNotExistException, CatalogException {
        try (Connection conn = dataSource.getConnection();
                PreparedStatement pstmt =
                        conn.prepareStatement(
                                "select kind, script, comment, `password`, schema_properties from flink_catalog_tables where database_name = ? and catalog_name = ? and object_name = ?")) {
            pstmt.setString(1, tablePath.getDatabaseName());
            pstmt.setString(2, getName());
            pstmt.setString(3, tablePath.getObjectName());
            ResultSet resultSet = pstmt.executeQuery();
            while (resultSet.next()) {
                Map<String, String> schemaProperties =
                        objectMapper.readValue(resultSet.getString(5), Map.class);
                if (CatalogBaseTable.TableKind.TABLE.name().equals(resultSet.getString(1))) {
                    CatalogTable table =
                            CatalogPropertiesUtil.deserializeCatalogTable(schemaProperties);

                    if (!StringUtils.isNullOrWhitespaceOnly(resultSet.getString(4))) {
                        String password = EncryptUtil.decrypt(resultSet.getString(4), secretKey);
                        DesensitiveUtil.sensitiveForProperties(password, table.getOptions());
                    }
                    return table;
                } else {
                    return catalogViewSerialize.deserializeCatalogView(schemaProperties);
                }
            }

        } catch (Exception e) {
            throw new CatalogException(
                    String.format(
                            "Failed to get table %s in database %s",
                            tablePath.getObjectName(), tablePath.getDatabaseName()),
                    e);
        }
        throw new TableNotExistException(getName(), tablePath);
    }

    @Override
    public boolean tableExists(ObjectPath tablePath) throws CatalogException {
        try (Connection conn = dataSource.getConnection();
                PreparedStatement pstmt =
                        conn.prepareStatement(
                                "select object_name from flink_catalog_tables where database_name = ? and catalog_name = ? and object_name = ?")) {
            pstmt.setString(1, tablePath.getDatabaseName());
            pstmt.setString(2, getName());
            pstmt.setString(3, tablePath.getObjectName());
            ResultSet resultSet = pstmt.executeQuery();
            if (resultSet.next()) {
                return true;
            } else {
                return false;
            }
        } catch (Exception e) {
            throw new CatalogException(
                    String.format(
                            "Failed to determine whether table %s exists or not",
                            tablePath.getObjectName()),
                    e);
        }
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath)
            throws TableNotExistException, TableNotPartitionedException, CatalogException {
        return null;
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws TableNotExistException,
            TableNotPartitionedException,
            PartitionSpecInvalidException,
            CatalogException {
        return null;
    }

    @Override
    public List<CatalogPartitionSpec> listPartitionsByFilter(
            ObjectPath tablePath, List<Expression> filters)
            throws TableNotExistException, TableNotPartitionedException, CatalogException {
        return null;
    }

    @Override
    public CatalogPartition getPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws PartitionNotExistException, CatalogException {
        return null;
    }

    @Override
    public boolean partitionExists(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws CatalogException {
        return false;
    }

    @Override
    public List<String> listFunctions(String dbName)
            throws DatabaseNotExistException, CatalogException {
        try (Connection conn = dataSource.getConnection();
                PreparedStatement pstmt =
                        conn.prepareStatement(
                                "select object_name from flink_catalog_functions where database_name = ? and catalog_name = ? ")) {
            pstmt.setString(1, dbName);
            pstmt.setString(2, getName());
            ResultSet resultSet = pstmt.executeQuery();
            List<String> result = new ArrayList<>();
            while (resultSet.next()) {
                result.add(resultSet.getString(1));
            }
            return result;
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed to list functions in database %s", dbName), e);
        }
    }

    @Override
    public CatalogFunction getFunction(ObjectPath functionPath)
            throws FunctionNotExistException, CatalogException {
        try (Connection conn = dataSource.getConnection();
                PreparedStatement pstmt =
                        conn.prepareStatement(
                                "select class_name, function_language from flink_catalog_functions where database_name = ? and catalog_name = ? and object_name = ?")) {
            pstmt.setString(1, functionPath.getDatabaseName());
            pstmt.setString(2, getName());
            pstmt.setString(3, functionPath.getObjectName());
            ResultSet resultSet = pstmt.executeQuery();
            while (resultSet.next()) {
                return new CatalogFunctionImpl(
                        resultSet.getString(1), FunctionLanguage.valueOf(resultSet.getString(2)));
            }
        } catch (Exception e) {
            throw new CatalogException(
                    String.format(
                            "Failed to get function %s in database %s",
                            functionPath.getObjectName(), functionPath.getDatabaseName()),
                    e);
        }
        throw new FunctionNotExistException(getName(), functionPath);
    }

    @Override
    public boolean functionExists(ObjectPath functionPath) throws CatalogException {
        try (Connection conn = dataSource.getConnection();
                PreparedStatement pstmt =
                        conn.prepareStatement(
                                "select object_name from flink_catalog_functions where database_name = ? and catalog_name = ? and object_name = ?")) {
            pstmt.setString(1, functionPath.getDatabaseName());
            pstmt.setString(2, getName());
            pstmt.setString(3, functionPath.getObjectName());
            ResultSet resultSet = pstmt.executeQuery();
            if (resultSet.next()) {
                return true;
            } else {
                return false;
            }
        } catch (Exception e) {
            throw new CatalogException(
                    String.format(
                            "Failed to determine whether function %s exists or not",
                            functionPath.getObjectName()),
                    e);
        }
    }

    @Override
    public CatalogTableStatistics getTableStatistics(ObjectPath tablePath)
            throws TableNotExistException, CatalogException {
        return null;
    }

    @Override
    public CatalogColumnStatistics getTableColumnStatistics(ObjectPath tablePath)
            throws TableNotExistException, CatalogException {
        return null;
    }

    @Override
    public CatalogTableStatistics getPartitionStatistics(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws PartitionNotExistException, CatalogException {
        return null;
    }

    @Override
    public CatalogColumnStatistics getPartitionColumnStatistics(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws PartitionNotExistException, CatalogException {
        return null;
    }
}
