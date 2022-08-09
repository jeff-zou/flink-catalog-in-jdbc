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

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
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
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** A generic catalog implementation that holds all meta objects in jdbc. */
public class GenericInJdbcCatalog extends GenericInMemoryCatalog {

    private static final Logger LOG = LoggerFactory.getLogger(GenericInJdbcCatalog.class);

    private final String username;
    private final String pwd;
    private final String url;
    private ObjectMapper objectMapper;

    public GenericInJdbcCatalog(
            String catalogName, String defaultDatabase, String username, String pwd, String url) {
        super(catalogName, defaultDatabase);

        checkArgument(!StringUtils.isNullOrWhitespaceOnly(username));
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(pwd));
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(url));

        validateJdbcUrl(url);

        this.username = username;
        this.pwd = pwd;
        this.url = url;
        objectMapper = new ObjectMapper();
    }

    private void validateJdbcUrl(String url) {
        String[] parts = url.trim().split("\\/+");
        Preconditions.checkArgument(parts.length == 3);
    }

    @Override
    public void open() throws CatalogException {
        GenericCatalogLoader loader = new GenericCatalogLoader();
        try {
            loader.load(this.url, this.username, this.pwd, this, objectMapper);
        } catch (Exception e1) {
            throw new ValidationException(
                    String.format("Failed load catalog data from jdbc %s .", this.url), e1);
        }

        super.open();
        LOG.info("Catalog {} established connection to {}", getName(), url);
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
            if (exists) {
                PreparedStatement pstmt =
                        conn.prepareStatement(
                                "update flink_catalog_databases set comment = ? , properties =? where database_name= ? ) values (?, ?, ?)");
                pstmt.setString(1, database.getComment());
                pstmt.setString(2, objectMapper.writeValueAsString(database.getProperties()));
                pstmt.setString(3, databaseName);
                pstmt.execute();
            } else {
                PreparedStatement pstmt =
                        conn.prepareStatement(
                                "insert into flink_catalog_databases (database_name, comment, properties ) values (?, ?, ?)");
                pstmt.setString(1, databaseName);
                pstmt.setString(2, database.getComment());
                pstmt.setString(3, objectMapper.writeValueAsString(database.getProperties()));
                pstmt.execute();
            }

        } catch (SQLException e) {
            throw new ValidationException(String.format("Failed create database %s.", database), e);
        } catch (JsonProcessingException e1) {
            throw new ValidationException(String.format("Failed format map to json."), e1);
        }
        super.createDatabase(databaseName, database, ignoreIfExists);
    }

    public void loadDatabase(String databaseName, CatalogDatabase database, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException, CatalogException {
        super.createDatabase(databaseName, database, ignoreIfExists);
    }

    @Override
    public void dropDatabase(String databaseName, boolean ignoreIfNotExists, boolean cascade)
            throws DatabaseNotExistException, DatabaseNotEmptyException, CatalogException {
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(databaseName));
        if (super.databaseExists(databaseName)) {
            try (Connection conn = DriverManager.getConnection(url, username, pwd)) {
                conn.setAutoCommit(false);
                if (cascade) {
                    PreparedStatement pstmt =
                            conn.prepareStatement(
                                    "delete from  flink_catalog_tables where database_name = ?");
                    pstmt.setString(1, databaseName);
                    pstmt.execute();

                    pstmt =
                            conn.prepareStatement(
                                    "delete from  flink_catalog_functions where database_name = ?");
                    pstmt.setString(1, databaseName);
                    pstmt.execute();
                }

                PreparedStatement pstmt =
                        conn.prepareStatement(
                                "delete from  flink_catalog_databases where database_name = ?");
                pstmt.setString(1, databaseName);
                pstmt.execute();
                conn.commit();
            } catch (SQLException e) {
                throw new ValidationException(
                        String.format("Failed drop database %s.", databaseName), e);
            }
        } else if (!ignoreIfNotExists) {
            throw new DatabaseNotExistException(getName(), databaseName);
        }
        super.dropDatabase(databaseName, ignoreIfNotExists, cascade);
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
                                "insert into flink_catalog_databases (database_name, comment, properties ) values (?, ?, ?)");
                pstmt.setString(1, databaseName);
                pstmt.setString(2, newDatabase.getComment());
                pstmt.setString(3, objectMapper.writeValueAsString(newDatabase.getProperties()));
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
        super.alterDatabase(databaseName, newDatabase, ignoreIfNotExists);
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
                ResolvedCatalogTable catalogTable = (ResolvedCatalogTable) table;
                PreparedStatement pstmt =
                        conn.prepareStatement(
                                "insert into flink_catalog_tables (database_name, object_name, kind, script ) values (?, ?, ?, ? )");
                pstmt.setString(1, tablePath.getDatabaseName());
                pstmt.setString(2, tablePath.getObjectName());
                pstmt.setString(3, table.getTableKind().name());
                pstmt.setString(
                        4,
                        table.getTableKind() == CatalogBaseTable.TableKind.TABLE
                                ? ShowCreateUtils.buildShowCreateTableRow(
                                        catalogTable, tablePath.getObjectName(), false)
                                : ShowCreateUtils.buildShowCreateViewRow(
                                        catalogTable, tablePath.getObjectName(), false));

                pstmt.execute();
            } catch (SQLException e) {
                throw new ValidationException(
                        String.format("Failed drop database %s.", tablePath.getDatabaseName()), e);
            }
            super.createTable(tablePath, table, ignoreIfExists);
        }
    }

    public void loadTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfNotExists)
            throws TableAlreadyExistException, DatabaseNotExistException {
        super.createTable(tablePath, table, ignoreIfNotExists);
    }

    @Override
    public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        checkNotNull(tablePath);
        if (tableExists(tablePath)) {
            try (Connection conn = DriverManager.getConnection(url, username, pwd)) {
                PreparedStatement pstmt =
                        conn.prepareStatement(
                                "delete from flink_catalog_tables where database_name = ? and  object_name = ?");
                pstmt.setString(1, tablePath.getDatabaseName());
                pstmt.setString(2, tablePath.getObjectName());
                pstmt.execute();
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

        if (tableExists(tablePath)) {
            ObjectPath newPath = new ObjectPath(tablePath.getDatabaseName(), newTableName);

            if (tableExists(newPath)) {
                throw new TableAlreadyExistException(getName(), newPath);
            } else {
                try (Connection conn = DriverManager.getConnection(url, username, pwd)) {
                    PreparedStatement pstmt =
                            conn.prepareStatement(
                                    "update flink_catalog_tables set object_name=? where database_name = ? and  object_name = ?");
                    pstmt.setString(1, newTableName);
                    pstmt.setString(2, tablePath.getDatabaseName());
                    pstmt.setString(3, tablePath.getObjectName());
                    pstmt.execute();

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
                                "update flink_catalog_tables set properties = ? where database_name = ? and object_name= ?");
                pstmt.setString(2, tablePath.getDatabaseName());
                pstmt.setString(3, tablePath.getObjectName());
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(baos);
                oos.writeObject(newTable);
                byte[] bytes = baos.toByteArray();
                pstmt.setBinaryStream(1, new ByteArrayInputStream(bytes));
                pstmt.execute();
                super.alterTable(tablePath, newTable, ignoreIfNotExists);
            } catch (SQLException e) {
                throw new ValidationException(
                        String.format(
                                "Failed alter table %s.%s",
                                tablePath.getDatabaseName(), tablePath.getObjectName()),
                        e);
            } catch (JsonProcessingException e1) {
                throw new ValidationException(String.format("Failed format map to json."), e1);
            } catch (IOException e2) {
                throw new ValidationException(
                        String.format(
                                "Failed serialize table %s.%s",
                                tablePath.getDatabaseName(), tablePath.getObjectName()),
                        e2);
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
            throws TableNotExistException, TableNotPartitionedException,
                    PartitionSpecInvalidException, PartitionAlreadyExistsException,
                    CatalogException {
        super.createPartition(tablePath, partitionSpec, partition, ignoreIfExists);
        //
        try (Connection conn = DriverManager.getConnection(url, username, pwd)) {
            PreparedStatement pstmt =
                    conn.prepareStatement(
                            "insert into flink_catalog_tables_partitions(database_name, object_name, spec, partition_comment, partition_properties) values(?, ?, ?, ?, ?) ");
            pstmt.setString(1, tablePath.getDatabaseName());
            pstmt.setString(2, tablePath.getObjectName());
            pstmt.setString(3, objectMapper.writeValueAsString(partitionSpec.getPartitionSpec()));
            pstmt.setString(4, partition.getComment());
            pstmt.setString(5, objectMapper.writeValueAsString(partition.getProperties()));
            pstmt.execute();
        } catch (SQLException e) {
            try {
                super.dropPartition(tablePath, partitionSpec, ignoreIfExists);
            } catch (Exception e1) {
                throw new CatalogException(
                        String.format(
                                "Failed clean partition cache for table: %s.%s",
                                tablePath.getDatabaseName(), tablePath.getObjectName()));
            }

            throw new ValidationException(
                    String.format(
                            "Failed alter table %s.%s",
                            tablePath.getDatabaseName(), tablePath.getObjectName()),
                    e);
        } catch (JsonProcessingException e1) {
            throw new ValidationException(String.format("Failed format map to json."), e1);
        }
    }

    @Override
    public void dropPartition(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec, boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException {
        checkNotNull(tablePath);
        checkNotNull(partitionSpec);

        if (partitionExists(tablePath, partitionSpec)) {
            try (Connection conn = DriverManager.getConnection(url, username, pwd)) {
                String spec = objectMapper.writeValueAsString(partitionSpec.getPartitionSpec());
                PreparedStatement pstmt =
                        conn.prepareStatement(
                                "delete from flink_catalog_tables_partitions where database_name = ? and object_name = ? and spec = ? ");
                pstmt.setString(1, tablePath.getDatabaseName());
                pstmt.setString(2, tablePath.getObjectName());
                pstmt.setString(3, spec);
                pstmt.execute();

                pstmt =
                        conn.prepareStatement(
                                "delete from flink_catalog_tables_partitions_stats where database_name = ? and object_name = ? and spec = ? ");
                pstmt.setString(1, tablePath.getDatabaseName());
                pstmt.setString(2, tablePath.getObjectName());
                pstmt.setString(3, spec);
                pstmt.execute();

                pstmt =
                        conn.prepareStatement(
                                "delete from flink_catalog_tables_partitions_column_stats where database_name = ? and object_name = ? and spec = ? ");
                pstmt.setString(1, tablePath.getDatabaseName());
                pstmt.setString(2, tablePath.getObjectName());
                pstmt.setString(3, spec);
                pstmt.execute();
                super.dropPartition(tablePath, partitionSpec, ignoreIfNotExists);
            } catch (SQLException e) {
                throw new ValidationException(
                        String.format(
                                "Failed create partitions for %s.%s",
                                tablePath.getDatabaseName(), tablePath.getObjectName()),
                        e);
            } catch (JsonProcessingException e1) {
                throw new ValidationException(String.format("Failed format map to json."), e1);
            }
        } else if (!ignoreIfNotExists) {
            throw new PartitionNotExistException(getName(), tablePath, partitionSpec);
        }
    }

    @Override
    public void alterPartition(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogPartition newPartition,
            boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException {
        checkNotNull(tablePath);
        checkNotNull(partitionSpec);
        checkNotNull(newPartition);

        if (partitionExists(tablePath, partitionSpec)) {
            CatalogPartition existingPartition = getPartition(tablePath, partitionSpec);

            if (existingPartition.getClass() != newPartition.getClass()) {
                throw new CatalogException(
                        String.format(
                                "Partition types don't match. Existing partition is '%s' and new partition is '%s'.",
                                existingPartition.getClass().getName(),
                                newPartition.getClass().getName()));
            }

            try (Connection conn = DriverManager.getConnection(url, username, pwd)) {
                PreparedStatement pstmt =
                        conn.prepareStatement(
                                "update flink_catalog_tables_partitions set partition_comment = ? , partition_properties =? where database_name = ? and object_name = ? and spec = ?");
                pstmt.setString(1, newPartition.getComment());
                pstmt.setString(2, objectMapper.writeValueAsString(newPartition.getProperties()));
                pstmt.setString(3, tablePath.getDatabaseName());
                pstmt.setString(4, tablePath.getObjectName());
                pstmt.setString(
                        5, objectMapper.writeValueAsString(partitionSpec.getPartitionSpec()));
                pstmt.execute();
                super.alterPartition(tablePath, partitionSpec, newPartition, ignoreIfNotExists);
            } catch (SQLException e) {
                throw new ValidationException(
                        String.format(
                                "Failed alter table %s.%s",
                                tablePath.getDatabaseName(), tablePath.getObjectName()),
                        e);
            } catch (JsonProcessingException e1) {
                throw new ValidationException(String.format("Failed format map to json."), e1);
            }
        } else if (!ignoreIfNotExists) {
            throw new PartitionNotExistException(getName(), tablePath, partitionSpec);
        }
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
                                    "insert into flink_catalog_functions (database_name, object_name,  class_name, function_language ) values (?, ?, ?, ?, ?)");
                    pstmt.setString(1, path.getDatabaseName());
                    pstmt.setString(2, path.getObjectName());
                    pstmt.setString(3, function.getClassName());
                    pstmt.setString(4, function.getFunctionLanguage().name());
                    pstmt.execute();
                    super.createFunction(path, function, ignoreIfExists);
                } catch (SQLException e) {
                    throw new ValidationException(
                            String.format(
                                    "Failed alter table %s.%s",
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
                                "update flink_catalog_functions set class_name = ?, function_language =? where database_name = ? and object_name = ?");
                pstmt.setString(1, newFunction.getClassName());
                pstmt.setString(2, newFunction.getFunctionLanguage().name());
                pstmt.setString(3, path.getDatabaseName());
                pstmt.setString(4, path.getObjectName());

                pstmt.execute();
                super.alterFunction(path, newFunction, ignoreIfNotExists);
            } catch (SQLException e) {
                throw new ValidationException(
                        String.format(
                                "Failed alter table %s.%s",
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
                                "delete from flink_catalog_functions where database_name = ? and object_name =? ");
                pstmt.setString(1, path.getDatabaseName());
                pstmt.setString(2, path.getObjectName());
                pstmt.execute();
                super.dropFunction(path, ignoreIfNotExists);
            } catch (SQLException e) {
                throw new ValidationException(
                        String.format(
                                "Failed alter table %s.%s",
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
        checkNotNull(tablePath);
        checkNotNull(tableStatistics);
        if (tableExists(tablePath)) {
            try (Connection conn = DriverManager.getConnection(url, username, pwd)) {
                PreparedStatement pstmt =
                        conn.prepareStatement(
                                "insert into flink_catalog_tables_stats (database_name,object_name, row_count, file_count, total_size, raw_data_size, properties) values (?, ?, ?, ?, ?,?,?) ");
                pstmt.setString(1, tablePath.getDatabaseName());
                pstmt.setString(2, tablePath.getObjectName());
                pstmt.setLong(3, tableStatistics.getRowCount());
                pstmt.setInt(4, tableStatistics.getFileCount());
                pstmt.setLong(5, tableStatistics.getTotalSize());
                pstmt.setLong(6, tableStatistics.getRawDataSize());
                pstmt.setString(
                        7, objectMapper.writeValueAsString(tableStatistics.getProperties()));
                pstmt.execute();
                super.alterTableStatistics(tablePath, tableStatistics, ignoreIfNotExists);
            } catch (SQLException e) {
                throw new ValidationException(
                        String.format(
                                "Failed alter table %s.%s",
                                tablePath.getDatabaseName(), tablePath.getObjectName()),
                        e);
            } catch (JsonProcessingException e1) {
                throw new ValidationException(String.format("Failed format map to json."), e1);
            }
            super.alterTableStatistics(tablePath, tableStatistics, ignoreIfNotExists);
        } else if (!ignoreIfNotExists) {
            throw new TableNotExistException(getName(), tablePath);
        }
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
}
