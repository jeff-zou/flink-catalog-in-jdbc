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

package org.apache.flink.connector.jdbc.catalog.common;

import org.apache.calcite.sql.SqlNode;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.catalog.*;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.delegation.Parser;
import org.apache.flink.table.expressions.resolver.ExpressionResolver;
import org.apache.flink.table.module.ModuleManager;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.ddl.CreateTableOperation;
import org.apache.flink.table.operations.ddl.CreateViewOperation;
import org.apache.flink.table.planner.calcite.FlinkPlannerImpl;
import org.apache.flink.table.planner.catalog.CatalogManagerCalciteSchema;
import org.apache.flink.table.planner.delegation.ParserImpl;
import org.apache.flink.table.planner.delegation.PlannerContext;
import org.apache.flink.table.planner.operations.SqlToOperationConverter;
import org.apache.flink.table.planner.parse.CalciteParser;

import java.util.Collections;
import java.util.Optional;
import java.util.function.Supplier;

import static org.apache.calcite.jdbc.CalciteSchemaBuilder.asRootSchema;

/**
 * @Author: Jeff Zou @Date: 2022/8/8 17:13
 */
public class FlinkParser {

    private final String catalogName;
    private static final String DATABASE_NAME = "default";
    private final boolean isStreamingMode = true;

    private FlinkPlannerImpl planner;
    private CalciteParser calciteParser;

    private PlannerContext getPlannerContext() {
        return plannerContext;
    }

    private final TableConfig tableConfig;

    private final CatalogManager catalogManager;

    private final PlannerContext plannerContext;

    public FlinkParser(String catalogName) {
        this.catalogName = catalogName;
        tableConfig = TableConfig.getDefault();
        final Catalog catalog = new GenericInMemoryCatalog(catalogName, DATABASE_NAME);
        catalogManager =
                preparedCatalogManager(catalogName).defaultCatalog(catalogName, catalog).build();
        final ModuleManager moduleManager = new ModuleManager();

        final FunctionCatalog functionCatalog =
                new FunctionCatalog(tableConfig, catalogManager, moduleManager);
        final Supplier<FlinkPlannerImpl> plannerSupplier =
                () -> getPlannerContext()
                        .createFlinkPlanner(
                                catalogManager.getCurrentCatalog(),
                                catalogManager.getCurrentDatabase());

        plannerContext =
                new PlannerContext(
                        false,
                        tableConfig,
                        new ModuleManager(),
                        functionCatalog,
                        catalogManager,
                        asRootSchema(
                                new CatalogManagerCalciteSchema(catalogManager, isStreamingMode)),
                        Collections.emptyList());

        final Parser parser =
                new ParserImpl(
                        catalogManager,
                        plannerSupplier,
                        () -> plannerSupplier.get().parser(),
                        plannerContext.getSqlExprToRexConverterFactory());
        ExpressionResolver.ExpressionResolverBuilder builder =
                ExpressionResolver.resolverFor(
                        new TableConfig(),
                        name -> Optional.empty(),
                        functionCatalog.asLookup(parser::parseIdentifier),
                        catalogManager.getDataTypeFactory(),
                        parser::parseSqlExpression);

        catalogManager.initSchemaResolver(isStreamingMode, builder);
        planner = getPlannerBySqlDialect(SqlDialect.DEFAULT);
        calciteParser = getParserBySqlDialect(SqlDialect.DEFAULT);
    }

    public static CatalogManager.Builder preparedCatalogManager(String catalogName) {
        return CatalogManager.newBuilder()
                .classLoader(FlinkParser.class.getClassLoader())
                .config(new Configuration())
                .defaultCatalog(catalogName, new GenericInMemoryCatalog(catalogName, DATABASE_NAME))
                .executionConfig(new ExecutionConfig());
    }

    private FlinkPlannerImpl getPlannerBySqlDialect(SqlDialect sqlDialect) {
        tableConfig.setSqlDialect(sqlDialect);
        return plannerContext.createFlinkPlanner(
                catalogManager.getCurrentCatalog(), catalogManager.getCurrentDatabase());
    }

    private CalciteParser getParserBySqlDialect(SqlDialect sqlDialect) {
        tableConfig.setSqlDialect(sqlDialect);
        return plannerContext.createCalciteParser();
    }

    /**
     * @param sql
     * @return
     */
    public Operation parse(String sql) {
        SqlNode node = calciteParser.parse(sql);
        return SqlToOperationConverter.convert(planner, catalogManager, node).get();
    }

    public void createDatabase(String databaseName, CatalogDatabase catalogDatabase) {
        try {
            catalogManager
                    .getCatalog(catalogName)
                    .get()
                    .createDatabase(databaseName, catalogDatabase, true);
        } catch (Exception e) {

        }
    }

    public void creatTable(CreateTableOperation createTableOperation) {
        createTable(
                createTableOperation.getCatalogTable(),
                createTableOperation.getTableIdentifier(),
                createTableOperation.isIgnoreIfExists());
    }

    public void creatView(CreateViewOperation createViewOperation) {
        createTable(
                createViewOperation.getCatalogView(),
                createViewOperation.getViewIdentifier(),
                createViewOperation.isIgnoreIfExists());
    }

    public void createTable(
            CatalogBaseTable table, ObjectIdentifier objectIdentifier, boolean ignoreIfExists) {
        catalogManager.createTable(table, objectIdentifier, ignoreIfExists);
    }

    public CatalogBaseTable getTable(String databaseName, String objectName)
            throws TableNotExistException, CatalogException {
        ObjectPath objectPath = new ObjectPath(databaseName, objectName);
        return catalogManager.getCatalog(catalogName).get().getTable(objectPath);
    }

    public SqlNode parseNode(String sql) {
        return calciteParser.parse(sql);
    }
}
