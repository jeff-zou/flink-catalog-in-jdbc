package org.apache.flink.connector.jdbc.catalog.common;

import static org.apache.calcite.jdbc.CalciteSchemaBuilder.asRootSchema;

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
import org.apache.flink.table.resource.ResourceManager;

import java.net.URL;
import java.util.Collections;
import java.util.Optional;
import java.util.function.Supplier;

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

        ResourceManager resourceManager =
                ResourceManager.createResourceManager(
                        new URL[0],
                        Thread.currentThread().getContextClassLoader(),
                        tableConfig.getConfiguration());
        final FunctionCatalog functionCatalog =
                new FunctionCatalog(tableConfig, resourceManager, catalogManager, moduleManager);
        final Supplier<FlinkPlannerImpl> plannerSupplier =
                () -> getPlannerContext().createFlinkPlanner();

        plannerContext =
                new PlannerContext(
                        false,
                        tableConfig,
                        new ModuleManager(),
                        functionCatalog,
                        catalogManager,
                        asRootSchema(
                                new CatalogManagerCalciteSchema(catalogManager, isStreamingMode)),
                        Collections.emptyList(),
                        Thread.currentThread().getContextClassLoader());

        final Parser parser =
                new ParserImpl(
                        catalogManager,
                        plannerSupplier,
                        () -> plannerSupplier.get().parser(),
                        plannerContext.getRexFactory());
        ExpressionResolver.ExpressionResolverBuilder builder =
                ExpressionResolver.resolverFor(
                        TableConfig.getDefault(),
                        Thread.currentThread().getContextClassLoader(),
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
        return plannerContext.createFlinkPlanner();
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
