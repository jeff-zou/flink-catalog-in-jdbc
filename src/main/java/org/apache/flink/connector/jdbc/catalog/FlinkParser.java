package org.apache.flink.connector.jdbc.catalog;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.delegation.Parser;
import org.apache.flink.table.expressions.resolver.ExpressionResolver;
import org.apache.flink.table.module.ModuleManager;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.ddl.CreateTableOperation;
import org.apache.flink.table.planner.calcite.FlinkPlannerImpl;
import org.apache.flink.table.planner.catalog.CatalogManagerCalciteSchema;
import org.apache.flink.table.planner.delegation.ParserImpl;
import org.apache.flink.table.planner.delegation.PlannerContext;
import org.apache.flink.table.planner.operations.SqlToOperationConverter;
import org.apache.flink.table.planner.parse.CalciteParser;
import org.apache.flink.table.resource.ResourceManager;

import org.apache.calcite.sql.SqlNode;

import java.net.URL;
import java.util.Collections;
import java.util.Optional;
import java.util.function.Supplier;

import static org.apache.calcite.jdbc.CalciteSchemaBuilder.asRootSchema;

/**
 * @Author: Jeff Zou @Date: 2022/8/8 17:13
 */
public class FlinkParser {
    private static final String CATALOG_NAME = "catalogLoader";
    private static final String DATABASE_NAME = "default";
    private final boolean isStreamingMode = true;
    private final TableConfig tableConfig = TableConfig.getDefault();
    private final Catalog catalog = new GenericInMemoryCatalog(CATALOG_NAME, DATABASE_NAME);
    private final CatalogManager catalogManager =
            preparedCatalogManager().defaultCatalog(CATALOG_NAME, catalog).build();
    private final ModuleManager moduleManager = new ModuleManager();

    ResourceManager resourceManager =
            ResourceManager.createResourceManager(
                    new URL[0],
                    Thread.currentThread().getContextClassLoader(),
                    tableConfig.getConfiguration());
    private final FunctionCatalog functionCatalog =
            new FunctionCatalog(tableConfig, resourceManager, catalogManager, moduleManager);
    private final Supplier<FlinkPlannerImpl> plannerSupplier =
            () -> getPlannerContext().createFlinkPlanner();

    private final PlannerContext plannerContext =
            new PlannerContext(
                    false,
                    tableConfig,
                    new ModuleManager(),
                    functionCatalog,
                    catalogManager,
                    asRootSchema(new CatalogManagerCalciteSchema(catalogManager, isStreamingMode)),
                    Collections.emptyList(),
                    Thread.currentThread().getContextClassLoader());

    private final Parser parser =
            new ParserImpl(
                    catalogManager,
                    plannerSupplier,
                    () -> plannerSupplier.get().parser(),
                    plannerContext.getRexFactory());

    private FlinkPlannerImpl planner;
    private CalciteParser calciteParser;

    private PlannerContext getPlannerContext() {
        return plannerContext;
    }

    public FlinkParser() {
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

    public static CatalogManager.Builder preparedCatalogManager() {
        return CatalogManager.newBuilder()
                .classLoader(FlinkParser.class.getClassLoader())
                .config(new Configuration())
                .defaultCatalog(
                        CATALOG_NAME, new GenericInMemoryCatalog(CATALOG_NAME, DATABASE_NAME))
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
                    .getCatalog(CATALOG_NAME)
                    .get()
                    .createDatabase(databaseName, catalogDatabase, true);
        } catch (Exception e) {

        }
    }

    public void creatTable(CreateTableOperation createTableOperation) {
        catalogManager.createTable(
                createTableOperation.getCatalogTable(),
                createTableOperation.getTableIdentifier(),
                createTableOperation.isIgnoreIfExists());
    }

    public CatalogBaseTable getTable(String databaseName, String objectName)
            throws TableNotExistException, CatalogException {
        ObjectPath objectPath = new ObjectPath(databaseName, objectName);
        return catalogManager.getCatalog(CATALOG_NAME).get().getTable(objectPath);
    }

    public SqlNode parseNode(String sql) {
        return calciteParser.parse(sql);
    }
}
