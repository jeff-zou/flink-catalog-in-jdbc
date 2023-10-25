package org.apache.flink.connector.jdbc.catalog.common;

import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogManager;

public class CatalogUtil {

    public static void createDatabase(
            String catalogName,
            String databaseName,
            CatalogDatabase catalogDatabase,
            CatalogManager catalogManager) {
        try {
            catalogManager
                    .getCatalog(catalogName)
                    .get()
                    .createDatabase(databaseName, catalogDatabase, true);
        } catch (Exception e) {

        }
    }
}
