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

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogView;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.QueryOperationCatalogView;
import org.apache.flink.table.catalog.ResolvedCatalogBaseTable;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.utils.EncodingUtils;

import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @Author: Jeff Zou @Date: 2022/8/8 14:35
 */
public class ShowCreateUtils {

    private ShowCreateUtils() {
    }

    static String buildCreateFormattedPrefix(
            String tableType, String databaseName, String tableName, boolean isTemporary) {
        return String.format(
                "CREATE %s%s %s.%s (%s",
                isTemporary ? "TEMPORARY " : "",
                tableType,
                databaseName,
                tableName,
                System.lineSeparator());
    }

    static Optional<String> extractFormattedPrimaryKey(
            ResolvedCatalogBaseTable<?> table, String printIndent) {
        Optional<UniqueConstraint> primaryKey = table.getResolvedSchema().getPrimaryKey();
        return primaryKey.map(
                (uniqueConstraint) -> {
                    return String.format("%s%s", printIndent, uniqueConstraint);
                });
    }

    static String getColumnString(Column column) {
        StringBuilder sb = new StringBuilder();
        sb.append(EncodingUtils.escapeIdentifier(column.getName()));
        sb.append(" ");
        if (column instanceof Column.ComputedColumn) {
            sb.append(
                    (String) column.explainExtras()
                            .orElseThrow(
                                    () -> {
                                        return new TableException(
                                                String.format(
                                                        "Column expression can not be null for computed column '%s'",
                                                        column.getName()));
                                    }));
        } else {
            sb.append(column.getDataType().getLogicalType().asSerializableString());
            column.explainExtras()
                    .ifPresent(
                            (e) -> {
                                sb.append(" ");
                                sb.append(e);
                            });
        }

        return sb.toString();
    }

    static String extractFormattedColumns(ResolvedCatalogBaseTable<?> table, String printIndent) {
        return (String) table.getResolvedSchema().getColumns().stream()
                .map(
                        (column) -> {
                            return String.format(
                                    "%s%s", printIndent, getColumnString(column));
                        })
                .collect(Collectors.joining(",\n"));
    }

    static Optional<String> extractFormattedWatermarkSpecs(
            ResolvedCatalogBaseTable<?> table, String printIndent) {
        return table.getResolvedSchema().getWatermarkSpecs().isEmpty()
                ? Optional.empty()
                : Optional.of(
                        table.getResolvedSchema().getWatermarkSpecs().stream()
                                .map(
                                        (watermarkSpec) -> {
                                            return String.format(
                                                    "%sWATERMARK FOR %s AS %s",
                                                    printIndent,
                                                    EncodingUtils.escapeIdentifier(
                                                            watermarkSpec.getRowtimeAttribute()),
                                                    watermarkSpec
                                                            .getWatermarkExpression()
                                                            .asSerializableString());
                                        })
                                .collect(Collectors.joining("\n")));
    }

    static Optional<String> extractFormattedComment(ResolvedCatalogBaseTable<?> table) {
        String comment = table.getComment();
        return StringUtils.isNotEmpty(comment)
                ? Optional.of(EncodingUtils.escapeSingleQuotes(comment))
                : Optional.empty();
    }

    static Optional<String> extractFormattedPartitionedInfo(ResolvedCatalogTable catalogTable) {
        return !catalogTable.isPartitioned()
                ? Optional.empty()
                : Optional.of(
                        catalogTable.getPartitionKeys().stream()
                                .map(EncodingUtils::escapeIdentifier)
                                .collect(Collectors.joining(", ")));
    }

    static Optional<String> extractFormattedOptions(
            ResolvedCatalogBaseTable<?> table, String printIndent) {
        return !Objects.isNull(table.getOptions()) && !table.getOptions().isEmpty()
                ? Optional.of(
                        table.getOptions().entrySet().stream()
                                .map(
                                        (entry) -> {
                                            return String.format(
                                                    "%s'%s' = '%s'",
                                                    printIndent,
                                                    EncodingUtils.escapeSingleQuotes(
                                                            (String) entry.getKey()),
                                                    EncodingUtils.escapeSingleQuotes(
                                                            (String) entry.getValue()));
                                        })
                                .collect(Collectors.joining(",\n")))
                : Optional.empty();
    }

    static String extractFormattedColumnNames(ResolvedCatalogBaseTable<?> baseTable) {
        return (String) baseTable.getResolvedSchema().getColumns().stream()
                .map(Column::getName)
                .map(EncodingUtils::escapeIdentifier)
                .collect(Collectors.joining(", "));
    }

    public static String buildShowCreateTableRow(
            ResolvedCatalogBaseTable<?> table,
            String databaseName,
            String tableName,
            boolean isTemporary) {
        if (table.getTableKind() == CatalogBaseTable.TableKind.VIEW) {
            throw new TableException(
                    String.format(
                            "SHOW CREATE TABLE is only supported for tables, but %s is a view. Please use SHOW CREATE VIEW instead.",
                            tableName));
        } else {
            String printIndent = "  ";
            StringBuilder sb =
                    (new StringBuilder())
                            .append(
                                    buildCreateFormattedPrefix(
                                            "TABLE", databaseName, tableName, isTemporary));
            sb.append(extractFormattedColumns(table, "  "));
            extractFormattedWatermarkSpecs(table, "  ")
                    .ifPresent(
                            (watermarkSpecs) -> {
                                sb.append(",\n").append(watermarkSpecs);
                            });
            extractFormattedPrimaryKey(table, "  ")
                    .ifPresent(
                            (pk) -> {
                                sb.append(",\n").append(pk);
                            });
            sb.append("\n) ");
            extractFormattedComment(table)
                    .ifPresent(
                            (c) -> {
                                sb.append(
                                        String.format("COMMENT '%s'%s", c, System.lineSeparator()));
                            });
            extractFormattedPartitionedInfo((ResolvedCatalogTable) table)
                    .ifPresent(
                            (partitionedInfoFormatted) -> {
                                sb.append("PARTITIONED BY (")
                                        .append(partitionedInfoFormatted)
                                        .append(")\n");
                            });
            extractFormattedOptions(table, "  ")
                    .ifPresent(
                            (v) -> {
                                sb.append("WITH (\n").append(v).append("\n)\n");
                            });
            return sb.toString();
        }
    }

    public static String buildShowCreateViewRow(
            ResolvedCatalogBaseTable<?> view,
            String databaseName,
            String tableName,
            boolean isTemporary) {
        if (view.getTableKind() != CatalogBaseTable.TableKind.VIEW) {
            throw new TableException(
                    String.format(
                            "SHOW CREATE VIEW is only supported for views, but %s is a table. Please use SHOW CREATE TABLE instead.",
                            tableName));
        } else {
            StringBuilder stringBuilder = new StringBuilder();
            if (view.getOrigin() instanceof QueryOperationCatalogView) {
                throw new TableException(
                        "SHOW CREATE VIEW is not supported for views registered by Table API.");
            } else {
                stringBuilder.append(
                        String.format(
                                "CREATE %sVIEW %s.%s%s as%s%s",
                                isTemporary ? "TEMPORARY " : "",
                                databaseName,
                                tableName,
                                String.format("(%s)", extractFormattedColumnNames(view)),
                                System.lineSeparator(),
                                ((CatalogView) view.getOrigin()).getExpandedQuery()));
                extractFormattedComment(view)
                        .ifPresent(
                                (c) -> {
                                    stringBuilder.append(
                                            String.format(
                                                    " COMMENT '%s'%s", c, System.lineSeparator()));
                                });
                return stringBuilder.toString();
            }
        }
    }
}
