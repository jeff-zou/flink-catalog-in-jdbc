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

import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogPropertiesUtil;
import org.apache.flink.table.catalog.CatalogView;
import org.apache.flink.table.catalog.ResolvedCatalogView;
import org.apache.flink.table.catalog.ResolvedSchema;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CatalogViewSerialize {

    private Method deserializeSchemaMethod;

    private Method serializeSchemaMethod;

    public CatalogViewSerialize() throws NoSuchMethodException {
        Class<CatalogPropertiesUtil> cls = CatalogPropertiesUtil.class;
        deserializeSchemaMethod = cls.getDeclaredMethod("deserializeSchema", Map.class);
        deserializeSchemaMethod.setAccessible(true);

        serializeSchemaMethod =
                cls.getDeclaredMethod("serializeResolvedSchema", Map.class, ResolvedSchema.class);
        serializeSchemaMethod.setAccessible(true);
    }

    private final String SCHEMA = "schema";
    private final String SEPARATOR = ".";
    private final String COMMENT = "comment";
    private final String PARTITION = "partition";
    private final String KEYS = "keys";
    private final String EXPANDED_QUERY = "expanded_query";
    private final String ORIGINAL_QUERY = "original_query";
    private final String PARTITION_KEYS = compoundKey(PARTITION, KEYS);

    private Map<String, String> deserializeOptions(Map<String, String> map) {
        return map.entrySet().stream()
                .filter(
                        e -> {
                            final String key = e.getKey();
                            return !key.startsWith(SCHEMA + SEPARATOR)
                                    && !key.startsWith(PARTITION_KEYS + SEPARATOR)
                                    && !key.equals(COMMENT);
                        })
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public CatalogView deserializeCatalogView(Map<String, String> properties)
            throws InvocationTargetException, IllegalAccessException {
        Map<String, String> options = deserializeOptions(properties);
        Schema schema =
                (Schema) deserializeSchemaMethod.invoke(CatalogPropertiesUtil.class, properties);
        String comment = properties.get(COMMENT);
        String expandedQuery = properties.get(EXPANDED_QUERY);
        String originalQuery = properties.get(ORIGINAL_QUERY);
        return CatalogView.of(schema, comment, originalQuery, expandedQuery, options);
    }

    public Map<String, String> serializeCatalogView(ResolvedCatalogView view)
            throws InvocationTargetException, IllegalAccessException {
        Map<String, String> properties = new HashMap<>();
        serializeSchemaMethod.invoke(
                CatalogPropertiesUtil.class, properties, view.getResolvedSchema());
        properties.put(COMMENT, view.getComment());
        properties.put(EXPANDED_QUERY, view.getExpandedQuery());
        properties.put(ORIGINAL_QUERY, view.getOriginalQuery());
        return properties;
    }

    private String compoundKey(Object... components) {
        return Stream.of(components).map(Object::toString).collect(Collectors.joining(SEPARATOR));
    }
}
