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

package org.apache.flink.connector.jdbc.catalog.factory;

import static org.apache.flink.connector.jdbc.catalog.factory.GenericInJdbcCatalogFactoryOptions.*;
import static org.apache.flink.table.factories.FactoryUtil.PROPERTY_VERSION;

import com.zaxxer.hikari.HikariConfig;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.connector.jdbc.catalog.GenericInJdbcCatalog;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

/** Factory for {@link GenericInJdbcCatalog}. */
public class GenericInJdbcCatalogFactory implements CatalogFactory {

    private static final Logger LOG = LoggerFactory.getLogger(GenericInJdbcCatalogFactory.class);

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(DEFAULT_DATABASE);
        options.add(USERNAME);
        options.add(PASSWORD);
        options.add(URL);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(PROPERTY_VERSION);
        options.add(SECRET_KEY);
        options.add(POOL_MAX_SIZE);
        options.add(POOL_MIN_IDLE);
        options.add(MAX_LIFE_TIME);
        options.add(CONNECTOR_TIMEOUT);
        options.add(JDBC_DRIVER_CLASS);
        options.add(CONNECTON_TEST_QUERY);
        return options;
    }

    @Override
    public Catalog createCatalog(Context context) {
        final FactoryUtil.CatalogFactoryHelper helper =
                FactoryUtil.createCatalogFactoryHelper(this, context);
        helper.validate();

        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setPassword(helper.getOptions().get(PASSWORD));
        hikariConfig.setUsername(helper.getOptions().get(USERNAME));
        hikariConfig.setJdbcUrl(helper.getOptions().get(URL));
        hikariConfig.setMinimumIdle(helper.getOptions().get(POOL_MIN_IDLE));
        hikariConfig.setMaximumPoolSize(helper.getOptions().get(POOL_MAX_SIZE));
        hikariConfig.setConnectionTimeout(helper.getOptions().get(CONNECTOR_TIMEOUT));
        hikariConfig.setMaxLifetime(helper.getOptions().get(MAX_LIFE_TIME));
        hikariConfig.setDriverClassName(helper.getOptions().get(JDBC_DRIVER_CLASS));
        hikariConfig.setConnectionTestQuery(helper.getOptions().get(CONNECTON_TEST_QUERY));
        return new GenericInJdbcCatalog(
                context.getName(),
                helper.getOptions().get(DEFAULT_DATABASE),
                helper.getOptions().get(SECRET_KEY),
                hikariConfig);
    }
}
