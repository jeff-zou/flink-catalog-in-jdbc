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

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.catalog.CommonCatalogOptions;

/**
 * {@link ConfigOption}s for {@link org.apache.flink.connector.jdbc.catalog.GenericInJdbcCatalog}.
 */
@Internal
public class GenericInJdbcCatalogFactoryOptions {

    public static final String IDENTIFIER = "generic_in_jdbc";

    public static final ConfigOption<String> DEFAULT_DATABASE =
            ConfigOptions.key(CommonCatalogOptions.DEFAULT_DATABASE_KEY)
                    .stringType()
                    .noDefaultValue();

    public static final ConfigOption<String> USERNAME =
            ConfigOptions.key("username").stringType().noDefaultValue();

    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password").stringType().noDefaultValue();

    public static final ConfigOption<String> URL =
            ConfigOptions.key("url").stringType().noDefaultValue();

    public static final ConfigOption<String> SECRET_KEY =
            ConfigOptions.key("secret.key").stringType().noDefaultValue();

    public static final ConfigOption<String> JDBC_DRIVER_CLASS =
            ConfigOptions.key("jdbc.driver.class")
                    .stringType()
                    .defaultValue("com.mysql.cj.jdbc.Driver");

    public static final ConfigOption<String> CONNECTON_TEST_QUERY =
            ConfigOptions.key("connection.test.query").stringType().defaultValue("SELECT 1");

    public static final ConfigOption<Integer> POOL_MAX_SIZE =
            ConfigOptions.key("pool.max.size").intType().defaultValue(2);

    public static final ConfigOption<Integer> POOL_MIN_IDLE =
            ConfigOptions.key("pool.min.idle").intType().defaultValue(1);

    public static final ConfigOption<Integer> MAX_LIFE_TIME =
            ConfigOptions.key("max.life.time")
                    .intType()
                    .defaultValue(1800000)
                    .withDescription("set maximum lifetime for the connection (milliseconds)");

    public static final ConfigOption<Integer> CONNECTOR_TIMEOUT =
            ConfigOptions.key("connect.time.out")
                    .intType()
                    .defaultValue(30000)
                    .withDescription("set connection timeout (milliseconds)");

    private GenericInJdbcCatalogFactoryOptions() {}
}
