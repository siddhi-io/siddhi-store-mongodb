/*
 * Copyright (c) 2026, WSO2 LLC. (https://www.wso2.com).
 *
 * WSO2 LLC. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.siddhi.extension.store.mongodb;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.extension.store.mongodb.util.MongoTableUtils;
import io.siddhi.query.api.annotation.Annotation;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class MongoTableUtilsTest {

    @Test
    public void testConnectionTimeoutIsApplied() {
        Map<String, String> configuration = new HashMap<>();
        configuration.put("connectTimeout", "120000");
        ConfigReader configReader = new MapConfigReader(configuration);

        MongoClientSettings settings = MongoTableUtils.extractMongoClientSettings(
                new ConnectionString("mongodb://localhost:27017/test"),
                Annotation.annotation("store"), configReader);

        Assert.assertEquals(settings.getSocketSettings().getConnectTimeout(TimeUnit.MILLISECONDS), 120000);
    }

    private static class MapConfigReader implements ConfigReader {
        private final Map<String, String> configuration;

        MapConfigReader(Map<String, String> configuration) {
            this.configuration = configuration;
        }

        @Override
        public String readConfig(String name, String defaultValue) {
            return configuration.containsKey(name) ? configuration.get(name) : defaultValue;
        }

        @Override
        public Map<String, String> getAllConfigs() {
            return Collections.unmodifiableMap(configuration);
        }
    }
}
