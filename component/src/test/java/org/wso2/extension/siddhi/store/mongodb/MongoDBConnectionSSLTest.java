/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.wso2.extension.siddhi.store.mongodb;

import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;

public class MongoDBConnectionSSLTest {
    private static final Logger log = Logger.getLogger(MongoDBConnectionSSLTest.class);

    private static String uri;

    @BeforeClass
    public void init() {
        log.info("== Mongo Table Secure Connection tests started ==");
        uri = MongoTableTestUtils.resolveUri();
    }

    @AfterClass
    public void shutdown() {
        log.info("== Mongo Table Secure Connection tests completed ==");
    }

    @Test
    public void mongoTableSSLConnectionTest1() {
        log.info("mongoTableSSLConnectionTest1 - " +
                "   DASC5-862:Defining a MongoDB table with by defining ssl in mongodburl");

        MongoTableTestUtils.dropCollection("FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@store(type = 'mongodb', " +
                "mongodb.uri=''" + uri + "?ssl=true')" +
                "@PrimaryKey('symbol')" +
                "define table FooTable (symbol string, price float, volume long); ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();

        boolean doesCollectionExists = MongoTableTestUtils.doesCollectionExists("FooTable");
        Assert.assertEquals(doesCollectionExists, true, "Definition failed");
    }

    @Test
    public void mongoTableSSLConnectionTest2() {
        log.info("mongoTableSSLConnectionTest2 - " +
                "DASC5-863:Defining a MongoDB table with multiple options defined in mongodburl");

        MongoTableTestUtils.dropCollection("FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@store(type = 'mongodb', " +
                "mongodb.uri='" + uri + "?ssl=true&maxPoolSize=100')" +
                "@PrimaryKey('symbol')" +
                "define table FooTable (symbol string, price float, volume long); ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();

        boolean doesCollectionExists = MongoTableTestUtils.doesCollectionExists("FooTable");
        Assert.assertEquals(doesCollectionExists, true, "Definition failed");
    }
}
