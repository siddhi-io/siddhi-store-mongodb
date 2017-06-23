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

public class MongoDBMultipleHostsConnectionTest {
    private static final Logger log = Logger.getLogger(MongoDBMultipleHostsConnectionTest.class);

    private static String uri;

    @BeforeClass
    public void init() {
        log.info("== Mongo Table Replica Set Connection tests started ==");
        uri = MongoTableTestUtils.resolveUri();
    }

    @AfterClass
    public void shutdown() {
        log.info("== Mongo Table Replica Set Connection tests completed ==");
    }


    @Test
    public void mongoTableMultipleHostsConnectionTest1() {
        log.info("mongoTableMultipleHostsConnectionTest1 - " +
                "DASC5-962:Defining a MongoDB event table with multiple hosts and default ports");

        MongoTableTestUtils.dropCollection("FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@store(type = 'mongodb' , " +
                "mongodb.uri=\"mongodb://admin:admin@localhost:27017,192.168.11.17:27017/Foo\") " +
                "define table FooTable (symbol string, price float, volume long); ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();

        boolean doesCollectionExists = MongoTableTestUtils.doesCollectionExists("FooTable");
        Assert.assertEquals(doesCollectionExists, true, "Definition failed");
    }

    @Test
    public void mongoTableMultipleHostsConnectionTest2() {
        log.info("mongoTableMultipleHostsConnectionTest2 - " +
                "DASC5-963:Defining a MongoDB event table with multiple hosts and non-default ports");

        MongoTableTestUtils.dropCollection("FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@store(type = 'mongodb' , " +
                "mongodb.uri=\"mongodb://admin:admin@localhost:27018,192.168.11.17:27018/Foo\") " +
                "define table FooTable (symbol string, price float, volume long); ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();

        boolean doesCollectionExists = MongoTableTestUtils.doesCollectionExists("FooTable");
        Assert.assertEquals(doesCollectionExists, true, "Definition failed");
    }


}
