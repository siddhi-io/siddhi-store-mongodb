/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.wso2.extension.siddhi.store.mongodb;

import com.mongodb.MongoException;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.stream.input.InputHandler;


public class UpdateMongoTableTest {
    private static final Logger log = Logger.getLogger(UpdateMongoTableTest.class);

    @BeforeClass
    public void init() {
        log.info("== Mongo Table UPDATE tests started ==");
    }

    @AfterClass
    public void shutdown() {
        log.info("== Mongo Table UPDATE tests completed ==");
    }

    @Test
    public void updateFromMongoTableTest1() throws InterruptedException, MongoException {
        log.info("updateFromMongoTableTest1");
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
            MongoTableTestUtils.clearCollection();
            String streams = "" +
                    "define stream StockStream (symbol string, price float, volume long); " +
                    "define stream UpdateStockStream (symbol string, price float, volume long); " +
                    "@Store(type = 'mongodb' , mongodb.uri='mongodb://admin:admin@127.0.0.1/Foo') " +
                    "@PrimaryKey('symbol','price')" +
                    "define table FooTable (symbol string, price float, volume long);";
            String query = "" +
                    "@info(name = 'query1') " +
                    "from StockStream " +
                    "insert into FooTable ;" +
                    "" +
                    "@info(name = 'query2') " +
                    "from UpdateStockStream " +
                    "update FooTable " +
                    "   on FooTable.symbol == 'IBM' ;";

            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
            InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
            InputHandler updateStockStream = siddhiAppRuntime.getInputHandler("UpdateStockStream");
            siddhiAppRuntime.start();

            stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
            stockStream.send(new Object[]{"IBM", 74.6f, 100L});
            stockStream.send(new Object[]{"WSO2_2", 57.6f, 100L});
            updateStockStream.send(new Object[]{"IBM_2", 575.6f, 500L});
            Thread.sleep(1000);

            long totalDocumentsInCollection = MongoTableTestUtils.getDocumentsCount();
            Assert.assertEquals(totalDocumentsInCollection, 3, "Update failed");


            siddhiAppRuntime.shutdown();
        } catch (MongoException e) {
            log.info("Test case 'updateFromMongoTableTest1' ignored due to " + e.getMessage());
            throw e;
        }
    }
}
