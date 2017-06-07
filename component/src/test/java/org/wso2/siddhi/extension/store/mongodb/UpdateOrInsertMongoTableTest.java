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

package org.wso2.siddhi.extension.store.mongodb;

import com.mongodb.MongoException;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.stream.input.InputHandler;


public class UpdateOrInsertMongoTableTest {
    private static final Logger log = Logger.getLogger(UpdateOrInsertMongoTableTest.class);

    @BeforeClass
    public void init() {
        log.info("== Mongo Table UPDATE/INSERT tests started ==");
    }

    @AfterClass
    public void shutdown() {
        log.info("== Mongo Table UPDATE/INSERT tests completed ==");
    }

    @Test
    public void updateOrInsertMongoTableTest1() throws InterruptedException, MongoException {
        log.info("updateOrInsertMongoTableTest1");
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
            MongoTableTestUtils.clearCollection();
            String streams = "" +
                    "define stream StockStream (symbol string, price float, volume long); " +
                    "define stream UpdateStockStream (symbol string, price float, volume long); " +
                    "@Store(type = 'mongodb' , mongodb.uri='mongodb://admin:admin@127.0.0.1/Foo') " +
                    "define table FooTable (symbol string, price float, volume long);";
            String query = "" +
                    "@info(name = 'query1') " +
                    "from StockStream " +
                    "insert into FooTable ;" +
                    "" +
                    "@info(name = 'query2') " +
                    "from UpdateStockStream " +
                    "update or insert into FooTable " +
                    "" +
                    "   on FooTable.symbol== symbol ;";

            ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);
            InputHandler stockStream = executionPlanRuntime.getInputHandler("StockStream");
            InputHandler updateStockStream = executionPlanRuntime.getInputHandler("UpdateStockStream");
            executionPlanRuntime.start();

            stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
            stockStream.send(new Object[]{"GOOG", 75.6F, 100L});
            stockStream.send(new Object[]{"WSO2", 57.6F, 100L});
            updateStockStream.send(new Object[]{"GOOG", 10.6F, 100L});
            Thread.sleep(500);

            long totalDocumentsInCollection = MongoTableTestUtils.getDocumentsCount();
            Assert.assertEquals(totalDocumentsInCollection, 3, "Update failed");

            executionPlanRuntime.shutdown();
        } catch (MongoException e) {
            log.info("Test case 'updateOrInsertMongoTableTest1' ignored due to " + e.getMessage());
            throw e;
        }
    }
}
