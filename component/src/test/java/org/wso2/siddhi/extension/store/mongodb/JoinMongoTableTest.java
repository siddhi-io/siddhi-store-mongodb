/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.query.api.exception.ExecutionPlanValidationException;

public class JoinMongoTableTest {
    private static final Logger log = Logger.getLogger(JoinMongoTableTest.class);
    private int inEventCount;
    private int removeEventCount;
    private boolean eventArrived;

    @BeforeClass
    public void init() {
        inEventCount = 0;
        removeEventCount = 0;
        eventArrived = false;
        log.info("== Mongo Table JOIN tests started ==");
    }

    @AfterClass
    public void shutdown() {
        log.info("== Mongo Table JOIN tests completed ==");
    }

    @Test
    public void testMongoTableJoinQuery1() throws InterruptedException, MongoException {
        log.info("testMongoTableJoinQuery1 -" +
                "DASC5-915:Read events from a MongoDB collection successfully");
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
            MongoTableTestUtils.clearCollection();
            String streams = "" +
                    "define stream StockStream (symbol string, price float, volume long); " +
                    "define stream CheckStockStream (symbol string); " +
                    "@Store(type = 'mongodb' , mongodb.uri='mongodb://admin:admin@127.0.0.1/Foo')" +
                    "define table FooTable (symbol string, price float, volume long);";
            String query = "" +
                    "@info(name = 'query1') " +
                    "from StockStream " +
                    "insert into FooTable ;" +
                    "" +
                    "@info(name = 'query2') " +
                    "from CheckStockStream#window.length(1) join FooTable " +
                    "select CheckStockStream.symbol as checkSymbol, FooTable.symbol as symbol, " +
                    "FooTable.volume as volume  " +
                    "insert into OutputStream ;";

            ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);
            executionPlanRuntime.addCallback("query2", new QueryCallback() {
                @Override
                public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                    if (inEvents != null) {
                        for (Event event : inEvents) {
                            inEventCount++;
                            switch (inEventCount) {
                                case 1:
                                    Assert.assertEquals(new Object[]{"WSO2_check", "WSO2", 100L}, event.getData());
                                    break;
                                case 2:
                                    Assert.assertEquals(new Object[]{"WSO2_check", "IBM", 10L}, event.getData());
                                    break;
                                default:
                                    break;
                            }
                        }
                        eventArrived = true;
                    }
                    if (removeEvents != null) {
                        removeEventCount = removeEventCount + removeEvents.length;
                    }
                    eventArrived = true;
                }

            });

            InputHandler stockStream = executionPlanRuntime.getInputHandler("StockStream");
            InputHandler checkStockStream = executionPlanRuntime.getInputHandler("CheckStockStream");
            executionPlanRuntime.start();

            stockStream.send(new Object[]{"WSO2", 5.6f, 100L});
            stockStream.send(new Object[]{"IBM", 7.6f, 10L});
            checkStockStream.send(new Object[]{"WSO2_check"});
            Thread.sleep(1000);
            executionPlanRuntime.shutdown();

            Assert.assertEquals(inEventCount, 2, "Number of success events");
            Assert.assertEquals(removeEventCount, 0, "Number of remove events");
            Assert.assertEquals(eventArrived, true, "Event arrived");
        } catch (MongoException e) {
            log.info("Test case 'testMongoTableJoinQuery1' ignored due to " + e.getMessage());
            throw e;
        }
    }

    @Test(expectedExceptions = ExecutionPlanValidationException.class)
    public void testMongoTableJoinQuery2() throws InterruptedException, MongoException {
        log.info("testMongoTableJoinQuery2DASC5-916:Read events from a non existing MongoDB collection");
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
            MongoTableTestUtils.clearCollection();
            String streams = "" +
                    "define stream StockStream (symbol string, price float, volume long); " +
                    "define stream CheckStockStream (symbol string); " +
                    "@Store(type = 'mongodb' , mongodb.uri='mongodb://admin:admin@127.0.0.1/Foo')" +
                    "define table FooTable (symbol string, price float, volume long);";
            String query = "" +
                    "@info(name = 'query1') " +
                    "from StockStream " +
                    "insert into FooTable ;" +
                    "" +
                    "@info(name = 'query2') " +
                    "from CheckStockStream#window.length(1) join FooTable123 " +
                    "select CheckStockStream.symbol as checkSymbol, FooTable.symbol as symbol, " +
                    "FooTable.volume as volume  " +
                    "insert into OutputStream ;";

            ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);
            executionPlanRuntime.start();
            executionPlanRuntime.shutdown();

        } catch (MongoException e) {
            log.info("Test case 'testMongoTableJoinQuery2' ignored due to " + e.getMessage());
            throw e;
        }
    }

    @Test(expectedExceptions = ExecutionPlanValidationException.class)
    public void testMongoTableJoinQuery3() throws InterruptedException, MongoException {
        log.info("testMongoTableJoinQuery - " +
                "DASC5-917:Read events from a MongoDB collection by sending through non existing stream");
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
            MongoTableTestUtils.clearCollection();
            String streams = "" +
                    "define stream StockStream (symbol string, price float, volume long); " +
                    "define stream CheckStockStream (symbol string); " +
                    "@Store(type = 'mongodb' , mongodb.uri='mongodb://admin:admin@127.0.0.1/Foo')" +
                    "define table FooTable (symbol string, price float, volume long);";
            String query = "" +
                    "@info(name = 'query1') " +
                    "from StockStream " +
                    "insert into FooTable ;" +
                    "" +
                    "@info(name = 'query2') " +
                    "from CheckStockStream123#window.length(1) join FooTable " +
                    "select CheckStockStream.symbol as checkSymbol, FooTable.symbol as symbol, " +
                    "FooTable.volume as volume  " +
                    "insert into OutputStream ;";

            ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);
            executionPlanRuntime.start();
            executionPlanRuntime.shutdown();

        } catch (MongoException e) {
            log.info("Test case 'testMongoTableJoinQuery3' ignored due to " + e.getMessage());
            throw e;
        }
    }

    @Test(expectedExceptions = ExecutionPlanValidationException.class)
    public void testMongoTableJoinQuery5() throws InterruptedException, MongoException {
        log.info("testMongoTableJoinQuery5 - " +
                "DASC5-919:Read events from a MongoDB collection for non existing attributes");
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
            MongoTableTestUtils.clearCollection();
            String streams = "" +
                    "define stream StockStream (symbol string, price float, volume long); " +
                    "define stream CheckStockStream (symbol string); " +
                    "@Store(type = 'mongodb' , mongodb.uri='mongodb://admin:admin@127.0.0.1/Foo')" +
                    "define table FooTable (symbol string, price float, volume long);";
            String query = "" +
                    "@info(name = 'query1') " +
                    "from StockStream " +
                    "insert into FooTable ;" +
                    "" +
                    "@info(name = 'query2') " +
                    "from CheckStockStream123#window.length(1) join FooTable " +
                    "select CheckStockStream.hello as checkHello, FooTable.symbol as symbol, " +
                    "FooTable.volume as volume  " +
                    "insert into OutputStream ;";

            ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);
            executionPlanRuntime.start();
            executionPlanRuntime.shutdown();

        } catch (MongoException e) {
            log.info("Test case 'testMongoTableJoinQuery5' ignored due to " + e.getMessage());
            throw e;
        }
    }

}
