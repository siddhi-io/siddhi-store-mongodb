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
import org.wso2.siddhi.query.api.exception.DuplicateDefinitionException;
import org.wso2.siddhi.query.api.exception.ExecutionPlanValidationException;
import org.wso2.siddhi.query.compiler.exception.SiddhiParserException;


public class InsertIntoMongoTableTest {
    private static final Logger log = Logger.getLogger(InsertIntoMongoTableTest.class);

    @BeforeClass
    public void init() {
        log.info("== Mongo Table INSERT tests started ==");
    }

    @AfterClass
    public void shutdown() {
        log.info("== Mongo Table INSERT tests completed ==");
    }

    @Test
    public void insertIntoMongoTableTest1() throws InterruptedException {
        log.info("insertIntoMongoTableTest1 - DASC5-877:Insert events to a MongoDB table successfully");
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
            MongoTableTestUtils.clearCollection();
            String streams = "" +
                    "@Store(type = 'mongodb' , mongodb.uri='mongodb://admin:admin@127.0.0.1:27017/Foo') " +
                    "define table FooTable (symbol string, price float, volume long); " +
                    "define stream StockStream (symbol string, price float, volume long);";
            String query = "" +
                    "@info(name = 'query1') " +
                    "from StockStream   " +
                    "insert into FooTable ;";

            ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);
            InputHandler stockStream = executionPlanRuntime.getInputHandler("StockStream");
            executionPlanRuntime.start();

            stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
            stockStream.send(new Object[]{"IBM", 75.6f, 100L});
            stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
            stockStream.send(new Object[]{"IBM", 75.6f, 100L});
            executionPlanRuntime.shutdown();

            long totalDocumentsInCollection = MongoTableTestUtils.getDocumentsCount();
            Assert.assertEquals(totalDocumentsInCollection, 4, "Insertion failed");
        } catch (MongoException e) {
            log.info("Test case 'insertIntoMongoTableTest1' ignored due to " + e.getMessage());
            throw e;
        }
    }

    @Test(expectedExceptions = DuplicateDefinitionException.class)
    public void insertIntoMongoTableTest2() throws InterruptedException {
        log.info("insertIntoMongoTableTest2 - " +
                "DASC5-878:Insert events to a MongoDB table when query has less attributes to select from");
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
            MongoTableTestUtils.clearCollection();
            String streams = "" +
                    "@Store(type = 'mongodb' , mongodb.uri='mongodb://admin:admin@127.0.0.1:27017/Foo') " +
                    "define table FooTable (symbol string, price float, volume long); " +
                    "@source(type='inMemory')" +
                    "define stream StockStream (symbol string, price float, volume long);";
            String query = "" +
                    "@info(name = 'query1') " +
                    "from StockStream  " +
                    "select symbol, price " +
                    "insert into FooTable ;";

            ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);
            InputHandler stockStream = executionPlanRuntime.getInputHandler("StockStream");
            executionPlanRuntime.start();

            stockStream.send(new Object[]{"WSO2", 55.6f});
            stockStream.send(new Object[]{"IBM", 75.6f, 100L});
            stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
            stockStream.send(new Object[]{75.6f, 100L});

            executionPlanRuntime.shutdown();

            long totalDocumentsInCollection = MongoTableTestUtils.getDocumentsCount();
            Assert.assertEquals(totalDocumentsInCollection, 2, "Insertion Failed");
        } catch (MongoException e) {
            log.info("Test case 'insertIntoMongoTableTest2' ignored due to " + e.getMessage());
            throw e;
        }
    }

    @Test(expectedExceptions = ArrayIndexOutOfBoundsException.class)
    public void insertIntoMongoTableTest3() throws InterruptedException {
        log.info("insertIntoMongoTableTest3 - " +
                "DASC5-879:[N] Insert events to a MongoDB table when query has more attributes to select from");
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
            MongoTableTestUtils.clearCollection();
            String streams = "" +
                    "@Store(type = 'mongodb' , mongodb.uri='mongodb://admin:admin@127.0.0.1:27017/Foo') " +
                    "define table FooTable (symbol string, price float, volume long); " +
                    "define stream StockStream (symbol string, price float, volume long);";
            String query = "" +
                    "@info(name = 'query1') " +
                    "from StockStream   " +
                    "insert into FooTable ;";

            ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);
            InputHandler stockStream = executionPlanRuntime.getInputHandler("StockStream");
            executionPlanRuntime.start();

            stockStream.send(new Object[]{"WSO2", 55.6f, 100L, 12});
            stockStream.send(new Object[]{"IBM", 75.6f, 100L, true});
            stockStream.send(new Object[]{"WSO2", 55.6f, 100L});

            executionPlanRuntime.shutdown();
        } catch (MongoException e) {
            log.info("Test case 'insertIntoMongoTableTest3' ignored due to " + e.getMessage());
            throw e;
        }
    }


    @Test
    public void insertIntoMongoTableTest4() throws InterruptedException {
        log.info("insertIntoMongoTableTest4 - " +
                "DASC5-880:[N] Insert events to a non existing MongoDB table");
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
            MongoTableTestUtils.clearCollection();
            String streams = "" +
                    "@Store(type = 'mongodb' , mongodb.uri='mongodb://admin:admin@127.0.0.1:27017/Foo') " +
                    "define table FooTable (symbol string, price float, volume long); " +
                    "define stream StockStream (symbol string, price float, volume long);";
            String query = "" +
                    "@info(name = 'query1') " +
                    "from StockStream   " +
                    "insert into FooTable1234 ;";

            ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);
            InputHandler stockStream = executionPlanRuntime.getInputHandler("StockStream");
            executionPlanRuntime.start();

            stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
            stockStream.send(new Object[]{"IBM", 75.6f, 100L});
            stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
            executionPlanRuntime.shutdown();

            boolean doesCollectionExists = MongoTableTestUtils.doesCollectionExists();
            Assert.assertEquals(doesCollectionExists, true, "Definition failed");

            long totalDocumentsInCollection = MongoTableTestUtils.getDocumentsCount();
            Assert.assertEquals(totalDocumentsInCollection, 0, "Insertion failed");

        } catch (MongoException e) {
            log.info("Test case 'insertIntoMongoTableTest4' ignored due to " + e.getMessage());
            throw e;
        }
    }

    @Test(expectedExceptions = ExecutionPlanValidationException.class)
    public void insertIntoMongoTableTest5() throws InterruptedException {
        log.info("insertIntoMongoTableTest5 - " +
                "DASC5-883:[N] Insert events to a MongoDB table by selecting from non existing stream");
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
            MongoTableTestUtils.clearCollection();
            String streams = "" +
                    "@Store(type = 'mongodb' , mongodb.uri='mongodb://admin:admin@127.0.0.1:27017/Foo') " +
                    "define table FooTable (symbol string, price float, volume long); " +
                    "" +
                    "define stream StockStream (symbol string, price float, volume long);";
            String query = "" +
                    "@info(name = 'query1') " +
                    "from StockStream1212   " +
                    "insert into FooTable ;";

            ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);
            executionPlanRuntime.start();
            executionPlanRuntime.shutdown();

        } catch (MongoException e) {
            log.info("Test case 'insertIntoMongoTableTest5' ignored due to " + e.getMessage());
            throw e;
        }
    }

    @Test(expectedExceptions = SiddhiParserException.class)
    public void insertIntoMongoTableTest6() throws InterruptedException {
        log.info("insertIntoMongoTableTest6 - " +
                "DASC5-888:[N] Insert events to a MongoDB table when the stream has not defined");
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
            MongoTableTestUtils.clearCollection();
            String streams = "" +
                    "@Store(type = 'mongodb' , mongodb.uri='mongodb://admin:admin@127.0.0.1:27017/Foo') " +
                    "define table FooTable (symbol string, price float, volume long); ";
            String query = "" +
                    "@info(name = 'query1') " +
                    "from StockStream" +
                    "insert into FooTable ;";

            ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);
            executionPlanRuntime.start();
            executionPlanRuntime.shutdown();

        } catch (MongoException e) {
            log.info("Test case 'insertIntoMongoTableTest6' ignored due to " + e.getMessage());
            throw e;
        }
    }

    @Test
    public void insertIntoMongoTableTest7() throws InterruptedException {
        log.info("insertIntoMongoTableTest7 - " +
                "DASC5-889:[N] Insert events data to MongoDB table when the table has not defined");
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
            MongoTableTestUtils.clearCollection();
            String streams = "" +
                    "define stream StockStream (symbol string, price float, volume long);";
            String query = "" +
                    "@info(name = 'query1') " +
                    "from StockStream " +
                    "insert into FooTable ;";

            ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);
            executionPlanRuntime.start();
            executionPlanRuntime.shutdown();


        } catch (MongoException e) {
            log.info("Test case 'insertIntoMongoTableTest7' ignored due to " + e.getMessage());
            throw e;
        }
    }


    @Test
    public void insertIntoMongoTableTest8() throws InterruptedException {
        log.info("insertIntoMongoTableTest8 - " +
                "DASC5-890:Insert events to a MongoDB table when there are multiple primary keys defined");
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
            MongoTableTestUtils.clearCollection();

            String streams = "" +
                    "@Store(type = 'mongodb' , mongodb.uri='mongodb://admin:admin@127.0.0.1/Foo') " +
                    "@PrimaryKey('symbol','price')" +
                    "define table FooTable (symbol string, price float, volume long); " +
                    "define stream StockStream (symbol string, price float, volume long);";
            String query = "" +
                    "@info(name = 'query1') " +
                    "from StockStream   " +
                    "insert into FooTable ;";

            ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);
            InputHandler stockStream = executionPlanRuntime.getInputHandler("StockStream");
            executionPlanRuntime.start();

            stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
            stockStream.send(new Object[]{"IBM", 75.6F, 100L});
            stockStream.send(new Object[]{"MSFT", 57.6F, 100L});
            Thread.sleep(1000);

            executionPlanRuntime.shutdown();

            boolean doesCollectionExists = MongoTableTestUtils.doesCollectionExists();
            Assert.assertEquals(doesCollectionExists, true, "Definition failed");

        } catch (MongoException e) {
            log.info("Test case 'insertIntoMongoTableTest8' ignored due to " + e.getMessage());
            throw e;
        }
    }

    @Test
    public void insertIntoMongoTableTest9() throws InterruptedException {
        log.info("insertIntoMongoTableTest9 - " +
                "DASC5-892:Insert an event to a MongoDB table when the same value was " +
                "inserted for a defined primary key");
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
            MongoTableTestUtils.clearCollection();
            String streams = "" +
                    "@Store(type = 'mongodb' , mongodb.uri='mongodb://admin:admin@127.0.0.1/Foo') " +
                    "@PrimaryKey('symbol')" +
                    "define table FooTable (symbol string, price float, volume long); " +
                    "define stream StockStream (symbol string, price float, volume long);";
            String query = "" +
                    "@info(name = 'query1') " +
                    "from StockStream   " +
                    "insert into FooTable ;";

            ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams + query);
            InputHandler stockStream = executionPlanRuntime.getInputHandler("StockStream");
            executionPlanRuntime.start();

            stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
            stockStream.send(new Object[]{"IBM", 75.6F, 100L});
            stockStream.send(new Object[]{"WSO2", 57.1F, 100L});
            Thread.sleep(1000);

            executionPlanRuntime.shutdown();

            long totalDocumentsInCollection = MongoTableTestUtils.getDocumentsCount();
            Assert.assertEquals(totalDocumentsInCollection, 2, "Primary Key Duplication failed");
        } catch (MongoException e) {
            log.info("Test case 'insertIntoMongoTableTest9' ignored due to " + e.getMessage());
            throw e;
        }
    }
}
