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

import org.apache.log4j.Logger;
import org.bson.Document;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.query.api.exception.SiddhiAppValidationException;

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
    public void updateOrInsertMongoTableTest1() throws InterruptedException {
        log.info("updateOrInsertMongoTableTest1 - DASC5-929:Configure siddhi to perform insert/update on " +
                "MongoDB document");

        MongoTableTestUtils.dropCollection("FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@store(type = 'mongodb' , mongodb.uri='mongodb://admin:admin@127.0.0.1/Foo') " +
                "define table FooTable (symbol string, price float, volume long);";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into FooTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from FooStream " +
                "update or insert into FooTable " +
                "   on FooTable.symbol== symbol ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        stockStream.send(new Object[]{"GOOG", 75.6F, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6F, 100L});
        fooStream.send(new Object[]{"GOOG", 10.6, 100});
        Thread.sleep(500);

        siddhiAppRuntime.shutdown();

        long totalDocumentsInCollection = MongoTableTestUtils.getDocumentsCount("FooTable");
        Assert.assertEquals(totalDocumentsInCollection, 3, "Update failed");

        Document expectedUpdatedDocument = new Document()
                .append("symbol", "GOOG")
                .append("price", 10.6)
                .append("volume", 100);
        Document updatedDocument = MongoTableTestUtils.getDocument("FooTable", "{symbol:'GOOG'}");
        Assert.assertEquals(updatedDocument, expectedUpdatedDocument, "Update Failed");
    }

    @Test
    public void updateOrInsertMongoTableTest2() throws InterruptedException {
        log.info("updateOrInsertMongoTableTest2 - DASC5-930:Configure siddhi to perform insert/update on MongoDB " +
                "Document when no any matching record exist");

        MongoTableTestUtils.dropCollection("FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@store(type = 'mongodb' , mongodb.uri='mongodb://admin:admin@127.0.0.1/Foo') " +
                "define table FooTable (symbol string, price float, volume long);";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into FooTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from FooStream " +
                "update or insert into FooTable " +
                "   on FooTable.symbol== symbol ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        stockStream.send(new Object[]{"GOOG", 75.6F, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6F, 100L});
        fooStream.send(new Object[]{"GOOG_2", 10.6, 100});
        Thread.sleep(500);

        siddhiAppRuntime.shutdown();

        long totalDocumentsInCollection = MongoTableTestUtils.getDocumentsCount("FooTable");
        Assert.assertEquals(totalDocumentsInCollection, 4, "Update failed");

        Document expectedUpdatedDocument = new Document()
                .append("symbol", "GOOG_2")
                .append("price", 10.6)
                .append("volume", 100);
        Document updatedDocument = MongoTableTestUtils.getDocument("FooTable", "{symbol:'GOOG_2'}");
        Assert.assertEquals(updatedDocument, expectedUpdatedDocument, "Update Failed");
    }

    @Test
    public void updateOrInsertMongoTableTest3() throws InterruptedException {
        log.info("updateOrInsertMongoTableTest3 - DASC5-931:Configure siddhi to perform insert/update when there " +
                "are some of matching records exist");

        MongoTableTestUtils.dropCollection("FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@store(type = 'mongodb' , mongodb.uri='mongodb://admin:admin@127.0.0.1/Foo') " +
                "define table FooTable (symbol string, price float, volume long);";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into FooTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from FooStream " +
                "update or insert into FooTable " +
                "   on FooTable.symbol== symbol ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        stockStream.send(new Object[]{"GOOG", 75.6F, 100L});
        fooStream.send(new Object[]{"WSO2", 57.6, 100});
        fooStream.send(new Object[]{"GOOG_2", 10.6, 100});
        Thread.sleep(500);

        siddhiAppRuntime.shutdown();

        long totalDocumentsInCollection = MongoTableTestUtils.getDocumentsCount("FooTable");
        Assert.assertEquals(totalDocumentsInCollection, 3, "Update failed");

        Document expectedUpdatedDocument = new Document()
                .append("symbol", "WSO2")
                .append("price", 57.6)
                .append("volume", 100);
        Document updatedDocument = MongoTableTestUtils.getDocument("FooTable", "{symbol:'WSO2'}");
        Assert.assertEquals(updatedDocument, expectedUpdatedDocument, "Update Failed");
    }

    @Test(expectedExceptions = SiddhiAppValidationException.class)
    public void updateOrInsertMongoTableTest4() {
        log.info("updateOrInsertMongoTableTest4 - DASC5-932:[N] Configure siddhi to perform insert/update with " +
                "a non existing stream");

        MongoTableTestUtils.dropCollection("FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@store(type = 'mongodb' , mongodb.uri='mongodb://admin:admin@127.0.0.1/Foo') " +
                "define table FooTable (symbol string, price float, volume long);";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into FooTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from FooStream123 " +
                "update or insert into FooTable " +
                "   on FooTable.symbol== symbol ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppValidationException.class)
    public void updateOrInsertMongoTableTest5() {
        log.info("updateOrInsertMongoTableTest5 - DASC5-933:[N] Configure siddhi to perform insert/update with an " +
                "undefined MongoDB Document");

        MongoTableTestUtils.dropCollection("FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@store(type = 'mongodb' , mongodb.uri='mongodb://admin:admin@127.0.0.1/Foo') " +
                "define table FooTable (symbol string, price float, volume long);";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into FooTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from FooStream " +
                "update or insert into FooTable123 " +
                "   on FooTable.symbol== symbol ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppValidationException.class)
    public void updateOrInsertMongoTableTest6() {
        log.info("updateOrInsertMongoTableTest6 - DASC5-934:[N] Configure siddhi to perform insert/update on " +
                "MongoDB Document with a non-existing attribute");

        MongoTableTestUtils.dropCollection("FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@store(type = 'mongodb' , mongodb.uri='mongodb://admin:admin@127.0.0.1/Foo') " +
                "define table FooTable (symbol string, price float, volume long);";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into FooTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from FooStream " +
                "update or insert into FooTable " +
                "   on FooTable.symbol123 == symbol ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppValidationException.class)
    public void updateOrInsertMongoTableTest7() {
        log.info("updateOrInsertMongoTableTest7 - DASC5-935:[N] Configure siddhi to perform insert/update on " +
                "MongoDB Document incorrect siddhi query");

        MongoTableTestUtils.dropCollection("FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@store(type = 'mongodb' , mongodb.uri='mongodb://admin:admin@127.0.0.1/Foo') " +
                "define table FooTable (symbol string, price float, volume long);";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into FooTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from FooTable " +
                "update or insert into FooStream " +
                "   on FooTable.symbol == symbol ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }
}
