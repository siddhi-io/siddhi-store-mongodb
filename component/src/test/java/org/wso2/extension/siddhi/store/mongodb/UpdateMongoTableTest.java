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
    public void updateFromMongoTableTest1() throws InterruptedException {
        log.info("updateFromMongoTableTest1 - DASC5-893:Update events of a MongoDB table successfully");

        MongoTableTestUtils.dropCollection("FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@store(type = 'mongodb' , mongodb.uri='mongodb://admin:admin@127.0.0.1/Foo') " +
                "@PrimaryKey('symbol','price')" +
                "define table FooTable (symbol string, price float, volume long);";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into FooTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from FooStream " +
                "select symbol, price, volume " +
                "update FooTable " +
                "set FooTable.symbol = symbol, FooTable.price = price, FooTable.volume = volume " +
                "on FooTable.symbol == symbol;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 74.6f, 100L});
        stockStream.send(new Object[]{"WSO2_2", 57.6f, 100L});
        fooStream.send(new Object[]{"IBM", 575.6, 500});
        Thread.sleep(1000);

        siddhiAppRuntime.shutdown();

        long totalDocumentsInCollection = MongoTableTestUtils.getDocumentsCount("FooTable");
        Assert.assertEquals(totalDocumentsInCollection, 3, "Update failed");

        Document expectedUpdatedDocument = new Document()
                .append("symbol", "IBM")
                .append("price", 575.6)
                .append("volume", 500);
        Document updatedDocument = MongoTableTestUtils.getDocument("FooTable", "{symbol:'IBM'}");
        Assert.assertEquals(updatedDocument, expectedUpdatedDocument, "Update Failed");
    }

    @Test
    public void updateFromMongoTableTest2() throws InterruptedException {
        log.info("updateFromMongoTableTest2 - DASC5-894:Updates events of a MongoDB table when query has less " +
                "attributes to select from");

        MongoTableTestUtils.dropCollection("FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@store(type = 'mongodb' , mongodb.uri='mongodb://admin:admin@127.0.0.1/Foo') " +
                "@PrimaryKey('symbol','price')" +
                "define table FooTable (symbol string, price float, volume long);";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into FooTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from FooStream " +
                "select symbol, price " +
                "update FooTable " +
                "set FooTable.symbol = symbol, FooTable.price = price, FooTable.volume = volume " +
                "   on FooTable.symbol == 'IBM' ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 74.6f, 100L});
        stockStream.send(new Object[]{"WSO2_2", 57.6f, 100L});
        fooStream.send(new Object[]{"IBM_2", 575.6f, 500L});
        Thread.sleep(1000);

        siddhiAppRuntime.shutdown();

        long totalDocumentsInCollection = MongoTableTestUtils.getDocumentsCount("FooTable");
        Assert.assertEquals(totalDocumentsInCollection, 3, "Update failed");
    }

    @Test(expectedExceptions = SiddhiAppValidationException.class)
    public void updateFromMongoTableTest3() {
        log.info("updateFromMongoTableTest3 - DASC5-895:Update events of a MongoDB table when query has more " +
                "attributes to select from");

        MongoTableTestUtils.dropCollection("FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@store(type = 'mongodb' , mongodb.uri='mongodb://admin:admin@127.0.0.1/Foo') " +
                "@PrimaryKey('symbol','price')" +
                "define table FooTable (symbol string, price float, volume long);";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into FooTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from FooStream " +
                "select symbol, price, volume, length " +
                "update FooTable " +
                "set FooTable.symbol = symbol, FooTable.price = price, FooTable.volume = volume " +
                "on FooTable.symbol == symbol;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppValidationException.class)
    public void updateFromMongoTableTest4() {
        log.info("updateFromMongoTableTest4 - DASC5-896:Updates events of a non existing MongoDB table");

        MongoTableTestUtils.dropCollection("FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@store(type = 'mongodb' , mongodb.uri='mongodb://admin:admin@127.0.0.1/Foo') " +
                "@PrimaryKey('symbol','price')" +
                "define table FooTable (symbol string, price float, volume long);";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into FooTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from FooStream " +
                "select symbol, price, volume " +
                "update FooTable3455 " +
                "set FooTable.symbol = symbol, FooTable.price = price, FooTable.volume = volume " +
                "on FooTable.symbol == symbol;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppValidationException.class)
    public void updateFromMongoTableTest5() {
        log.info("updateFromMongoTableTest5 - DASC5-897:Updates events of a MongoDB table by selecting from non " +
                "existing stream");

        MongoTableTestUtils.dropCollection("FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@store(type = 'mongodb' , mongodb.uri='mongodb://admin:admin@127.0.0.1/Foo') " +
                "@PrimaryKey('symbol','price')" +
                "define table FooTable (symbol string, price float, volume long);";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into FooTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from FooStream123 " +
                "select symbol, price, volume " +
                "update FooTable " +
                "set FooTable.symbol = symbol, FooTable.price = price, FooTable.volume = volume " +
                "on FooTable.symbol == symbol;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppValidationException.class)
    public void updateFromMongoTableTest6() {
        log.info("updateFromMongoTableTest6 - DASC5-899:Updates events of a MongoDB table based on a non-existing " +
                "attribute");

        MongoTableTestUtils.dropCollection("FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@store(type = 'mongodb' , mongodb.uri='mongodb://admin:admin@127.0.0.1/Foo') " +
                "@PrimaryKey('symbol','price')" +
                "define table FooTable (symbol string, price float, volume long);";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into FooTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from FooStream " +
                "select symbol, price, volume " +
                "update FooTable " +
                "set FooTable.symbol = symbol, FooTable.price = price, FooTable.volume = volume " +
                "on FooTable.length == length;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppValidationException.class)
    public void updateFromMongoTableTest7() {
        log.info("updateFromMongoTableTest7 - DASC5-900:Updates events of a MongoDB table for non-existing attributes");

        MongoTableTestUtils.dropCollection("FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@store(type = 'mongodb' , mongodb.uri='mongodb://admin:admin@127.0.0.1/Foo') " +
                "@PrimaryKey('symbol','price')" +
                "define table FooTable (symbol string, price float, volume long);";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into FooTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from FooStream " +
                "select symbol, price, volume " +
                "update FooTable " +
                "set FooTable.length = length, FooTable.age = age, FooTable.time = time " +
                "on FooTable.symbol == symbol;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void updateFromMongoTableTest8() throws InterruptedException {
        log.info("updateFromMongoTableTest8 - DASC5-901:Updates events of a MongoDB table for non-existing " +
                "attribute value");

        MongoTableTestUtils.dropCollection("FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@store(type = 'mongodb' , mongodb.uri='mongodb://admin:admin@127.0.0.1/Foo') " +
                "@PrimaryKey('symbol','price')" +
                "define table FooTable (symbol string, price float, volume long);";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into FooTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from FooStream " +
                "select symbol, price, volume " +
                "update FooTable " +
                "set FooTable.symbol = symbol, FooTable.price = price, FooTable.volume = volume " +
                "on FooTable.symbol == symbol;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 74.6f, 100L});
        stockStream.send(new Object[]{"WSO2_2", 57.6f, 100L});
        fooStream.send(new Object[]{"IBM_2", 575.6, 500});
        Thread.sleep(1000);

        siddhiAppRuntime.shutdown();

        long totalDocumentsInCollection = MongoTableTestUtils.getDocumentsCount("FooTable");
        Assert.assertEquals(totalDocumentsInCollection, 3, "Update failed");
    }
}
