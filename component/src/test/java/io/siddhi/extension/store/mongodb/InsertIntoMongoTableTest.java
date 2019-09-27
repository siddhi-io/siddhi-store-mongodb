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
package io.siddhi.extension.store.mongodb;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.query.api.exception.DuplicateDefinitionException;
import io.siddhi.query.api.exception.SiddhiAppValidationException;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.HashMap;

public class InsertIntoMongoTableTest {

    private static final Logger log = Logger.getLogger(InsertIntoMongoTableTest.class);

    private static String uri = MongoTableTestUtils.resolveBaseUri();

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

        MongoTableTestUtils.dropCollection(uri, "FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@source(type='inMemory', topic='stock') " +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@Store(type=\"mongodb\", mongodb.uri='" + uri + "')" +
                "@PrimaryKey(\"symbol\")" +
                "define table FooTable (symbol string, price float, volume long);";
        String query = "" +
                "@info(name = 'query1') " +
                "from FooStream " +
                "select symbol, price, volume " +
                "insert into FooTable;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();

        fooStream.send(new Object[]{"WSO2", 55.6f, 100L});

        siddhiAppRuntime.shutdown();

        long totalDocumentsInCollection = MongoTableTestUtils.getDocumentsCount(uri, "FooTable");
        Assert.assertEquals(totalDocumentsInCollection, 1, "Insertion failed");

    }

    @Test(expectedExceptions = DuplicateDefinitionException.class)
    public void insertIntoMongoTableTest2() {
        log.info("insertIntoMongoTableTest2 - " +
                "DASC5-878:Insert events to a MongoDB table when query has less attributes to select from");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@source(type='inMemory', topic='stock') " +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@Store(type=\"mongodb\", mongodb.uri='" + uri + "')" +
                "@PrimaryKey(\"symbol\")" +
                "define table FooTable (symbol string, price float, volume long);";
        String query = "" +
                "@info(name = 'query1') " +
                "from FooStream " +
                "select symbol, volume " +
                "insert into FooTable;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppValidationException.class)
    public void insertIntoMongoTableTest3() {
        log.info("insertIntoMongoTableTest3 - " +
                "DASC5-879:[N] Insert events to a MongoDB table when query has more attributes to select from");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@source(type='inMemory', topic='stock') " +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@Store(type=\"mongodb\", mongodb.uri='" + uri + "')" +
                "@PrimaryKey(\"symbol\")" +
                "define table FooTable (symbol string, price float, volume long);";
        String query = "" +
                "@info(name = 'query1') " +
                "from FooStream " +
                "select symbol, price, length, volume " +
                "insert into FooTable;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }


    @Test
    public void insertIntoMongoTableTest4() throws InterruptedException {
        log.info("insertIntoMongoTableTest4 - " +
                "DASC5-880:[N] Insert events to a non existing MongoDB table");

        MongoTableTestUtils.dropCollection(uri, "FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@source(type='inMemory', topic='stock') " +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@Store(type=\"mongodb\", mongodb.uri='" + uri + "')" +
                "@PrimaryKey(\"symbol\")" +
                "define table FooTable (symbol string, price float, volume long);";
        String query = "" +
                "@info(name = 'query1') " +
                "from FooStream " +
                "select symbol, price, volume " +
                "insert into FooTable144;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();

        fooStream.send(new Object[]{"WSO2", 55.6f, 100L});

        siddhiAppRuntime.shutdown();

        long totalDocumentsInCollection = MongoTableTestUtils.getDocumentsCount(uri, "FooTable");
        Assert.assertEquals(totalDocumentsInCollection, 0, "Insertion failed");
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void insertIntoMongoTableTest5() {
        log.info("insertIntoMongoTableTest5 - " +
                "DASC5-883:[N] Insert events to a MongoDB table by selecting from non existing stream");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@source(type='inMemory', topic='stock') " +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@Store(type=\"mongodb\", mongodb.uri='" + uri + "')" +
                "@PrimaryKey(\"symbol\")" +
                "define table FooTable (symbol string, price float, volume long);";
        String query = "" +
                "@info(name = 'query1') " +
                "from FooStream123 " +
                "select symbol, price, volume " +
                "insert into FooTable;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void insertIntoMongoTableTest6() {
        log.info("insertIntoMongoTableTest6 - " +
                "DASC5-888:[N] Insert events to a MongoDB table when the stream has not defined");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@source(type='inMemory', topic='stock') " +
                "@Store(type=\"mongodb\", mongodb.uri='" + uri + "')" +
                "@PrimaryKey(\"symbol\")" +
                "define table FooTable (symbol string, price float, volume long);";
        String query = "" +
                "@info(name = 'query1') " +
                "from FooStream " +
                "select symbol, price, volume " +
                "insert into FooTable;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void insertIntoMongoTableTest7() {
        log.info("insertIntoMongoTableTest7 - " +
                "DASC5-889:[N] Insert events data to MongoDB table when the table has not defined");

        MongoTableTestUtils.dropCollection(uri, "FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@source(type='inMemory', topic='stock') " +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@Store(type=\"mongodb\", mongodb.uri='" + uri + "')" +
                "@PrimaryKey(\"symbol\")";
        String query = "" +
                "@info(name = 'query1') " +
                "from FooStream " +
                "select symbol, price, volume " +
                "insert into FooTable;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();

        boolean doesCollectionExists = MongoTableTestUtils.doesCollectionExists(uri, "FooTable");
        Assert.assertEquals(doesCollectionExists, false, "Definition was created");
    }


    @Test
    public void insertIntoMongoTableTest8() throws InterruptedException {
        log.info("insertIntoMongoTableTest8 - " +
                "DASC5-890:Insert events to a MongoDB table when there are multiple primary keys defined");

        MongoTableTestUtils.dropCollection(uri, "FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@source(type='inMemory', topic='stock') " +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@Store(type=\"mongodb\", mongodb.uri='" + uri + "')" +
                "@PrimaryKey(\"symbol\",\"price\")" +
                "define table FooTable (symbol string, price float, volume long);";
        String query = "" +
                "@info(name = 'query1') " +
                "from FooStream " +
                "select symbol, price, volume " +
                "insert into FooTable;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();

        fooStream.send(new Object[]{"WSO2", 55.6f, 100L});

        siddhiAppRuntime.shutdown();

        long totalDocumentsInCollection = MongoTableTestUtils.getDocumentsCount(uri, "FooTable");
        Assert.assertEquals(totalDocumentsInCollection, 1, "Insertion failed");
    }

    @Test
    public void insertIntoMongoTableTest9() throws InterruptedException {
        log.info("insertIntoMongoTableTest9 - " +
                "DASC5-892:Insert an event to a MongoDB table when the same value was " +
                "inserted for a defined primary key");

        MongoTableTestUtils.dropCollection(uri, "FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@source(type='inMemory', topic='stock') " +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@Store(type=\"mongodb\", mongodb.uri='" + uri + "')" +
                "@PrimaryKey(\"symbol\")" +
                "define table FooTable (symbol string, price float, volume long);";
        String query = "" +
                "@info(name = 'query1') " +
                "from FooStream " +
                "select symbol, price, volume " +
                "insert into FooTable;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();

        fooStream.send(new Object[]{"WSO2", 55.6f, 100L});
        fooStream.send(new Object[]{"IBM", 55.6f, 100L});
        fooStream.send(new Object[]{"WSO2", 55.6f, 100L});

        siddhiAppRuntime.shutdown();

        long totalDocumentsInCollection = MongoTableTestUtils.getDocumentsCount(uri, "FooTable");
        Assert.assertEquals(totalDocumentsInCollection, 2, "Insertion failed");
    }

    @Test
    public void insertIntoMongoTableTest10() throws InterruptedException {
        log.info("insertIntoMongoTableTest10 - " +
                "DASC5-967:Unprivileged user attempts to insert events to a MongoDB table successfully");

        Logger siddhiAppLogger = Logger.getLogger(SiddhiAppRuntime.class);
        UnitTestAppender appender = new UnitTestAppender();
        siddhiAppLogger.addAppender(appender);

        String uri = MongoTableTestUtils
                .resolveBaseUri("mongodb://admin121:admin123@{{mongo.servers}}/{{mongo.database}}");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@source(type='inMemory', topic='stock') " +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@Store(type=\"mongodb\", mongodb.uri='" + uri + "')" +
                "@PrimaryKey(\"symbol\")" +
                "define table FooTable (symbol string, price float, volume long);";
        String query = "" +
                "@info(name = 'query1') " +
                "from FooStream " +
                "select symbol, price, volume " +
                "insert into FooTable;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.start();
        Thread.sleep(5000);

        if (appender.getMessages() != null) {
            AssertJUnit.assertTrue(appender.getMessages().contains("Error in retrieving collection names " +
                    "from the database 'admin' : "));
        } else {
            AssertJUnit.fail();
        }
        siddhiAppLogger.removeAppender(appender);

        siddhiAppRuntime.shutdown();
    }

    @Test
    public void insertIntoMongoTableTest11() throws InterruptedException {
        log.info("insertIntoMongoTableTest11 - " +
                "DASC5-969:User attempts to insert events with duplicate values for the Indexing fields " +
                "which are unique");

        MongoTableTestUtils.dropCollection(uri, "FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@source(type='inMemory', topic='stock') " +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@Store(type=\"mongodb\", mongodb.uri='" + uri + "')" +
                "@IndexBy(\"price 1 {unique:true}\")" +
                "define table FooTable (symbol string, price float, volume long);";
        String query = "" +
                "@info(name = 'query1') " +
                "from FooStream " +
                "select symbol, price, volume " +
                "insert into FooTable;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();

        fooStream.send(new Object[]{"WSO2", 55.6f, 100L});
        fooStream.send(new Object[]{"IBM", 55.6f, 100L});

        siddhiAppRuntime.shutdown();

        long totalDocumentsInCollection = MongoTableTestUtils.getDocumentsCount(uri, "FooTable");
        Assert.assertEquals(totalDocumentsInCollection, 1, "Insertion failed");
    }

    @Test
    public void insertIntoMongoTableTest12() throws InterruptedException {
        log.info("insertIntoMongoTableTest12");
        //Object inserts

        MongoTableTestUtils.dropCollection(uri, "FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@source(type='inMemory', topic='stock') " +
                "define stream FooStream (symbol string, price float, input Object); " +
                "@Store(type=\"mongodb\", mongodb.uri='" + uri + "')" +
                "@PrimaryKey(\"symbol\")" +
                "define table FooTable (symbol string, price float, input Object);";
        String query = "" +
                "@info(name = 'query1') " +
                "from FooStream " +
                "select symbol, price, input " +
                "insert into FooTable;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();

        HashMap<String, String> input = new HashMap<>();
        input.put("symbol", "IBM");
        fooStream.send(new Object[]{"WSO2", 55.6f, input});

        siddhiAppRuntime.shutdown();

        long totalDocumentsInCollection = MongoTableTestUtils.getDocumentsCount(uri, "FooTable");
        Assert.assertEquals(totalDocumentsInCollection, 1, "Insertion failed");

    }
}
