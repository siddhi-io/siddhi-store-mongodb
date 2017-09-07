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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.exception.SiddhiAppCreationException;
import org.wso2.siddhi.core.stream.input.InputHandler;


public class DeleteFromMongoTableTest {

    private static final Log log = LogFactory.getLog(DeleteFromMongoTableTest.class);

    private static String uri = MongoTableTestUtils.resolveBaseUri();

    @BeforeClass
    public void init() {
        log.info("== MongoDB Collection DELETE tests started ==");
    }

    @AfterClass
    public void shutdown() {
        log.info("== MongoDB Collection DELETE tests completed ==");
    }

    @Test
    public void deleteFromMongoTableTest1() throws InterruptedException {
        log.info("deleteFromMongoTableTest1 - " +
                "DASC5-903:Delete an event of a MongoDB table successfully");

        MongoTableTestUtils.dropCollection(uri, "FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream DeleteStockStream (symbol string, price float, volume long); " +
                "@store(type = 'mongodb' , mongodb.uri='" + uri + "')" +
                "@PrimaryKey('symbol')" +
                "define table FooTable (symbol string, price float, volume long);";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into FooTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from DeleteStockStream " +
                "delete FooTable " +
                "   on (FooTable.symbol == symbol) ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler deleteStockStream = siddhiAppRuntime.getInputHandler("DeleteStockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        stockStream.send(new Object[]{"IBM", 75.6F, 100L});
        stockStream.send(new Object[]{"WSO52", 57.6F, 100L});
        deleteStockStream.send(new Object[]{"IBM", 75.6F, 100L});
        deleteStockStream.send(new Object[]{"WSO2", 55.6F, 100L});

        siddhiAppRuntime.shutdown();

        long totalDocumentsInCollection = MongoTableTestUtils.getDocumentsCount(uri, "FooTable");
        Assert.assertEquals(totalDocumentsInCollection, 1, "Deletion failed");
    }


    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void deleteFromMongoTableTest2() {
        log.info("deleteFromMongoTableTest2 - " +
                "DASC5-904:Delete an event from a non existing MongoDB table");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream DeleteStockStream (symbol string, price float, volume long); " +
                "@store(type = 'mongodb' , mongodb.uri='" + uri + "')" +
                "define table FooTable (symbol string, price float, volume long);";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into FooTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from DeleteStockStream " +
                "delete FooTable1234 " +
                "on FooTable.symbol == symbol;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }


    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void deleteFromMongoTableTest3() {
        log.info("deleteFromMongoTableTest3 - " +
                "DASC5-905:Delete an event from a MongoDB table by selecting from non existing stream");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream DeleteStockStream (symbol string, price float, volume long); " +
                "@store(type = 'mongodb' , mongodb.uri='" + uri + "')" +
                "define table FooTable (symbol string, price float, volume long);";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into FooTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from DeleteStockStream345 " +
                "delete FooTable " +
                "on FooTable.symbol == symbol;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void deleteFromMongoTableTest4() {
        log.info("deleteFromMongoTableTest4 - " +
                "DASC5-906:Delete an event from a MongoDB table based on a non-existing attribute");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream DeleteStockStream (symbol string, price float, volume long); " +
                "@store(type = 'mongodb' , mongodb.uri='" + uri + "')" +
                "define table FooTable (symbol string, price float, volume long);";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into FooTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from DeleteStockStream " +
                "delete FooTable " +
                "   on (FooTable.length == length) ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void deleteFromMongoTableTest5() throws InterruptedException {
        log.info("deleteFromMongoTableTest5 - " +
                "DASC5-909:Delete an event from a MongoDB table based on a non-existing attribute value");

        MongoTableTestUtils.dropCollection(uri, "FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream DeleteStockStream (symbol string, price float, volume long); " +
                "@store(type = 'mongodb' , mongodb.uri='" + uri + "')" +
                "define table FooTable (symbol string, price float, volume long);";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into FooTable;" +
                "" +
                "@info(name = 'query2') " +
                "from DeleteStockStream " +
                "delete FooTable " +
                "   on (FooTable.symbol == symbol) ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler deleteStockStream = siddhiAppRuntime.getInputHandler("DeleteStockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        stockStream.send(new Object[]{"IBM", 75.6F, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6F, 100L});
        deleteStockStream.send(new Object[]{"IBM_v2", 75.6F, 100L});
        deleteStockStream.send(new Object[]{"WSO2_v2", 55.6F, 100L});

        siddhiAppRuntime.shutdown();

        long totalDocumentsInCollection = MongoTableTestUtils.getDocumentsCount(uri, "FooTable");
        Assert.assertEquals(totalDocumentsInCollection, 3, "Deletion failed");
    }
}
