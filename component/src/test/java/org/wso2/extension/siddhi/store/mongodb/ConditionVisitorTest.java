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

import com.mongodb.MongoException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.table.record.ConditionVisitor;
import org.wso2.siddhi.query.api.exception.SiddhiAppValidationException;

public class ConditionVisitorTest {

    private final Log log = LogFactory.getLog(ConditionVisitor.class);

    @Test(expectedExceptions = SiddhiAppValidationException.class)
    public void conditionBuilderTest1() throws InterruptedException, MongoException {
        log.info("conditionBuilderTest1");
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
            MongoTableTestUtils.clearCollection();
            String streams = "" +
                    "define stream StockStream (symbol string, price float, volume long); " +
                    "define stream DeleteStockStream (symbol string, price float, volume long); " +
                    "@Store(type = 'mongodb' , mongodb.uri='mongodb://admin:admin@127.0.0.1/Foo')" +
                    "define table FooTable (symbol string, price float, volume long);";
            String query = "" +
                    "@info(name = 'query1') " +
                    "from StockStream " +
                    "insert into FooTable ;" +
                    "" +
                    "@info(name = 'query2') " +
                    "from DeleteStockStream " +
                    "delete FooTable " +
                    "   on 'IBM' == symbol  ;";

            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
            InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
            InputHandler deleteStockStream = siddhiAppRuntime.getInputHandler("DeleteStockStream");
            siddhiAppRuntime.start();

            stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
            stockStream.send(new Object[]{"IBM", 75.6F, 100L});
            stockStream.send(new Object[]{"WSO2", 57.6F, 100L});
            deleteStockStream.send(new Object[]{"WSO2", 57.6F, 100L});
            Thread.sleep(1000);

            siddhiAppRuntime.shutdown();
        } catch (MongoException e) {
            log.info("Test case 'conditionBuilderTest1' ignored due to " + e.getMessage());
            throw e;
        }

    }

    @Test(expectedExceptions = SiddhiAppValidationException.class)
    public void conditionBuilderTest2() throws InterruptedException, MongoException {
        log.info("conditionBuilderTest2");
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
            MongoTableTestUtils.clearCollection();
            String streams = "" +
                    "define stream StockStream (symbol string, price float, volume long); " +
                    "define stream DeleteStockStream (symbol string, price float, volume long); " +
                    "@Store(type = 'mongodb' , mongodb.uri='mongodb://admin:admin@127.0.0.1/Foo')" +
                    "define table FooTable (symbol string, price float, volume long);";

            String query = "" +
                    "@info(name = 'query1') " +
                    "from StockStream " +
                    "insert into FooTable ;" +
                    "" +
                    "@info(name = 'query2') " +
                    "from DeleteStockStream " +
                    "delete FooTable " +
                    "   on symbol == 'IBM'  ;";

            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
            InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
            InputHandler deleteStockStream = siddhiAppRuntime.getInputHandler("DeleteStockStream");
            siddhiAppRuntime.start();

            stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
            stockStream.send(new Object[]{"IBM", 75.6F, 100L});
            stockStream.send(new Object[]{"WSO2", 57.6F, 100L});
            deleteStockStream.send(new Object[]{"IBM", 57.6F, 100L});
            Thread.sleep(1000);

            siddhiAppRuntime.shutdown();
        } catch (MongoException e) {
            log.info("Test case 'conditionBuilderTest2' ignored due to " + e.getMessage());
            throw e;
        }
    }

    @Test
    public void conditionBuilderTest3() throws InterruptedException, MongoException {
        log.info("conditionBuilderTest3");
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
            MongoTableTestUtils.clearCollection();
            String streams = "" +
                    "define stream StockStream (symbol string, price float, volume long); " +
                    "define stream DeleteStockStream (symbol string, price float, volume long); " +
                    "@Store(type = 'mongodb' , mongodb.uri='mongodb://admin:admin@127.0.0.1/Foo')" +
                    "define table FooTable (symbol string, price float, volume long);";
            String query = "" +
                    "@info(name = 'query1') " +
                    "from StockStream " +
                    "insert into FooTable ;" +
                    "" +
                    "@info(name = 'query2') " +
                    "from DeleteStockStream " +
                    "delete FooTable " +
                    "   on FooTable.symbol==symbol;";

            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
            InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
            InputHandler deleteStockStream = siddhiAppRuntime.getInputHandler("DeleteStockStream");
            siddhiAppRuntime.start();

            stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
            stockStream.send(new Object[]{"IBM", 75.6F, 100L});
            stockStream.send(new Object[]{"IBM", 57.6F, 100L});
            deleteStockStream.send(new Object[]{"IBM", 57.6F, 100L});
            Thread.sleep(1000);

            long totalDocumentsInCollection = MongoTableTestUtils.getDocumentsCount();
            Assert.assertEquals(totalDocumentsInCollection, 1, "Deletion failed");
            siddhiAppRuntime.shutdown();
        } catch (MongoException e) {
            log.info("Test case 'conditionBuilderTest7' ignored due to " + e.getMessage());
            throw e;
        }
    }

    @Test
    public void conditionBuilderTest4() throws InterruptedException, MongoException {
        log.info("conditionBuilderTest4");
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
            MongoTableTestUtils.clearCollection();
            String streams = "" +
                    "define stream StockStream (symbol string, price float, volume long); " +
                    "define stream DeleteStockStream (symbol string, price float, volume long); " +
                    "@Store(type = 'mongodb' , mongodb.uri='mongodb://admin:admin@127.0.0.1/Foo')" +
                    "define table FooTable (symbol string, price float, volume long);";
            String query = "" +
                    "@info(name = 'query1') " +
                    "from StockStream " +
                    "insert into FooTable ;" +
                    "" +
                    "@info(name = 'query2') " +
                    "from DeleteStockStream " +
                    "delete FooTable " +
                    "   on symbol == FooTable.symbol;";

            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
            InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
            InputHandler deleteStockStream = siddhiAppRuntime.getInputHandler("DeleteStockStream");
            siddhiAppRuntime.start();

            stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
            stockStream.send(new Object[]{"IBM", 75.6F, 100L});
            stockStream.send(new Object[]{"IBM", 57.6F, 100L});
            deleteStockStream.send(new Object[]{"IBM", 57.6F, 100L});
            Thread.sleep(1000);

            long totalDocumentsInCollection = MongoTableTestUtils.getDocumentsCount();
            Assert.assertEquals(totalDocumentsInCollection, 1, "Deletion failed");
            siddhiAppRuntime.shutdown();
        } catch (MongoException e) {
            log.info("Test case 'conditionBuilderTest8' ignored due to " + e.getMessage());
            throw e;
        }
    }

    @Test
    public void conditionBuilderTest5() throws InterruptedException, MongoException {
        log.info("conditionBuilderTest5");
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
            MongoTableTestUtils.clearCollection();
            String streams = "" +
                    "define stream StockStream (symbol string, price float, volume long); " +
                    "define stream DeleteStockStream (symbol string, price float, volume long); " +
                    "@Store(type = 'mongodb' , mongodb.uri='mongodb://admin:admin@127.0.0.1/Foo')" +
                    "define table FooTable (symbol string, price float, volume long);";
            String query = "" +
                    "@info(name = 'query1') " +
                    "from StockStream " +
                    "insert into FooTable ;" +
                    "" +
                    "@info(name = 'query2') " +
                    "from DeleteStockStream " +
                    "delete FooTable " +
                    "   on 'IBM' == FooTable.symbol  ;";

            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
            InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
            InputHandler deleteStockStream = siddhiAppRuntime.getInputHandler("DeleteStockStream");
            siddhiAppRuntime.start();

            stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
            stockStream.send(new Object[]{"IBM", 75.6F, 100L});
            stockStream.send(new Object[]{"WSO2", 57.6F, 100L});
            deleteStockStream.send(new Object[]{"WSO2", 57.6F, 100L});
            Thread.sleep(1000);

            long totalDocumentsInCollection = MongoTableTestUtils.getDocumentsCount();
            Assert.assertEquals(totalDocumentsInCollection, 2, "Deletion failed");
            siddhiAppRuntime.shutdown();
        } catch (MongoException e) {
            log.info("Test case 'conditionBuilderTest1' ignored due to " + e.getMessage());
            throw e;
        }

    }

    @Test
    public void conditionBuilderTest6() throws InterruptedException, MongoException {
        log.info("conditionBuilderTest6");
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
            MongoTableTestUtils.clearCollection();
            String streams = "" +
                    "define stream StockStream (symbol string, price float, volume long); " +
                    "define stream DeleteStockStream (symbol string, price float, volume long); " +
                    "@Store(type = 'mongodb' , mongodb.uri='mongodb://admin:admin@127.0.0.1/Foo')" +
                    "define table FooTable (symbol string, price float, volume long);";

            String query = "" +
                    "@info(name = 'query1') " +
                    "from StockStream " +
                    "insert into FooTable ;" +
                    "" +
                    "@info(name = 'query2') " +
                    "from DeleteStockStream " +
                    "delete FooTable " +
                    "   on FooTable.symbol == 'IBM'  ;";

            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
            InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
            InputHandler deleteStockStream = siddhiAppRuntime.getInputHandler("DeleteStockStream");
            siddhiAppRuntime.start();

            stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
            stockStream.send(new Object[]{"IBM", 75.6F, 100L});
            stockStream.send(new Object[]{"WSO2", 57.6F, 100L});
            deleteStockStream.send(new Object[]{"IBM", 57.6F, 100L});
            Thread.sleep(1000);

            long totalDocumentsInCollection = MongoTableTestUtils.getDocumentsCount();
            Assert.assertEquals(totalDocumentsInCollection, 2, "Deletion failed");
            siddhiAppRuntime.shutdown();
        } catch (MongoException e) {
            log.info("Test case 'conditionBuilderTest2' ignored due to " + e.getMessage());
            throw e;
        }
    }

    @Test
    public void conditionBuilderTest7() throws InterruptedException, MongoException {
        log.info("conditionBuilderTest7");
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
            MongoTableTestUtils.clearCollection();
            String streams = "" +
                    "define stream StockStream (symbol string, price float, volume long); " +
                    "define stream DeleteStockStream (symbol string, price float, volume long); " +
                    "@Store(type = 'mongodb' , mongodb.uri='mongodb://admin:admin@127.0.0.1/Foo')" +
                    "define table FooTable (symbol string, price float, volume long);";

            String query = "" +
                    "@info(name = 'query1') " +
                    "from StockStream " +
                    "insert into FooTable ;" +
                    "" +
                    "@info(name = 'query2') " +
                    "from DeleteStockStream " +
                    "delete FooTable " +
                    "   on FooTable.symbol != symbol  ;";

            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
            InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
            InputHandler deleteStockStream = siddhiAppRuntime.getInputHandler("DeleteStockStream");
            siddhiAppRuntime.start();

            stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
            stockStream.send(new Object[]{"IBM", 75.6F, 100L});
            stockStream.send(new Object[]{"WSO2", 57.6F, 100L});
            deleteStockStream.send(new Object[]{"IBM", 57.6F, 100L});
            Thread.sleep(1000);

            long totalDocumentsInCollection = MongoTableTestUtils.getDocumentsCount();
            Assert.assertEquals(totalDocumentsInCollection, 1, "Deletion failed");
            siddhiAppRuntime.shutdown();
        } catch (MongoException e) {
            log.info("Test case 'conditionBuilderTest7' ignored due to " + e.getMessage());
            throw e;
        }
    }


    @Test
    public void conditionBuilderTest8() throws InterruptedException, MongoException {
        log.info("conditionBuilderTest8");
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
            MongoTableTestUtils.clearCollection();
            String streams = "" +
                    "define stream StockStream (symbol string, price float, volume long); " +
                    "define stream DeleteStockStream (symbol string, price float, volume long); " +
                    "@Store(type = 'mongodb' , mongodb.uri='mongodb://admin:admin@127.0.0.1/Foo')" +
                    "define table FooTable (symbol string, price float, volume long);";

            String query = "" +
                    "@info(name = 'query1') " +
                    "from StockStream " +
                    "insert into FooTable ;" +
                    "" +
                    "@info(name = 'query2') " +
                    "from DeleteStockStream " +
                    "delete FooTable " +
                    "   on FooTable.price > price  ;";

            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
            InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
            InputHandler deleteStockStream = siddhiAppRuntime.getInputHandler("DeleteStockStream");
            siddhiAppRuntime.start();

            stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
            stockStream.send(new Object[]{"IBM", 75.6F, 100L});
            stockStream.send(new Object[]{"WSO2", 57.6F, 100L});
            deleteStockStream.send(new Object[]{"IBM", 57.6F, 100L});
            Thread.sleep(1000);

            long totalDocumentsInCollection = MongoTableTestUtils.getDocumentsCount();
            Assert.assertEquals(totalDocumentsInCollection, 2, "Deletion failed");
            siddhiAppRuntime.shutdown();
        } catch (MongoException e) {
            log.info("Test case 'conditionBuilderTest8 ignored due to " + e.getMessage());
            throw e;
        }
    }

    @Test
    public void conditionBuilderTest9() throws InterruptedException, MongoException {
        log.info("conditionBuilderTest9");
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
            MongoTableTestUtils.clearCollection();
            String streams = "" +
                    "define stream StockStream (symbol string, price float, volume long); " +
                    "define stream DeleteStockStream (symbol string, price float, volume long); " +
                    "@Store(type = 'mongodb' , mongodb.uri='mongodb://admin:admin@127.0.0.1/Foo')" +
                    "define table FooTable (symbol string, price float, volume long);";

            String query = "" +
                    "@info(name = 'query1') " +
                    "from StockStream " +
                    "insert into FooTable ;" +
                    "" +
                    "@info(name = 'query2') " +
                    "from DeleteStockStream " +
                    "delete FooTable " +
                    "   on FooTable.price >= 57.6F  ;";

            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
            InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
            InputHandler deleteStockStream = siddhiAppRuntime.getInputHandler("DeleteStockStream");
            siddhiAppRuntime.start();

            stockStream.send(new Object[]{"WSO2", 55.6, 100L});
            stockStream.send(new Object[]{"IBM", 75.6, 100L});
            stockStream.send(new Object[]{"WSO2", 57.6, 100L});
            deleteStockStream.send(new Object[]{"IBM", 57.6, 100L});
            Thread.sleep(1000);

            long totalDocumentsInCollection = MongoTableTestUtils.getDocumentsCount();
            Assert.assertEquals(totalDocumentsInCollection, 1, "Deletion failed");
            siddhiAppRuntime.shutdown();
        } catch (MongoException e) {
            log.info("Test case 'conditionBuilderTest9 ignored due to " + e.getMessage());
            throw e;
        }
    }

    @Test
    public void conditionBuilderTest10() throws InterruptedException, MongoException {
        log.info("conditionBuilderTest10");
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
            MongoTableTestUtils.clearCollection();
            String streams = "" +
                    "define stream StockStream (symbol string, price float, volume long); " +
                    "define stream DeleteStockStream (symbol string, price float, volume long); " +
                    "@Store(type = 'mongodb' , mongodb.uri='mongodb://admin:admin@127.0.0.1/Foo')" +
                    "define table FooTable (symbol string, price float, volume long);";

            String query = "" +
                    "@info(name = 'query1') " +
                    "from StockStream " +
                    "insert into FooTable ;" +
                    "" +
                    "@info(name = 'query2') " +
                    "from DeleteStockStream " +
                    "delete FooTable " +
                    "   on FooTable.price < price  ;";

            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
            InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
            InputHandler deleteStockStream = siddhiAppRuntime.getInputHandler("DeleteStockStream");
            siddhiAppRuntime.start();

            stockStream.send(new Object[]{"WSO2", 55.6, 100L});
            stockStream.send(new Object[]{"IBM", 75.6, 100L});
            stockStream.send(new Object[]{"WSO2", 57.6, 100L});
            deleteStockStream.send(new Object[]{"IBM", 57.6, 100L});
            Thread.sleep(1000);

            long totalDocumentsInCollection = MongoTableTestUtils.getDocumentsCount();
            Assert.assertEquals(totalDocumentsInCollection, 2, "Deletion failed");
            siddhiAppRuntime.shutdown();
        } catch (MongoException e) {
            log.info("Test case 'conditionBuilderTest10 ignored due to " + e.getMessage());
            throw e;
        }
    }

    @Test
    public void conditionBuilderTest11() throws InterruptedException, MongoException {
        log.info("conditionBuilderTest11");
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
            MongoTableTestUtils.clearCollection();
            String streams = "" +
                    "define stream StockStream (symbol string, price float, volume long); " +
                    "define stream DeleteStockStream (symbol string, price float, volume long); " +
                    "@Store(type = 'mongodb' , mongodb.uri='mongodb://admin:admin@127.0.0.1/Foo')" +
                    "define table FooTable (symbol string, price float, volume long);";

            String query = "" +
                    "@info(name = 'query1') " +
                    "from StockStream " +
                    "insert into FooTable ;" +
                    "" +
                    "@info(name = 'query2') " +
                    "from DeleteStockStream " +
                    "delete FooTable " +
                    "   on FooTable.price <= price  ;";

            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
            InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
            InputHandler deleteStockStream = siddhiAppRuntime.getInputHandler("DeleteStockStream");
            siddhiAppRuntime.start();

            stockStream.send(new Object[]{"WSO2", 55.6, 100L});
            stockStream.send(new Object[]{"IBM", 75.6, 100L});
            stockStream.send(new Object[]{"WSO2", 57.6, 100L});
            deleteStockStream.send(new Object[]{"IBM", 57.6, 100L});
            Thread.sleep(1000);

            long totalDocumentsInCollection = MongoTableTestUtils.getDocumentsCount();
            Assert.assertEquals(totalDocumentsInCollection, 1, "Deletion failed");
            siddhiAppRuntime.shutdown();
        } catch (MongoException e) {
            log.info("Test case 'conditionBuilderTest9 ignored due to " + e.getMessage());
            throw e;
        }
    }


    @Test
    public void conditionBuilderTest12() throws InterruptedException, MongoException {
        log.info("conditionBuilderTest12");
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
            MongoTableTestUtils.clearCollection();
            String streams = "" +
                    "define stream StockStream (symbol string, price float, volume long); " +
                    "define stream DeleteStockStream (symbol string, price float, volume long); " +
                    "@Store(type = 'mongodb' , mongodb.uri='mongodb://admin:admin@127.0.0.1/Foo')" +
                    "define table FooTable (symbol string, price float, volume long);";

            String query = "" +
                    "@info(name = 'query1') " +
                    "from StockStream " +
                    "insert into FooTable ;" +
                    "" +
                    "@info(name = 'query2') " +
                    "from DeleteStockStream " +
                    "delete FooTable " +
                    "   on FooTable.symbol == symbol AND FooTable.price <= price  ;";

            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
            InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
            InputHandler deleteStockStream = siddhiAppRuntime.getInputHandler("DeleteStockStream");
            siddhiAppRuntime.start();

            stockStream.send(new Object[]{"WSO2", 55.6, 100L});
            stockStream.send(new Object[]{"IBM", 55.6, 100L});
            stockStream.send(new Object[]{"WSO2", 57.6, 100L});
            deleteStockStream.send(new Object[]{"WSO2", 57.6, 100L});
            Thread.sleep(1000);

            long totalDocumentsInCollection = MongoTableTestUtils.getDocumentsCount();
            Assert.assertEquals(totalDocumentsInCollection, 1, "Deletion failed");
            siddhiAppRuntime.shutdown();
        } catch (MongoException e) {
            log.info("Test case 'conditionBuilderTest12 ignored due to " + e.getMessage());
            throw e;
        }
    }

    @Test
    public void conditionBuilderTest13() throws InterruptedException, MongoException {
        log.info("conditionBuilderTest13");
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
            MongoTableTestUtils.clearCollection();
            String streams = "" +
                    "define stream StockStream (symbol string, price float, volume long); " +
                    "define stream DeleteStockStream (symbol string, price float, volume long); " +
                    "@Store(type = 'mongodb' , mongodb.uri='mongodb://admin:admin@127.0.0.1/Foo')" +
                    "define table FooTable (symbol string, price float, volume long);";

            String query = "" +
                    "@info(name = 'query1') " +
                    "from StockStream " +
                    "insert into FooTable ;" +
                    "" +
                    "@info(name = 'query2') " +
                    "from DeleteStockStream " +
                    "delete FooTable " +
                    "   on FooTable.symbol == symbol OR FooTable.price <= price  ;";

            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
            InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
            InputHandler deleteStockStream = siddhiAppRuntime.getInputHandler("DeleteStockStream");
            siddhiAppRuntime.start();

            stockStream.send(new Object[]{"WSO2", 55.6, 100L});
            stockStream.send(new Object[]{"IBM", 55.6, 100L});
            stockStream.send(new Object[]{"WSO2", 57.6, 100L});
            deleteStockStream.send(new Object[]{"WSO2", 57.6, 100L});
            Thread.sleep(1000);

            long totalDocumentsInCollection = MongoTableTestUtils.getDocumentsCount();
            Assert.assertEquals(totalDocumentsInCollection, 0, "Deletion failed");
            siddhiAppRuntime.shutdown();
        } catch (MongoException e) {
            log.info("Test case 'conditionBuilderTest13 ignored due to " + e.getMessage());
            throw e;
        }
    }

    @Test
    public void conditionBuilderTest14() throws InterruptedException, MongoException {
        log.info("conditionBuilderTest14");
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
            MongoTableTestUtils.clearCollection();
            String streams = "" +
                    "define stream StockStream (symbol string, price float, volume long); " +
                    "define stream DeleteStockStream (symbol string, price float, volume long); " +
                    "@Store(type = 'mongodb' , mongodb.uri='mongodb://admin:admin@127.0.0.1/Foo')" +
                    "define table FooTable (symbol string, price float, volume long);";

            String query = "" +
                    "@info(name = 'query1') " +
                    "from StockStream " +
                    "insert into FooTable ;" +
                    "" +
                    "@info(name = 'query2') " +
                    "from DeleteStockStream " +
                    "delete FooTable " +
                    "   on NOT (FooTable.symbol == symbol);";

            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
            InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
            InputHandler deleteStockStream = siddhiAppRuntime.getInputHandler("DeleteStockStream");
            siddhiAppRuntime.start();

            stockStream.send(new Object[]{"WSO2", 55.6, 100L});
            stockStream.send(new Object[]{"IBM", 55.6, 100L});
            stockStream.send(new Object[]{"WSO2", 57.6, 100L});
            deleteStockStream.send(new Object[]{"WSO2", 57.6, 100L});
            Thread.sleep(1000);

            long totalDocumentsInCollection = MongoTableTestUtils.getDocumentsCount();
            Assert.assertEquals(totalDocumentsInCollection, 2, "Deletion failed");
            siddhiAppRuntime.shutdown();
        } catch (MongoException e) {
            log.info("Test case 'conditionBuilderTest14 ignored due to " + e.getMessage());
            throw e;
        }
    }


    @Test(enabled = false)
    public void conditionBuilderTest15() throws InterruptedException, MongoException {
        log.info("conditionBuilderTest15");
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
            MongoTableTestUtils.clearCollection();
            String streams = "" +
                    "define stream StockStream (symbol string, price float, volume long); " +
                    "define stream DeleteStockStream (symbol string, price float, volume long); " +
                    "@Store(type = 'mongodb' , mongodb.uri='mongodb://admin:admin@127.0.0.1/Foo')" +
                    "define table FooTable (symbol string, price float, volume long);";

            String query = "" +
                    "@info(name = 'query1') " +
                    "from StockStream " +
                    "insert into FooTable ;" +
                    "" +
                    "@info(name = 'query2') " +
                    "from DeleteStockStream " +
                    "delete FooTable " +
                    "   on (FooTable.symbol) in symbol;";

            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
            InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
            InputHandler deleteStockStream = siddhiAppRuntime.getInputHandler("DeleteStockStream");
            siddhiAppRuntime.start();

            stockStream.send(new Object[]{"WSO2", 55.6, 100L});
            stockStream.send(new Object[]{"IBM", 55.6, 100L});
            stockStream.send(new Object[]{"WSO2", 57.6, 100L});
            deleteStockStream.send(new Object[]{"WSO2", 57.6, 100L});
            Thread.sleep(1000);

            long totalDocumentsInCollection = MongoTableTestUtils.getDocumentsCount();
            Assert.assertEquals(totalDocumentsInCollection, 2, "Deletion failed");
            siddhiAppRuntime.shutdown();
        } catch (MongoException e) {
            log.info("Test case 'conditionBuilderTest15 ignored due to " + e.getMessage());
            throw e;
        }
    }

    @Test
    public void conditionBuilderTest16() throws InterruptedException, MongoException {
        log.info("conditionBuilderTest16");
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
            MongoTableTestUtils.clearCollection();
            String streams = "" +
                    "define stream StockStream (symbol string, price float, volume long); " +
                    "define stream DeleteStockStream (symbol string, price float, volume long); " +
                    "@Store(type = 'mongodb' , mongodb.uri='mongodb://admin:admin@127.0.0.1/Foo')" +
                    "define table FooTable (symbol string, price float, volume long);";

            String query = "" +
                    "@info(name = 'query1') " +
                    "from StockStream " +
                    "insert into FooTable ;" +
                    "" +
                    "@info(name = 'query2') " +
                    "from DeleteStockStream " +
                    "delete FooTable " +
                    "   on NOT (FooTable.symbol is NULL);";

            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
            InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
            InputHandler deleteStockStream = siddhiAppRuntime.getInputHandler("DeleteStockStream");
            siddhiAppRuntime.start();

            stockStream.send(new Object[]{"WSO2", 55.6, 100L});
            stockStream.send(new Object[]{"IBM", 55.6, 100L});
            stockStream.send(new Object[]{"WSO2", 57.6, 100L});
            deleteStockStream.send(new Object[]{"WSO2", 57.6, 100L});
            Thread.sleep(1000);

            long totalDocumentsInCollection = MongoTableTestUtils.getDocumentsCount();
            Assert.assertEquals(totalDocumentsInCollection, 0, "Deletion failed");
            siddhiAppRuntime.shutdown();
        } catch (MongoException e) {
            log.info("Test case 'conditionBuilderTest16 ignored due to " + e.getMessage());
            throw e;
        }
    }

    @Test(expectedExceptions = SiddhiAppValidationException.class)
    public void conditionBuilderTest17() throws InterruptedException, MongoException {
        log.info("conditionBuilderTest17");
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
            MongoTableTestUtils.clearCollection();
            String streams = "" +
                    "define stream StockStream (symbol string, price float, volume long); " +
                    "define stream DeleteStockStream (symbol string, price float, volume long); " +
                    "@Store(type = 'mongodb' , mongodb.uri='mongodb://admin:admin@127.0.0.1/Foo')" +
                    "define table FooTable (symbol string, price float, volume long);";

            String query = "" +
                    "@info(name = 'query1') " +
                    "from StockStream " +
                    "insert into FooTable ;" +
                    "" +
                    "@info(name = 'query2') " +
                    "from DeleteStockStream " +
                    "delete FooTable " +
                    "   on FooTable.price + price < 67;";

            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
            siddhiAppRuntime.start();
            siddhiAppRuntime.shutdown();
        } catch (MongoException e) {
            log.info("Test case 'conditionBuilderTest17 ignored due to " + e.getMessage());
            throw e;
        }
    }


    @Test(expectedExceptions = SiddhiAppValidationException.class)
    public void conditionBuilderTest18() throws InterruptedException, MongoException {
        log.info("conditionBuilderTest18");
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
            MongoTableTestUtils.clearCollection();
            String streams = "" +
                    "define stream StockStream (symbol string, price float, volume long); " +
                    "define stream DeleteStockStream (symbol string, price float, volume long); " +
                    "@Store(type = 'mongodb' , mongodb.uri='mongodb://admin:admin@127.0.0.1/Foo')" +
                    "define table FooTable (symbol string, price float, volume long);";

            String query = "" +
                    "@info(name = 'query1') " +
                    "from StockStream " +
                    "insert into FooTable ;" +
                    "" +
                    "@info(name = 'query2') " +
                    "from DeleteStockStream " +
                    "delete FooTable " +
                    "   on DateOf(FooTable.price) < 67;";

            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
            siddhiAppRuntime.start();
            siddhiAppRuntime.shutdown();
        } catch (MongoException e) {
            log.info("Test case 'conditionBuilderTest18 ignored due to " + e.getMessage());
            throw e;
        }
    }
}
