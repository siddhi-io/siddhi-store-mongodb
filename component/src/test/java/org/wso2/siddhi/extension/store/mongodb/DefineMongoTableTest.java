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
import org.bson.Document;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.extension.store.mongodb.exception.MongoTableException;


public class DefineMongoTableTest {
    private static final Logger log = Logger.getLogger(DefineMongoTableTest.class);

    @BeforeClass
    public void init() {
        log.info("== Mongo Table DEFINITION tests started ==");
    }

    @AfterClass
    public void shutdown() {
        log.info("== Mongo Table DEFINITION tests completed ==");
    }

    @Test
    public void mongoTableDefinitionTest1() throws InterruptedException {
        log.info("mongoTableDefinitionTest1 - DASC5-854:Defining a MongoDB event table successfully");
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
            MongoTableTestUtils.clearCollection();
            String streams = "" +
                    "@Store(type = 'mongodb' , mongodb.uri='mongodb://admin:admin@127.0.0.1/Foo') " +
                    "define table FooTable (symbol string, price float, volume long); ";

            ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams);
            executionPlanRuntime.start();
            executionPlanRuntime.shutdown();

            boolean doesCollectionExists = MongoTableTestUtils.doesCollectionExists();
            Assert.assertEquals(doesCollectionExists, true, "Definition failed");

        } catch (MongoException e) {
            log.info("Test case 'mongoTableDefinitionTest1' ignored due to " + e.getMessage());
            throw e;
        }
    }

    @Test
    public void mongoTableDefinitionTest2() throws InterruptedException {
        log.info("mongoTableDefinitionTest2 - DASC5-856:Defining a MongoDB event table with a Primary Key field");
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
            //  MongoTableTestUtils.clearCollection();

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
            stockStream.send(new Object[]{"MSFT", 57.6F, 100L});
            Thread.sleep(1000);

            executionPlanRuntime.shutdown();


            boolean doesCollectionExists = MongoTableTestUtils.doesCollectionExists();
            Assert.assertEquals(doesCollectionExists, true, "Definition failed");

        } catch (MongoException e) {
            log.info("Test case 'mongoTableDefinitionTest2' ignored due to " + e.getMessage());
            throw e;
        }
    }

    @Test(expectedExceptions = MongoTableException.class)
    public void mongoTableDefinitionTest3() throws InterruptedException {
        log.info("mongoTableDefinitionTest3 - " +
                "DASC5-857:Defining a MongoDB table without defining a value for Primary Key field");
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
            MongoTableTestUtils.clearCollection();

            String streams = "" +
                    "@Store(type = 'mongodb' , mongodb.uri='mongodb://admin:admin@127.0.0.1/Foo') " +
                    "@PrimaryKey('')" +
                    "define table FooTable (symbol string, price float, volume long); ";

            ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams);
            executionPlanRuntime.start();
            executionPlanRuntime.shutdown();

        } catch (MongoException e) {
            log.info("Test case 'mongoTableDefinitionTest3' ignored due to " + e.getMessage());
            throw e;
        }
    }

    @Test(expectedExceptions = MongoTableException.class)
    public void mongoTableDefinitionTest4() throws InterruptedException {
        log.info("mongoTableDefinitionTest4 - " +
                "DASC5-858:Defining a MongoDB table with an invalid value for Primary Key field");
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
            MongoTableTestUtils.clearCollection();

            String streams = "" +
                    "@Store(type = 'mongodb' , mongodb.uri='mongodb://admin:admin@127.0.0.1/Foo') " +
                    "@PrimaryKey('symbol234')" +
                    "define table FooTable (symbol string, price float, volume long); ";

            ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams);
            executionPlanRuntime.start();
            executionPlanRuntime.shutdown();

        } catch (MongoException e) {
            log.info("Test case 'mongoTableDefinitionTest4' ignored due to " + e.getMessage());
            throw e;
        }
    }

    @Test(expectedExceptions = MongoTableException.class)
    public void mongoTableDefinitionTest5() throws InterruptedException {
        log.info("mongoTableDefinitionTest5 - " +
                "DASC5-859:Defining a MongoDB table without having a mongodb uri field");
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
            MongoTableTestUtils.clearCollection();

            String streams = "" +
                    "@Store(type = 'mongodb') " +
                    "@PrimaryKey('symbol')" +
                    "define table FooTable (symbol string, price float, volume long); ";

            ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams);
            executionPlanRuntime.start();
            executionPlanRuntime.shutdown();

        } catch (MongoException e) {
            log.info("Test case 'mongoTableDefinitionTest5' ignored due to " + e.getMessage());
            throw e;
        }
    }

    @Test(expectedExceptions = MongoTableException.class)
    public void mongoTableDefinitionTest6() throws InterruptedException {
        log.info("mongoTableDefinitionTest6 - " +
                "DASC5-860:Defining a MongoDB table without defining a value for mongodb uri field");
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
            MongoTableTestUtils.clearCollection();

            String streams = "" +
                    "@Store(type = 'mongodb', mongodb.uri='') " +
                    "@PrimaryKey('symbol')" +
                    "define table FooTable (symbol string, price float, volume long); ";

            ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams);
            executionPlanRuntime.start();
            executionPlanRuntime.shutdown();
        } catch (MongoException e) {
            log.info("Test case 'mongoTableDefinitionTest6' ignored due to " + e.getMessage());
            throw e;
        }
    }


    @Test(expectedExceptions = MongoTableException.class)
    public void mongoTableDefinitionTest7() throws InterruptedException {
        log.info("mongoTableDefinitionTest7 - " +
                "DASC5-861:Defining a MongoDBS table with an invalid value for mongodb uri field");
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
            MongoTableTestUtils.clearCollection();

            String streams = "" +
                    "@Store(type = 'mongodb', mongodb.uri='1234444mongodb://admin:admin@localhost:27017/Foo') " +
                    "@PrimaryKey('symbol')" +
                    "define table FooTable (symbol string, price float, volume long); ";

            ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams);
            executionPlanRuntime.start();
            executionPlanRuntime.shutdown();
        } catch (MongoException e) {
            log.info("Test case 'mongoTableDefinitionTest7' ignored due to " + e.getMessage());
            throw e;
        }
    }

    @Test
    public void mongoTableDefinitionTest8() throws InterruptedException {
        log.info("mongoTableDefinitionTest8 - " +
                "DASC5-862:Defining a MongoDB table with a single option defined in mongodburl");
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
            MongoTableTestUtils.clearCollection();

            String streams = "" +
                    "@Store(type = 'mongodb', " +
                    "mongodb.uri='mongodb://admin:admin@localhost:27017/Foo?maxPoolSize=100')" +
                    "@PrimaryKey('symbol')" +
                    "define table FooTable (symbol string, price float, volume long); ";

            ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams);
            executionPlanRuntime.start();
            executionPlanRuntime.shutdown();

            boolean doesCollectionExists = MongoTableTestUtils.doesCollectionExists();
            Assert.assertEquals(doesCollectionExists, true, "Definition failed");

        } catch (MongoException e) {
            log.info("Test case 'mongoTableDefinitionTest8' ignored due to " + e.getMessage());
            throw e;
        }
    }


    @Test
    public void mongoTableDefinitionTest9() throws InterruptedException {
        log.info("mongoTableDefinitionTest9 - " +
                "DASC5-863:Defining a MongoDB table with multiple options defined in mongodburl");
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
            MongoTableTestUtils.clearCollection();

            String streams = "" +
                    "@Store(type = 'mongodb', " +
                    "mongodb.uri='mongodb://admin:admin@localhost/Foo?maxPoolSize=100&connectTimeoutMS=10000')" +
                    "@PrimaryKey('symbol')" +
                    "define table FooTable (symbol string, price float, volume long); ";

            ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams);
            executionPlanRuntime.start();
            executionPlanRuntime.shutdown();

            boolean doesCollectionExists = MongoTableTestUtils.doesCollectionExists();
            Assert.assertEquals(doesCollectionExists, true, "Definition failed");

        } catch (MongoException e) {
            log.info("Test case 'mongoTableDefinitionTest9' ignored due to " + e.getMessage());
            throw e;
        }
    }

    @Test
    public void mongoTableDefinitionTest10() throws InterruptedException {
        log.info("mongoTableDefinitionTest10 - " +
                "DASC5-864:Defining a MongoDB table with an invalid option defined in mongodburl");
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
            MongoTableTestUtils.clearCollection();

            String streams = "" +
                    "@Store(type = 'mongodb', " +
                    "mongodb.uri='mongodb://admin:admin@localhost:27017/Foo?wso2ssl=true')" +
                    "@PrimaryKey('symbol')" +
                    "define table FooTable (symbol string, price float, volume long); ";

            ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams);
            executionPlanRuntime.start();
            executionPlanRuntime.shutdown();

            boolean doesCollectionExists = MongoTableTestUtils.doesCollectionExists();
            Assert.assertEquals(doesCollectionExists, true, "Definition failed");

        } catch (MongoException e) {
            log.info("Test case 'mongoTableDefinitionTest10' ignored due to " + e.getMessage());
            throw e;
        }
    }

    @Test(expectedExceptions = MongoTableException.class)
    public void mongoTableDefinitionTest11() throws InterruptedException {
        log.info("mongoTableDefinitionTest11 - " +
                "DASC5-865:Defining a MongoDB table with an invalid value for an option defined in mongodburl");
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
            MongoTableTestUtils.clearCollection();

            String streams = "" +
                    "@Store(type = 'mongodb', " +
                    "mongodb.uri='mongodb://admin:admin@localhost:27017/Foo?w=majority5')" +
                    "@PrimaryKey('symbol')" +
                    "define table FooTable (symbol string, price float, volume long); ";

            ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams);
            executionPlanRuntime.start();
            executionPlanRuntime.shutdown();
        } catch (MongoException e) {
            log.info("Test case 'mongoTableDefinitionTest11' ignored due to " + e.getMessage());
            throw e;
        }
    }


    @Test(expectedExceptions = MongoTableException.class)
    public void mongoTableDefinitionTest12() throws InterruptedException {
        log.info("mongoTableDefinitionTest12 - " +
                "DASC5-866:Defining a MongoDB table without a value for an option defined in mongodburl");
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
            MongoTableTestUtils.clearCollection();

            String streams = "" +
                    "@Store(type = 'mongodb', " +
                    "mongodb.uri='mongodb://admin:admin@localhost:27017/Foo?maxPoolSize=')" +
                    "@PrimaryKey('symbol')" +
                    "define table FooTable (symbol string, price float, volume long); ";

            ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams);
            executionPlanRuntime.start();
            executionPlanRuntime.shutdown();

        } catch (MongoException e) {
            log.info("Test case 'mongoTableDefinitionTest12' ignored due to " + e.getMessage());
            throw e;
        }
    }

    @Test
    public void mongoTableDefinitionTest13() throws InterruptedException {
        log.info("mongoTableDefinitionTest13 - " +
                "DASC5-867:Defining a MongoDB table with contradictory values for the same option defined in " +
                "mongodburl");
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
            MongoTableTestUtils.clearCollection();
            String streams = "" +
                    "@Store(type = 'mongodb' , " +
                    "mongodb.uri='mongodb://admin:admin@127.0.0.1/Foo?maxPoolSize=5&maxPoolSize=100') " +
                    "define table FooTable (symbol string, price float, volume long); ";

            ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams);
            executionPlanRuntime.start();
            executionPlanRuntime.shutdown();

            boolean doesCollectionExists = MongoTableTestUtils.doesCollectionExists();
            Assert.assertEquals(doesCollectionExists, true, "Definition failed");

        } catch (MongoException e) {
            log.info("Test case 'mongoTableDefinitionTest1' ignored due to " + e.getMessage());
            throw e;
        }
    }

    @Test
    public void mongoTableDefinitionTest14() throws InterruptedException {
        log.info("mongoTableDefinitionTest14 - " +
                "DASC5-868:Defining a MongoDB event table with IndexBy field");
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
            MongoTableTestUtils.clearCollection();

            String streams = "" +
                    "@Store(type = 'mongodb', " +
                    "mongodb.uri='mongodb://admin:admin@localhost/Foo?maxPoolSize=100&connectTimeoutMS=10000')" +
                    "@IndexBy(\"price 1 {background:true}\", \"symbol -1 {unique:true}\")" +
                    "define table FooTable (symbol string, price float, volume long); ";

            ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams);
            executionPlanRuntime.start();
            executionPlanRuntime.shutdown();

            boolean doesCollectionExists = MongoTableTestUtils.doesCollectionExists();
            Assert.assertEquals(doesCollectionExists, true, "Definition failed");

            Document symbolIndex = MongoTableTestUtils.getIndexList().get(2);
            Document priceIndex = MongoTableTestUtils.getIndexList().get(1);

            //Assert Index Order and properties - price
            Assert.assertEquals(priceIndex.get("key"), new Document("price", 1), "Index Creation Failed");
            Assert.assertEquals(priceIndex.get("background"), true, "Index Creation Failed");

            //Assert Index Order and properties - symbol
            Assert.assertEquals(symbolIndex.get("key"), new Document("symbol", -1), "Index Creation Failed");
            Assert.assertEquals(symbolIndex.get("unique"), true, "Index Creation Failed");

            Thread.sleep(1000);

        } catch (MongoException e) {
            log.info("Test case 'mongoTableDefinitionTest14' ignored due to " + e.getMessage());
            throw e;
        }
    }

    @Test(expectedExceptions = MongoTableException.class)
    public void mongoTableDefinitionTest15() throws InterruptedException {
        log.info("mongoTableDefinitionTest15 - " +
                "DASC5-869:Defining a MongoDB table without defining a value for indexing column within IndexBy field");
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
            MongoTableTestUtils.clearCollection();

            String streams = "" +
                    "@Store(type = 'mongodb', " +
                    "mongodb.uri='mongodb://admin:admin@localhost:27017/Foo?maxPoolSize=')" +
                    "@IndexBy(\"1 {background:true}\")" +
                    "define table FooTable (symbol string, price float, volume long); ";

            ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams);
            executionPlanRuntime.start();
            executionPlanRuntime.shutdown();

        } catch (MongoException e) {
            log.info("Test case 'mongoTableDefinitionTest15' ignored due to " + e.getMessage());
            throw e;
        }
    }

    @Test(expectedExceptions = MongoTableException.class)
    public void mongoTableDefinitionTest16() throws InterruptedException {
        log.info("mongoTableDefinitionTest16 - " +
                "DASC5-870:Defining a MongoDB table with an invalid value for indexing column within IndexBy field");
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
            MongoTableTestUtils.clearCollection();

            String streams = "" +
                    "@Store(type = 'mongodb', mongodb.uri='mongodb://admin:admin@localhost:27017/Foo')" +
                    "@IndexBy(\"symbol1234 1 {unique:true}\")" +
                    "define table FooTable (symbol string, price float, volume long); ";

            ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams);
            executionPlanRuntime.start();
            executionPlanRuntime.shutdown();

        } catch (MongoException e) {
            log.info("Test case 'mongoTableDefinitionTest16' ignored due to " + e.getMessage());
            throw e;
        }
    }

    @Test(expectedExceptions = MongoTableException.class)
    public void mongoTableDefinitionTest17() throws InterruptedException {
        log.info("mongoTableDefinitionTest17 - " +
                "DASC5-872:Defining a MongoDB table without defining a value for sorting within IndexBy field");
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
            MongoTableTestUtils.clearCollection();

            String streams = "" +
                    "@Store(type = 'mongodb', mongodb.uri='mongodb://admin:admin@localhost:27017/Foo')" +
                    "@IndexBy(\"symbol {unique:true}\")" +
                    "define table FooTable (symbol string, price float, volume long); ";

            ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams);
            executionPlanRuntime.start();
            executionPlanRuntime.shutdown();

        } catch (MongoException e) {
            log.info("Test case 'mongoTableDefinitionTest17' ignored due to " + e.getMessage());
            throw e;
        }
    }

    @Test
    public void mongoTableDefinitionTest18() throws InterruptedException {
        log.info("mongoTableDefinitionTest18 - " +
                "DASC5-873:Defining a MongoDB table without defining options within IndexBy field");
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
            //   MongoTableTestUtils.clearCollection();

            String streams = "" +
                    "@Store(type = 'mongodb', mongodb.uri='mongodb://admin:admin@localhost:27017/Foo')" +
                    "@IndexBy(\"symbol 1 {}\")" +
                    "define table FooTable (symbol string, price float, volume long); ";

            ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams);
            executionPlanRuntime.start();
            executionPlanRuntime.shutdown();

            boolean doesCollectionExists = MongoTableTestUtils.doesCollectionExists();
            Assert.assertEquals(doesCollectionExists, true, "Definition failed");

            //Assert Index Order and properties - price
            Assert.assertEquals(doesCollectionExists, true, "Definition failed");

        } catch (MongoException e) {
            log.info("Test case 'mongoTableDefinitionTest18' ignored due to " + e.getMessage());
            throw e;
        }
    }

    @Test(expectedExceptions = MongoTableException.class)
    public void mongoTableDefinitionTest19() throws InterruptedException {
        log.info("mongoTableDefinitionTest19 - " +
                "DASC5-874:Defining a MongoDB table by defining non existing options within IndexBy field");
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
            MongoTableTestUtils.clearCollection();

            String streams = "" +
                    "@Store(type = 'mongodb', mongodb.uri='mongodb://admin:admin@localhost:27017/Foo')" +
                    "@IndexBy(\"symbol 1 {unique:222true}\")" +
                    "define table FooTable (symbol string, price float, volume long); ";

            ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams);
            executionPlanRuntime.start();
            executionPlanRuntime.shutdown();

            boolean doesCollectionExists = MongoTableTestUtils.doesCollectionExists();
            Assert.assertEquals(doesCollectionExists, true, "Definition failed");

        } catch (MongoException e) {
            log.info("Test case 'mongoTableDefinitionTest19' ignored due to " + e.getMessage());
            throw e;
        }
    }

    @Test(expectedExceptions = MongoTableException.class)
    public void mongoTableDefinitionTest20() throws InterruptedException {
        log.info("mongoTableDefinitionTest20 - " +
                "DASC5-875:Defining a MongoDB table by defining an option with an invalid value within IndexBy field");
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
            MongoTableTestUtils.clearCollection();

            String streams = "" +
                    "@Store(type = 'mongodb', mongodb.uri='mongodb://admin:admin@localhost:27017/Foo')" +
                    "@IndexBy(\"symbol 1 {max:'has'}\")" +
                    "define table FooTable (symbol string, price float, volume long); ";

            ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams);
            executionPlanRuntime.start();
            executionPlanRuntime.shutdown();

        } catch (MongoException e) {
            log.info("Test case 'mongoTableDefinitionTest20' ignored due to " + e.getMessage());
            throw e;
        }
    }

    @Test
    public void mongoTableDefinitionTest21() throws InterruptedException {
        log.info("mongoTableDefinitionTest21 - " +
                "DASC5-876:Defining a MongoDB table by having contradictory values for an option " +
                "defined within IndexBy field");
        SiddhiManager siddhiManager = new SiddhiManager();
        try {
            MongoTableTestUtils.clearCollection();

            String streams = "" +
                    "@Store(type = 'mongodb', mongodb.uri='mongodb://admin:admin@localhost:27017/Foo')" +
                    "@IndexBy(\"symbol 1 {unique:false, unique: true}\")" +
                    "define table FooTable (symbol string, price float, volume long); ";

            ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams);
            executionPlanRuntime.start();
            executionPlanRuntime.shutdown();


            boolean doesCollectionExists = MongoTableTestUtils.doesCollectionExists();
            Assert.assertEquals(doesCollectionExists, true, "Definition failed");

            Document symbolIndex = MongoTableTestUtils.getIndexList().get(1);

            //Assert Index Order and properties - price
            Assert.assertEquals(symbolIndex.get("key"), new Document("symbol", 1), "Index Creation Failed");
            Assert.assertEquals(symbolIndex.get("unique"), true, "Index Creation Failed");


        } catch (MongoException e) {
            log.info("Test case 'mongoTableDefinitionTest21' ignored due to " + e.getMessage());
            throw e;
        }
    }

    @Test(enabled = false)
    public void mongoTableDefinitionTest22() throws InterruptedException {
        log.info("mongoTableDefinitionTest22 - " +
                "DASC5-959:Defining a MongoDB event table with an existing collection with different attributes");
        boolean doesCollectionExists;
        doesCollectionExists = MongoTableTestUtils.doesCollectionExists();
        Assert.assertEquals(doesCollectionExists, true, "Foo collection not present before test");

        SiddhiManager siddhiManager = new SiddhiManager();
        try {
            String streams = "" +
                    "@Store(type=\"mongodb\", mongodb.uri=\"mongodb://admin:admin@localhost:27017/Foo\")" +
                    "define table FooTable (symbol string, price float, volume long);";

            ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(streams);
            executionPlanRuntime.start();
            executionPlanRuntime.shutdown();

            doesCollectionExists = MongoTableTestUtils.doesCollectionExists();
            Assert.assertEquals(doesCollectionExists, true, "Definition failed");

        } catch (MongoException e) {
            log.info("Test case 'mongoTableDefinitionTest22' ignored due to " + e.getMessage());
            throw e;
        }
    }

}
