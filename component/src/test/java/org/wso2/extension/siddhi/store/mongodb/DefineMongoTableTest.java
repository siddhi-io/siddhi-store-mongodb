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
import org.wso2.extension.siddhi.store.mongodb.exception.MongoTableException;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;

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
    public void mongoTableDefinitionTest1() {
        log.info("mongoTableDefinitionTest1 - " +
                "DASC5-958:Defining a MongoDB event table with a non existing collection.");

        MongoTableTestUtils.dropCollection("FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@store(type = 'mongodb' , mongodb.uri='mongodb://admin:admin@127.0.0.1/Foo') " +
                "define table FooTable (symbol string, price float, volume long); ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();

        boolean doesCollectionExists = MongoTableTestUtils.doesCollectionExists("FooTable");
        Assert.assertEquals(doesCollectionExists, true, "Definition failed");
    }

    @Test
    public void mongoTableDefinitionTest2() {
        log.info("mongoTableDefinitionTest2 - " +
                "DASC5-854:Defining a MongoDB event table with an existing collection");

        MongoTableTestUtils.createCollection("FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@store(type = 'mongodb' , mongodb.uri='mongodb://admin:admin@127.0.0.1/Foo') " +
                "@PrimaryKey('symbol')" +
                "define table FooTable (symbol string, price float, volume long); ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();

        boolean doesCollectionExists = MongoTableTestUtils.doesCollectionExists("FooTable");
        Assert.assertEquals(doesCollectionExists, true, "Definition failed");

    }

    @Test
    public void mongoTableDefinitionTest3() {
        log.info("mongoTableDefinitionTest3 - DASC5-856:Defining a MongoDB event table with a Primary Key field");

        MongoTableTestUtils.dropCollection("FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@store(type = 'mongodb' , mongodb.uri='mongodb://admin:admin@127.0.0.1/Foo') " +
                "@PrimaryKey('symbol')" +
                "define table FooTable (symbol string, price float, volume long); " +
                "define stream StockStream (symbol string, price float, volume long);";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();

        boolean doesCollectionExists = MongoTableTestUtils.doesCollectionExists("FooTable");
        Assert.assertEquals(doesCollectionExists, true, "Definition failed");

        Document indexExcepted = new org.bson.Document()
                .append("key", new org.bson.Document("symbol", 1))
                .append("name", "symbol_1")
                .append("v", 2)
                .append("unique", true);
        Document indexActual = MongoTableTestUtils.getIndex("FooTable", "symbol_1");
        Assert.assertEquals(indexActual, indexExcepted, "Primary Key Definition Failed");
    }

    @Test(expectedExceptions = MongoTableException.class)
    public void mongoTableDefinitionTest4() {
        log.info("mongoTableDefinitionTest4 - " +
                "DASC5-857:Defining a MongoDB table without defining a value for Primary Key field");

        MongoTableTestUtils.dropCollection("FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@store(type = 'mongodb' , mongodb.uri='mongodb://admin:admin@127.0.0.1/Foo') " +
                "@PrimaryKey('')" +
                "define table FooTable (symbol string, price float, volume long); ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = MongoTableException.class)
    public void mongoTableDefinitionTest5() {
        log.info("mongoTableDefinitionTest5 - " +
                "DASC5-858:Defining a MongoDB table with an invalid value for Primary Key field");

        MongoTableTestUtils.dropCollection("FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@store(type = 'mongodb' , mongodb.uri='mongodb://admin:admin@127.0.0.1/Foo') " +
                "@PrimaryKey('symbol234')" +
                "define table FooTable (symbol string, price float, volume long); ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = MongoTableException.class)
    public void mongoTableDefinitionTest6() {
        log.info("mongoTableDefinitionTest6 - " +
                "DASC5-859:Defining a MongoDB table without having a mongodb uri field");

        MongoTableTestUtils.dropCollection("FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@store(type = 'mongodb') " +
                "@PrimaryKey('symbol')" +
                "define table FooTable (symbol string, price float, volume long); ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = MongoTableException.class)
    public void mongoTableDefinitionTest7() {
        log.info("mongoTableDefinitionTest7 - " +
                "DASC5-860:Defining a MongoDB table without defining a value for mongodb uri field");

        MongoTableTestUtils.dropCollection("FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@store(type = 'mongodb', mongodb.uri='') " +
                "@PrimaryKey('symbol')" +
                "define table FooTable (symbol string, price float, volume long); ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = MongoTableException.class)
    public void mongoTableDefinitionTest8() {
        log.info("mongoTableDefinitionTest8 - " +
                "DASC5-861:Defining a MongoDBS table with an invalid value for mongodb uri field");

        MongoTableTestUtils.dropCollection("FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@store(type = 'mongodb', mongodb.uri='1234444mongodb://admin:admin@localhost:27017/Foo') " +
                "@PrimaryKey('symbol')" +
                "define table FooTable (symbol string, price float, volume long); ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test(enabled = false)
    public void mongoTableDefinitionTest9() {
        log.info("mongoTableDefinitionTest9 - " +
                "   Actions DASC5-862:Defining a MongoDB table with by defining ssl in mongodburl");

        MongoTableTestUtils.dropCollection("FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@store(type = 'mongodb', " +
                "mongodb.uri='mongodb://admin:admin@localhost:27017/Foo?ssl=true')" +
                "@PrimaryKey('symbol')" +
                "define table FooTable (symbol string, price float, volume long); ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();

        boolean doesCollectionExists = MongoTableTestUtils.doesCollectionExists("FooTable");
        Assert.assertEquals(doesCollectionExists, true, "Definition failed");
    }

    @Test(enabled = false)
    public void mongoTableDefinitionTest10() {
        log.info("mongoTableDefinitionTest10 - " +
                "DASC5-863:Defining a MongoDB table with multiple options defined in mongodburl");

        MongoTableTestUtils.dropCollection("FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@store(type = 'mongodb', " +
                "mongodb.uri='mongodb://admin:admin@localhost/Foo?ssl=true&maxPoolSize=100')" +
                "@PrimaryKey('symbol')" +
                "define table FooTable (symbol string, price float, volume long); ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();

        boolean doesCollectionExists = MongoTableTestUtils.doesCollectionExists("FooTable");
        Assert.assertEquals(doesCollectionExists, true, "Definition failed");
    }

    @Test(enabled = false)
    public void mongoTableDefinitionTest11() {
        log.info("mongoTableDefinitionTest11 - " +
                "DASC5-864:Defining a MongoDB table with an invalid option defined in mongodburl");

        MongoTableTestUtils.dropCollection("FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@store(type = 'mongodb', " +
                "mongodb.uri='mongodb://admin:admin@localhost:27017/Foo?wso2ssl=true')" +
                "@PrimaryKey('symbol')" +
                "define table FooTable (symbol string, price float, volume long); ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();

        boolean doesCollectionExists = MongoTableTestUtils.doesCollectionExists("FooTable");
        Assert.assertEquals(doesCollectionExists, true, "Definition failed");
    }

    @Test(enabled = false, expectedExceptions = MongoTableException.class)
    public void mongoTableDefinitionTest12() {
        log.info("mongoTableDefinitionTest12 - " +
                "DASC5-865:Defining a MongoDB table with an invalid value for an option defined in mongodburl");

        MongoTableTestUtils.dropCollection("FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@store(type = 'mongodb', " +
                "mongodb.uri='mongodb://admin:admin@localhost:27017/Foo?ssl=wso2true')" +
                "@PrimaryKey('symbol')" +
                "define table FooTable (symbol string, price float, volume long); ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test(enabled = false, expectedExceptions = MongoTableException.class)
    public void mongoTableDefinitionTest13() {
        log.info("mongoTableDefinitionTest13 - " +
                "DASC5-866:Defining a MongoDB table without a value for an option defined in mongodburl");

        MongoTableTestUtils.dropCollection("FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@store(type = 'mongodb', " +
                "mongodb.uri='mongodb://admin:admin@localhost:27017/Foo?maxPoolSize=')" +
                "@PrimaryKey('symbol')" +
                "define table FooTable (symbol string, price float, volume long); ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test(enabled = false)
    public void mongoTableDefinitionTest14() {
        log.info("mongoTableDefinitionTest14 - " +
                "DASC5-867:Defining a MongoDB table with contradictory values for the same option defined in " +
                "mongodburl");

        MongoTableTestUtils.dropCollection("FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@store(type = 'mongodb' , " +
                "mongodb.uri='mongodb://admin:admin@127.0.0.1/Foo?ssl=true&ssl=false') " +
                "define table FooTable (symbol string, price float, volume long); ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();

        boolean doesCollectionExists = MongoTableTestUtils.doesCollectionExists("FooTable");
        Assert.assertEquals(doesCollectionExists, true, "Definition failed");
    }

    @Test
    public void mongoTableDefinitionTest15() {
        log.info("mongoTableDefinitionTest15 - " +
                "DASC5-868:Defining a MongoDB event table with IndexBy field");

        MongoTableTestUtils.dropCollection("FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@store(type = 'mongodb', " +
                "mongodb.uri='mongodb://admin:admin@localhost/Foo?maxPoolSize=100&connectTimeoutMS=10000')" +
                "@IndexBy(\"price 1 {background:true}\")" +
                "define table FooTable (symbol string, price float, volume long); ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();

        boolean doesCollectionExists = MongoTableTestUtils.doesCollectionExists("FooTable");
        Assert.assertEquals(doesCollectionExists, true, "Definition failed");

        Document priceIndexExpected = new Document()
                .append("name", "price_1")
                .append("v", 2)
                .append("key", new Document("price", 1))
                .append("background", true);
        Document priceIndexActual = MongoTableTestUtils.getIndex("FooTable", "price_1");
        Assert.assertEquals(priceIndexActual, priceIndexExpected, "Index Creation Failed");
    }

    @Test(expectedExceptions = MongoTableException.class)
    public void mongoTableDefinitionTest16() {
        log.info("mongoTableDefinitionTest16 - " +
                "DASC5-869:Defining a MongoDB table without defining a value for indexing column within IndexBy field");

        MongoTableTestUtils.dropCollection("FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@store(type = 'mongodb', " +
                "mongodb.uri='mongodb://admin:admin@localhost:27017/Foo?maxPoolSize=')" +
                "@IndexBy(\"1 {background:true}\")" +
                "define table FooTable (symbol string, price float, volume long); ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = MongoTableException.class)
    public void mongoTableDefinitionTest17() {
        log.info("mongoTableDefinitionTest17 - " +
                "DASC5-870:Defining a MongoDB table with an invalid value for indexing column within IndexBy field");

        MongoTableTestUtils.dropCollection("FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@store(type = 'mongodb', mongodb.uri='mongodb://admin:admin@localhost:27017/Foo')" +
                "@IndexBy(\"symbol1234 1 {unique:true}\")" +
                "define table FooTable (symbol string, price float, volume long); ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void mongoTableDefinitionTest18() {
        log.info("mongoTableDefinitionTest18 - " +
                "DASC5-872:Defining a MongoDB table without defining a value for sorting within IndexBy field");

        MongoTableTestUtils.dropCollection("FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@store(type = 'mongodb', mongodb.uri='mongodb://admin:admin@localhost:27017/Foo')" +
                "@IndexBy(\"symbol {unique:true}\")" +
                "define table FooTable (symbol string, price float, volume long); ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();

        boolean doesCollectionExists = MongoTableTestUtils.doesCollectionExists("FooTable");
        Assert.assertEquals(doesCollectionExists, true, "Definition failed");

        Document indexExcepted = new org.bson.Document()
                .append("key", new org.bson.Document("symbol", 1))
                .append("name", "symbol_1")
                .append("v", 2)
                .append("unique", true);
        Document indexActual = MongoTableTestUtils.getIndex("FooTable", "symbol_1");
        Assert.assertEquals(indexActual, indexExcepted, "Index Definition Failed");
    }

    @Test
    public void mongoTableDefinitionTest19() {
        log.info("mongoTableDefinitionTest19 - " +
                "DASC5-872:Defining a MongoDB table without defining a value for sorting within IndexBy field");

        MongoTableTestUtils.dropCollection("FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@store(type = 'mongodb', mongodb.uri='mongodb://admin:admin@localhost:27017/Foo')" +
                "@IndexBy(\"symbol 1 {}\")" +
                "define table FooTable (symbol string, price float, volume long); ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();

        boolean doesCollectionExists = MongoTableTestUtils.doesCollectionExists("FooTable");
        Assert.assertEquals(doesCollectionExists, true, "Definition failed");

        Document indexExcepted = new org.bson.Document()
                .append("key", new org.bson.Document("symbol", 1))
                .append("name", "symbol_1")
                .append("v", 2);
        Document indexActual = MongoTableTestUtils.getIndex("FooTable", "symbol_1");
        Assert.assertEquals(indexActual, indexExcepted, "Index Definition Failed");
    }

    @Test(expectedExceptions = MongoTableException.class)
    public void mongoTableDefinitionTest20() {
        log.info("mongoTableDefinitionTest20 - " +
                "DASC5-874:Defining a MongoDB table by defining non existing options within IndexBy field");

        MongoTableTestUtils.dropCollection("FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@store(type = 'mongodb', mongodb.uri='mongodb://admin:admin@localhost:27017/Foo')" +
                "@IndexBy(\"symbol 1 {2222unique:true}\")" +
                "define table FooTable (symbol string, price float, volume long); ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();

        boolean doesCollectionExists = MongoTableTestUtils.doesCollectionExists("FooTable");
        Assert.assertEquals(doesCollectionExists, true, "Definition failed");

        Document indexExcepted = new org.bson.Document()
                .append("key", new org.bson.Document("symbol", 1))
                .append("name", "symbol_1")
                .append("v", 2);
        Document indexActual = MongoTableTestUtils.getIndex("FooTable", "symbol_1");
        Assert.assertEquals(indexActual, indexExcepted, "Index Definition Failed");
    }

    @Test(expectedExceptions = MongoTableException.class)
    public void mongoTableDefinitionTest21() {
        log.info("mongoTableDefinitionTest21 - " +
                "DASC5-875:Defining a MongoDB table by defining an option with an invalid value within IndexBy field");

        MongoTableTestUtils.dropCollection("FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@store(type = 'mongodb', mongodb.uri='mongodb://admin:admin@localhost:27017/Foo')" +
                "@IndexBy(\"symbol 1 {background:tr22ue}\")" +
                "define table FooTable (symbol string, price float, volume long); ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void mongoTableDefinitionTest22() {
        log.info("mongoTableDefinitionTest22 - " +
                "DASC5-876:Defining a MongoDB table by having contradictory values for an option " +
                "defined within IndexBy field");

        MongoTableTestUtils.dropCollection("FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@store(type = 'mongodb', mongodb.uri='mongodb://admin:admin@localhost:27017/Foo')" +
                "@IndexBy(\"symbol 1 {unique:true, unique: false}\")" +
                "define table FooTable (symbol string, price float, volume long); ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();

        boolean doesCollectionExists = MongoTableTestUtils.doesCollectionExists("FooTable");
        Assert.assertEquals(doesCollectionExists, true, "Definition failed");

        Document indexExcepted = new org.bson.Document()
                .append("key", new org.bson.Document("symbol", 1))
                .append("name", "symbol_1")
                .append("v", 2);
        Document indexActual = MongoTableTestUtils.getIndex("FooTable", "symbol_1");
        Assert.assertEquals(indexActual, indexExcepted, "Index Definition Failed");
    }

    @Test
    public void mongoTableDefinitionTest23() {
        log.info("mongoTableDefinitionTest23 - " +
                "DASC5-948:Defining a MongoDB event table with a new collection name");

        MongoTableTestUtils.dropCollection("newcollection");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@store(type = 'mongodb' , " +
                "mongodb.uri='mongodb://admin:admin@127.0.0.1/Foo', collection.name=\"newcollection\") " +
                "define table FooTable (symbol string, price float, volume long); ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();

        boolean doesCollectionExists = MongoTableTestUtils.doesCollectionExists("newcollection");
        Assert.assertEquals(doesCollectionExists, true, "Definition failed");
    }

    @Test
    public void mongoTableDefinitionTest24() {
        log.info("mongoTableDefinitionTest24 - " +
                "DASC5-949:Defining a MongoDB event table with a existing collection name");

        MongoTableTestUtils.createCollection("newcollection");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@store(type = 'mongodb' , mongodb.uri='mongodb://admin:admin@127.0.0.1/Foo', " +
                "collection.name=\"newcollection\") " +
                "define table FooTable (symbol string, price float, volume long); ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();

        boolean doesCollectionExists = MongoTableTestUtils.doesCollectionExists("newcollection");
        Assert.assertEquals(doesCollectionExists, true, "Definition failed");
    }

    @Test(enabled = false)
    public void mongoTableDefinitionTest25() {
        log.info("mongoTableDefinitionTest25 - " +
                "DASC5-962:Defining a MongoDB event table with multiple hosts and default ports");

        MongoTableTestUtils.dropCollection("FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@store(type = 'mongodb' , " +
                "mongodb.uri=\"mongodb://admin:admin@localhost:27017,192.168.11.17:27017/Foo\") " +
                "define table FooTable (symbol string, price float, volume long); ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();

        boolean doesCollectionExists = MongoTableTestUtils.doesCollectionExists("FooTable");
        Assert.assertEquals(doesCollectionExists, true, "Definition failed");
    }

    @Test(enabled = false)
    public void mongoTableDefinitionTest26() {
        log.info("mongoTableDefinitionTest26 - " +
                "DASC5-963:Defining a MongoDB event table with multiple hosts and non-default ports");

        MongoTableTestUtils.dropCollection("FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@store(type = 'mongodb' , " +
                "mongodb.uri=\"mongodb://admin:admin@localhost:27018,192.168.11.17:27018/Foo\") " +
                "define table FooTable (symbol string, price float, volume long); ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();

        boolean doesCollectionExists = MongoTableTestUtils.doesCollectionExists("FooTable");
        Assert.assertEquals(doesCollectionExists, true, "Definition failed");
    }

    @Test
    public void mongoTableDefinitionTest27() {
        log.info("mongoTableDefinitionTest27 - " +
                "DASC5-965:Defining a MongoDB event table by having multiple indexing columns");

        MongoTableTestUtils.dropCollection("FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@store(type = 'mongodb', " +
                "mongodb.uri='mongodb://admin:admin@localhost/Foo?maxPoolSize=100&connectTimeoutMS=10000')" +
                "@IndexBy(\"price 1 {background:true}\",\"volume 1 {background:true}\")" +
                "define table FooTable (symbol string, price float, volume long); ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();

        boolean doesCollectionExists = MongoTableTestUtils.doesCollectionExists("FooTable");
        Assert.assertEquals(doesCollectionExists, true, "Definition failed");

        Document priceIndexExpected = new Document()
                .append("name", "price_1")
                .append("v", 2)
                .append("key", new Document("price", 1))
                .append("background", true);
        Document priceIndexActual = MongoTableTestUtils.getIndex("FooTable", "price_1");
        Assert.assertEquals(priceIndexActual, priceIndexExpected, "Index Creation Failed");

        Document volumeIndexExpected = new Document()
                .append("name", "volume_1")
                .append("v", 2)
                .append("key", new Document("volume", 1))
                .append("background", true);
        Document volumeIndexActual = MongoTableTestUtils.getIndex("FooTable", "volume_1");
        Assert.assertEquals(volumeIndexActual, volumeIndexExpected, "Index Creation Failed");
    }

    @Test
    public void mongoTableDefinitionTest28() {
        log.info("mongoTableDefinitionTest28 - DASC5-856:Defining a MongoDB event table with a Primary Key field");

        MongoTableTestUtils.dropCollection("FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@store(type = 'mongodb' , mongodb.uri='mongodb://admin:admin@127.0.0.1/Foo') " +
                "@PrimaryKey('symbol','price')" +
                "define table FooTable (symbol string, price float, volume long); " +
                "define stream StockStream (symbol string, price float, volume long);";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();

        boolean doesCollectionExists = MongoTableTestUtils.doesCollectionExists("FooTable");
        Assert.assertEquals(doesCollectionExists, true, "Definition failed");

        Document key = new Document()
                .append("symbol", 1)
                .append("price", 1);
        Document indexExcepted = new org.bson.Document()
                .append("key", key)
                .append("name", "symbol_1_price_1")
                .append("v", 2)
                .append("unique", true);
        Document indexActual = MongoTableTestUtils.getIndex("FooTable", "symbol_1_price_1");
        Assert.assertEquals(indexActual, indexExcepted, "Primary Key Definition Failed");
    }

}
