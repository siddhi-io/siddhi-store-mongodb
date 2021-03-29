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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.bson.Document;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class DefineMongoTableTest {

    private static final Log log = LogFactory.getLog(DefineMongoTableTest.class);
    private static String uri = MongoTableTestUtils.resolveBaseUri();

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

        MongoTableTestUtils.dropCollection(uri, "FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@store(type = 'mongodb' , mongodb.uri='" + uri + "') " +
                "define table FooTable (symbol string, price float, volume long); ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();

        boolean doesCollectionExists = MongoTableTestUtils.doesCollectionExists(uri, "FooTable");
        Assert.assertEquals(doesCollectionExists, true, "Definition failed");
    }

    @Test
    public void mongoTableDefinitionTest2() {
        log.info("mongoTableDefinitionTest2 - " +
                "DASC5-854:Defining a MongoDB event table with an existing collection");

        MongoTableTestUtils.createCollection(uri, "FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@store(type = 'mongodb' , mongodb.uri='" + uri + "') " +
                "@PrimaryKey('symbol')" +
                "define table FooTable (symbol string, price float, volume long); ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();

        boolean doesCollectionExists = MongoTableTestUtils.doesCollectionExists(uri, "FooTable");
        Assert.assertEquals(doesCollectionExists, true, "Definition failed");

    }

    @Test
    public void mongoTableDefinitionTest3() {
        log.info("mongoTableDefinitionTest3 - DASC5-856:Defining a MongoDB event table with a Primary Key field");

        MongoTableTestUtils.dropCollection(uri, "FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@store(type = 'mongodb' , mongodb.uri='" + uri + "') " +
                "@PrimaryKey('symbol')" +
                "define table FooTable (symbol string, price float, volume long); " +
                "define stream StockStream (symbol string, price float, volume long);";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();

        boolean doesCollectionExists = MongoTableTestUtils.doesCollectionExists(uri, "FooTable");
        Assert.assertEquals(doesCollectionExists, true, "Definition failed");

        Document indexExcepted = new org.bson.Document()
                .append("key", new org.bson.Document("symbol", 1))
                .append("name", "symbol_1")
                .append("v", 2)
                .append("unique", true);
        Document indexActual = MongoTableTestUtils.getIndex(uri, "FooTable", "symbol_1");
        Assert.assertEquals(indexActual, indexExcepted, "Primary Key Definition Failed");
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void mongoTableDefinitionTest4() {
        log.info("mongoTableDefinitionTest4 - " +
                "DASC5-857:Defining a MongoDB table without defining a value for Primary Key field");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@store(type = 'mongodb' , mongodb.uri='" + uri + "') " +
                "@PrimaryKey('')" +
                "define table FooTable (symbol string, price float, volume long); ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void mongoTableDefinitionTest5() {
        log.info("mongoTableDefinitionTest5 - " +
                "DASC5-858:Defining a MongoDB table with an invalid value for Primary Key field");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@store(type = 'mongodb' , mongodb.uri='" + uri + "') " +
                "@PrimaryKey('symbol234')" +
                "define table FooTable (symbol string, price float, volume long); ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void mongoTableDefinitionTest6() {
        log.info("mongoTableDefinitionTest6 - " +
                "DASC5-859:Defining a MongoDB table without having a mongodb uri field");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@store(type = 'mongodb') " +
                "@PrimaryKey('symbol')" +
                "define table FooTable (symbol string, price float, volume long); ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void mongoTableDefinitionTest7() {
        log.info("mongoTableDefinitionTest7 - " +
                "DASC5-860:Defining a MongoDB table without defining a value for mongodb uri field");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@store(type = 'mongodb', mongodb.uri='') " +
                "@PrimaryKey('symbol')" +
                "define table FooTable (symbol string, price float, volume long); ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void mongoTableDefinitionTest8() {
        log.info("mongoTableDefinitionTest8 - " +
                "DASC5-861:Defining a MongoDBS table with an invalid value for mongodb uri field");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@store(type = 'mongodb', mongodb.uri='1234444" + uri + "') " +
                "@PrimaryKey('symbol')" +
                "define table FooTable (symbol string, price float, volume long); ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void mongoTableDefinitionTest9() {
        log.info("mongoTableDefinitionTest9 - " +
                "DASC5-864:Defining a MongoDB table with an invalid option defined in mongodburl");

        MongoTableTestUtils.dropCollection(uri, "FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@store(type = 'mongodb', " +
                "mongodb.uri='" + uri + "?wso2ssl=true')" +
                "@PrimaryKey('symbol')" +
                "define table FooTable (symbol string, price float, volume long); ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();

        boolean doesCollectionExists = MongoTableTestUtils.doesCollectionExists(uri, "FooTable");
        Assert.assertEquals(doesCollectionExists, true, "Definition failed");
    }

    @Test
    public void mongoTableDefinitionTest10() {
        log.info("mongoTableDefinitionTest10 - " +
                "DASC5-865:Defining a MongoDB table with an invalid value for an option defined in mongodburl");

        MongoTableTestUtils.dropCollection(uri, "FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@store(type = 'mongodb', " +
                "mongodb.uri='" + uri + "?ssl=wso2true')" +
                "@PrimaryKey('symbol')" +
                "define table FooTable (symbol string, price float, volume long); ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();

        boolean doesCollectionExists = MongoTableTestUtils.doesCollectionExists(uri, "FooTable");
        Assert.assertEquals(doesCollectionExists, true, "Definition failed");
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void mongoTableDefinitionTest11() {
        log.info("mongoTableDefinitionTest11 - " +
                "DASC5-866:Defining a MongoDB table without a value for an option defined in mongodburl");

        MongoTableTestUtils.dropCollection(uri, "FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@store(type = 'mongodb', " +
                "mongodb.uri='" + uri + "?maxPoolSize=')" +
                "@PrimaryKey('symbol')" +
                "define table FooTable (symbol string, price float, volume long); ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void mongoTableDefinitionTest12() {
        log.info("mongoTableDefinitionTest12 - " +
                "DASC5-867:Defining a MongoDB table with contradictory values for the same option defined in " +
                "mongodburl");

        MongoTableTestUtils.dropCollection(uri, "FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@store(type = 'mongodb' , " +
                "mongodb.uri='" + uri + "?ssl=true&ssl=false') " +
                "define table FooTable (symbol string, price float, volume long); ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();

        boolean doesCollectionExists = MongoTableTestUtils.doesCollectionExists(uri, "FooTable");
        Assert.assertEquals(doesCollectionExists, true, "Definition failed");
    }

    @Test
    public void mongoTableDefinitionTest13() {
        log.info("mongoTableDefinitionTest13 - " +
                "DASC5-868:Defining a MongoDB event table with IndexBy field");

        MongoTableTestUtils.dropCollection(uri, "FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@store(type = 'mongodb', " +
                "mongodb.uri='" + uri + "')" +
                "@IndexBy(\"price 1 {background:true}\")" +
                "define table FooTable (symbol string, price float, volume long); ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();

        boolean doesCollectionExists = MongoTableTestUtils.doesCollectionExists(uri, "FooTable");
        Assert.assertEquals(doesCollectionExists, true, "Definition failed");

        Document priceIndexExpected = new Document()
                .append("name", "price_1")
                .append("v", 2)
                .append("key", new Document("price", 1))
                .append("background", true);
        Document priceIndexActual = MongoTableTestUtils.getIndex(uri, "FooTable", "price_1");
        Assert.assertEquals(priceIndexActual, priceIndexExpected, "Index Creation Failed");
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void mongoTableDefinitionTest14() {
        log.info("mongoTableDefinitionTest14 - " +
                "DASC5-869:Defining a MongoDB table without defining a value for indexing column within IndexBy field");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@store(type = 'mongodb', " +
                "mongodb.uri='" + uri + "')" +
                "@IndexBy(\"1 {background:true}\")" +
                "define table FooTable (symbol string, price float, volume long); ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void mongoTableDefinitionTest15() {
        log.info("mongoTableDefinitionTest15 - " +
                "DASC5-870:Defining a MongoDB table with an invalid value for indexing column within IndexBy field");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@store(type = 'mongodb', mongodb.uri='" + uri + "')" +
                "@IndexBy(\"symbol1234 1 {unique:true}\")" +
                "define table FooTable (symbol string, price float, volume long); ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void mongoTableDefinitionTest16() {
        log.info("mongoTableDefinitionTest16 - " +
                "DASC5-872:Defining a MongoDB table without defining a value for sorting within IndexBy field");

        MongoTableTestUtils.dropCollection(uri, "FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@store(type = 'mongodb', mongodb.uri='" + uri + "')" +
                "@IndexBy(\"symbol {unique:true}\")" +
                "define table FooTable (symbol string, price float, volume long); ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();

        boolean doesCollectionExists = MongoTableTestUtils.doesCollectionExists(uri, "FooTable");
        Assert.assertEquals(doesCollectionExists, true, "Definition failed");

        Document indexExcepted = new org.bson.Document()
                .append("key", new org.bson.Document("symbol", 1))
                .append("name", "symbol_1")
                .append("v", 2)
                .append("unique", true);
        Document indexActual = MongoTableTestUtils.getIndex(uri, "FooTable", "symbol_1");
        Assert.assertEquals(indexActual, indexExcepted, "Index Definition Failed");
    }

    @Test
    public void mongoTableDefinitionTest17() {
        log.info("mongoTableDefinitionTest17 - " +
                "DASC5-872:Defining a MongoDB table without defining a value for sorting within IndexBy field");

        MongoTableTestUtils.dropCollection(uri, "FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@store(type = 'mongodb', mongodb.uri='" + uri + "')" +
                "@IndexBy(\"symbol 1 {}\")" +
                "define table FooTable (symbol string, price float, volume long); ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();

        boolean doesCollectionExists = MongoTableTestUtils.doesCollectionExists(uri, "FooTable");
        Assert.assertEquals(doesCollectionExists, true, "Definition failed");

        Document indexExcepted = new org.bson.Document()
                .append("key", new org.bson.Document("symbol", 1))
                .append("name", "symbol_1")
                .append("v", 2);
        Document indexActual = MongoTableTestUtils.getIndex(uri, "FooTable", "symbol_1");
        Assert.assertEquals(indexActual, indexExcepted, "Index Definition Failed");
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void mongoTableDefinitionTest18() {
        log.info("mongoTableDefinitionTest18 - " +
                "DASC5-874:Defining a MongoDB table by defining non existing options within IndexBy field");

        MongoTableTestUtils.dropCollection(uri, "FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@store(type = 'mongodb', mongodb.uri='" + uri + "')" +
                "@IndexBy(\"symbol 1 {2222unique:true}\")" +
                "define table FooTable (symbol string, price float, volume long); ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();

        boolean doesCollectionExists = MongoTableTestUtils.doesCollectionExists(uri, "FooTable");
        Assert.assertEquals(doesCollectionExists, true, "Definition failed");

        Document indexExcepted = new org.bson.Document()
                .append("key", new org.bson.Document("symbol", 1))
                .append("name", "symbol_1")
                .append("v", 2);
        Document indexActual = MongoTableTestUtils.getIndex(uri, "FooTable", "symbol_1");
        Assert.assertEquals(indexActual, indexExcepted, "Index Definition Failed");
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void mongoTableDefinitionTest19() {
        log.info("mongoTableDefinitionTest19 - " +
                "DASC5-875:Defining a MongoDB table by defining an option with an invalid value within IndexBy field");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@store(type = 'mongodb', mongodb.uri='" + uri + "')" +
                "@IndexBy(\"symbol 1 {background:tr22ue}\")" +
                "define table FooTable (symbol string, price float, volume long); ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void mongoTableDefinitionTest20() {
        log.info("mongoTableDefinitionTest20 - " +
                "DASC5-876:Defining a MongoDB table by having contradictory values for an option " +
                "defined within IndexBy field");

        MongoTableTestUtils.dropCollection(uri, "FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@store(type = 'mongodb', mongodb.uri='" + uri + "')" +
                "@IndexBy(\"symbol 1 {unique:true, unique: false}\")" +
                "define table FooTable (symbol string, price float, volume long); ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();

        boolean doesCollectionExists = MongoTableTestUtils.doesCollectionExists(uri, "FooTable");
        Assert.assertEquals(doesCollectionExists, true, "Definition failed");

        Document indexExcepted = new org.bson.Document()
                .append("key", new org.bson.Document("symbol", 1))
                .append("name", "symbol_1")
                .append("v", 2);
        Document indexActual = MongoTableTestUtils.getIndex(uri, "FooTable", "symbol_1");
        Assert.assertEquals(indexActual, indexExcepted, "Index Definition Failed");
    }

    @Test
    public void mongoTableDefinitionTest21() {
        log.info("mongoTableDefinitionTest21 - " +
                "DASC5-948:Defining a MongoDB event table with a new collection name");

        MongoTableTestUtils.dropCollection(uri, "newcollection");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@store(type = 'mongodb' , " +
                "mongodb.uri='" + uri + "', collection.name=\"newcollection\") " +
                "define table FooTable (symbol string, price float, volume long); ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();

        boolean doesCollectionExists = MongoTableTestUtils.doesCollectionExists(uri, "newcollection");
        Assert.assertEquals(doesCollectionExists, true, "Definition failed");
    }

    @Test
    public void mongoTableDefinitionTest22() {
        log.info("mongoTableDefinitionTest22 - " +
                "DASC5-949:Defining a MongoDB event table with a existing collection name");

        MongoTableTestUtils.createCollection(uri, "newcollection");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@store(type = 'mongodb' , mongodb.uri='" + uri + "', " +
                "collection.name=\"newcollection\") " +
                "define table FooTable (symbol string, price float, volume long); ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();

        boolean doesCollectionExists = MongoTableTestUtils.doesCollectionExists(uri, "newcollection");
        Assert.assertEquals(doesCollectionExists, true, "Definition failed");
    }

    @Test
    public void mongoTableDefinitionTest23() {
        log.info("mongoTableDefinitionTest23 - " +
                "DASC5-965:Defining a MongoDB event table by having multiple indexing columns");

        MongoTableTestUtils.dropCollection(uri, "FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@store(type = 'mongodb', mongodb.uri='" + uri + "')" +
                "@IndexBy(\"price 1 {background:true}\",\"volume 1 {background:true}\")" +
                "define table FooTable (symbol string, price float, volume long); ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();

        boolean doesCollectionExists = MongoTableTestUtils.doesCollectionExists(uri, "FooTable");
        Assert.assertEquals(doesCollectionExists, true, "Definition failed");

        Document priceIndexExpected = new Document()
                .append("name", "price_1")
                .append("v", 2)
                .append("key", new Document("price", 1))
                .append("background", true);
        Document priceIndexActual = MongoTableTestUtils.getIndex(uri, "FooTable", "price_1");
        Assert.assertEquals(priceIndexActual, priceIndexExpected, "Index Creation Failed");

        Document volumeIndexExpected = new Document()
                .append("name", "volume_1")
                .append("v", 2)
                .append("key", new Document("volume", 1))
                .append("background", true);
        Document volumeIndexActual = MongoTableTestUtils.getIndex(uri, "FooTable", "volume_1");
        Assert.assertEquals(volumeIndexActual, volumeIndexExpected, "Index Creation Failed");
    }

    @Test
    public void mongoTableDefinitionTest24() {
        log.info("mongoTableDefinitionTest24 - DASC5-856:Defining a MongoDB event table with a Primary Key field");

        MongoTableTestUtils.dropCollection(uri, "FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@store(type = 'mongodb' , mongodb.uri='" + uri + "') " +
                "@PrimaryKey('symbol','price')" +
                "define table FooTable (symbol string, price float, volume long); " +
                "define stream StockStream (symbol string, price float, volume long);";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();

        boolean doesCollectionExists = MongoTableTestUtils.doesCollectionExists(uri, "FooTable");
        Assert.assertEquals(doesCollectionExists, true, "Definition failed");

        Document key = new Document()
                .append("symbol", 1)
                .append("price", 1);
        Document indexExcepted = new org.bson.Document()
                .append("key", key)
                .append("name", "symbol_1_price_1")
                .append("v", 2)
                .append("unique", true);
        Document indexActual = MongoTableTestUtils.getIndex(uri, "FooTable", "symbol_1_price_1");
        Assert.assertEquals(indexActual, indexExcepted, "Primary Key Definition Failed");
    }

    @Test
    public void mongoTableDefinitionTest25() {
        log.info("mongoTableDefinitionTest25 - " +
                "DASC5-868:Defining a MongoDB event table with Index field");

        MongoTableTestUtils.dropCollection(uri, "FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@store(type = 'mongodb', " +
                "mongodb.uri='" + uri + "')" +
                "@Index(\"price:1\", \"{background:true}\")" +
                "define table FooTable (symbol string, price float, volume long); ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();

        boolean doesCollectionExists = MongoTableTestUtils.doesCollectionExists(uri, "FooTable");
        Assert.assertEquals(doesCollectionExists, true, "Definition failed");

        Document priceIndexExpected = new Document()
                .append("name", "price_1")
                .append("v", 2)
                .append("key", new Document("price", 1))
                .append("background", true);
        Document priceIndexActual = MongoTableTestUtils.getIndex(uri, "FooTable", "price_1");
        Assert.assertEquals(priceIndexActual, priceIndexExpected, "Index Creation Failed");
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void mongoTableDefinitionTest26() {
        log.info("mongoTableDefinitionTest26 - " +
                "DASC5-869:Defining a MongoDB table without defining a value for indexing column within Index field");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@store(type = 'mongodb', " +
                "mongodb.uri='" + uri + "')" +
                "@Index(\"1 {background:true}\")" +
                "define table FooTable (symbol string, price float, volume long); ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void mongoTableDefinitionTest27() {
        log.info("mongoTableDefinitionTest27 - " +
                "DASC5-870:Defining a MongoDB table with an invalid value for indexing column within IndexBy field");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@store(type = 'mongodb', mongodb.uri='" + uri + "')" +
                "@Index(\"symbol1234:1\", \"{unique:true}\")" +
                "define table FooTable (symbol string, price float, volume long); ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void mongoTableDefinitionTest28() {
        log.info("mongoTableDefinitionTest28 - " +
                "DASC5-872:Defining a MongoDB table without defining a value for sorting within Index field");

        MongoTableTestUtils.dropCollection(uri, "FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@store(type = 'mongodb', mongodb.uri='" + uri + "')" +
                "@Index(\"symbol\", \"{unique:true}\")" +
                "define table FooTable (symbol string, price float, volume long); ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();

        boolean doesCollectionExists = MongoTableTestUtils.doesCollectionExists(uri, "FooTable");
        Assert.assertEquals(doesCollectionExists, true, "Definition failed");

        Document indexExcepted = new org.bson.Document()
                .append("key", new org.bson.Document("symbol", 1))
                .append("name", "symbol_1")
                .append("v", 2)
                .append("unique", true);
        Document indexActual = MongoTableTestUtils.getIndex(uri, "FooTable", "symbol_1");
        Assert.assertEquals(indexActual, indexExcepted, "Index Definition Failed");
    }

    @Test
    public void mongoTableDefinitionTest29() {
        log.info("mongoTableDefinitionTest29 - " +
                "DASC5-872:Defining a MongoDB table without defining a value for sorting within Index field");

        MongoTableTestUtils.dropCollection(uri, "FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@store(type = 'mongodb', mongodb.uri='" + uri + "')" +
                "@Index(\"symbol:1\", \"{}\")" +
                "define table FooTable (symbol string, price float, volume long); ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();

        boolean doesCollectionExists = MongoTableTestUtils.doesCollectionExists(uri, "FooTable");
        Assert.assertEquals(doesCollectionExists, true, "Definition failed");

        Document indexExcepted = new org.bson.Document()
                .append("key", new org.bson.Document("symbol", 1))
                .append("name", "symbol_1")
                .append("v", 2);
        Document indexActual = MongoTableTestUtils.getIndex(uri, "FooTable", "symbol_1");
        Assert.assertEquals(indexActual, indexExcepted, "Index Definition Failed");
    }

    @Test
    public void mongoTableDefinitionTest30() {
        log.info("mongoTableDefinitionTest30 - " +
                "DASC5-872:Defining a MongoDB table without defining a value for sorting within Index field");

        MongoTableTestUtils.dropCollection(uri, "FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@store(type = 'mongodb', mongodb.uri='" + uri + "')" +
                "@Index(\"symbol:1\")" +
                "define table FooTable (symbol string, price float, volume long); ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();

        boolean doesCollectionExists = MongoTableTestUtils.doesCollectionExists(uri, "FooTable");
        Assert.assertEquals(doesCollectionExists, true, "Definition failed");

        Document indexExcepted = new org.bson.Document()
                .append("key", new org.bson.Document("symbol", 1))
                .append("name", "symbol_1")
                .append("v", 2);
        Document indexActual = MongoTableTestUtils.getIndex(uri, "FooTable", "symbol_1");
        Assert.assertEquals(indexActual, indexExcepted, "Index Definition Failed");
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void mongoTableDefinitionTest31() {
        log.info("mongoTableDefinitionTest31 - " +
                "DASC5-874:Defining a MongoDB table by defining non existing options within IndexBy field");

        MongoTableTestUtils.dropCollection(uri, "FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@store(type = 'mongodb', mongodb.uri='" + uri + "')" +
                "@Index(\"symbol:1\", \"{2222unique:true}\")" +
                "define table FooTable (symbol string, price float, volume long); ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();

        boolean doesCollectionExists = MongoTableTestUtils.doesCollectionExists(uri, "FooTable");
        Assert.assertEquals(doesCollectionExists, true, "Definition failed");

        Document indexExcepted = new org.bson.Document()
                .append("key", new org.bson.Document("symbol", 1))
                .append("name", "symbol_1")
                .append("v", 2);
        Document indexActual = MongoTableTestUtils.getIndex(uri, "FooTable", "symbol_1");
        Assert.assertEquals(indexActual, indexExcepted, "Index Definition Failed");
    }

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void mongoTableDefinitionTest32() {
        log.info("mongoTableDefinitionTest30 - " +
                "DASC5-875:Defining a MongoDB table by defining an option with an invalid value within Index field");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@store(type = 'mongodb', mongodb.uri='" + uri + "')" +
                "@Index(\"symbol:1\", \"{background:tr22ue}\")" +
                "define table FooTable (symbol string, price float, volume long); ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void mongoTableDefinitionTest33() {
        log.info("mongoTableDefinitionTest33 - " +
                "DASC5-876:Defining a MongoDB table by having contradictory values for an option " +
                "defined within Index field");

        MongoTableTestUtils.dropCollection(uri, "FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@store(type = 'mongodb', mongodb.uri='" + uri + "')" +
                "@Index(\"symbol:1\", \"{unique:true, unique: false}\")" +
                "define table FooTable (symbol string, price float, volume long); ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();

        boolean doesCollectionExists = MongoTableTestUtils.doesCollectionExists(uri, "FooTable");
        Assert.assertEquals(doesCollectionExists, true, "Definition failed");

        Document indexExcepted = new org.bson.Document()
                .append("key", new org.bson.Document("symbol", 1))
                .append("name", "symbol_1")
                .append("v", 2);
        Document indexActual = MongoTableTestUtils.getIndex(uri, "FooTable", "symbol_1");
        Assert.assertEquals(indexActual, indexExcepted, "Index Definition Failed");
    }


    @Test
    public void mongoTableDefinitionTest34() {
        log.info("mongoTableDefinitionTest34 - " +
                "DASC5-965:Defining a MongoDB event table by having multiple indexing columns");

        MongoTableTestUtils.dropCollection(uri, "FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@store(type = 'mongodb', mongodb.uri='" + uri + "')" +
                "@Index(\"price:1\", \"{background:true}\")" +
                "@Index(\"volume:1\", \"{background:true}\")" +
                "define table FooTable (symbol string, price float, volume long); ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();

        boolean doesCollectionExists = MongoTableTestUtils.doesCollectionExists(uri, "FooTable");
        Assert.assertEquals(doesCollectionExists, true, "Definition failed");

        Document priceIndexExpected = new Document()
                .append("name", "price_1")
                .append("v", 2)
                .append("key", new Document("price", 1))
                .append("background", true);
        Document priceIndexActual = MongoTableTestUtils.getIndex(uri, "FooTable", "price_1");
        Assert.assertEquals(priceIndexActual, priceIndexExpected, "Index Creation Failed");

        Document volumeIndexExpected = new Document()
                .append("name", "volume_1")
                .append("v", 2)
                .append("key", new Document("volume", 1))
                .append("background", true);
        Document volumeIndexActual = MongoTableTestUtils.getIndex(uri, "FooTable", "volume_1");
        Assert.assertEquals(volumeIndexActual, volumeIndexExpected, "Index Creation Failed");
    }

    @Test
    public void mongoTableDefinitionTest35() {
        log.info("mongoTableDefinitionTest35 - " +
                "DASC5-965:Defining a MongoDB event table by having compound indexing columns");

        MongoTableTestUtils.dropCollection(uri, "FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@store(type = 'mongodb', mongodb.uri='" + uri + "')" +
                "@Index(\"price:1\", \"volume:1\", \"{background:true}\")" +
                "define table FooTable (symbol string, price float, volume long); ";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();

        boolean doesCollectionExists = MongoTableTestUtils.doesCollectionExists(uri, "FooTable");
        Assert.assertTrue(doesCollectionExists, "Definition failed");

        Document indexFields = new Document("price", 1);
        indexFields.put("volume", 1);

        Document compoundIndexExpected = new Document()
                .append("name", "volume_1_price_1")
                .append("v", 2)
                .append("key", indexFields)
                .append("background", true);
        Document compoundIndexActual = MongoTableTestUtils.getIndex(uri, "FooTable", "volume_1_price_1");
        Assert.assertEquals(compoundIndexActual, compoundIndexExpected, "Index Creation Failed");

    }

}
