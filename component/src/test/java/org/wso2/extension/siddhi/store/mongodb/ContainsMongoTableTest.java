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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.core.util.SiddhiTestHelper;

import java.util.concurrent.atomic.AtomicInteger;


public class ContainsMongoTableTest {

    private static final Log log = LogFactory.getLog(DeleteFromMongoTableTest.class);

    private AtomicInteger eventCount = new AtomicInteger(0);
    private int waitTime = 50;
    private int timeout = 30000;

    @BeforeClass
    public void init() {
        log.info("== MongoDB Collection IN tests started ==");
    }

    @AfterClass
    public void shutdown() {
        log.info("== MongoDB Collection IN tests completed ==");
    }

    @BeforeMethod
    public void testInit() {
        eventCount.set(0);
    }

    @Test
    public void containsMongoTableTest1() throws InterruptedException {
        log.info("containsMongoTableTest1 - " +
                "DASC5-911:Configure siddhi to check whether particular records exist in a MongoDB Collection");

        MongoTableTestUtils.dropCollection("FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@source(type='inMemory') " +
                "define stream FooStream (symbol string, price float, volume long);" +
                "@store(type = 'mongodb' , mongodb.uri='mongodb://admin:admin@127.0.0.1/Foo')" +
                "define table FooTable (symbol string, price float, volume long);";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream   " +
                "insert into FooTable ;" +

                "@info(name='query2')" +
                "from FooStream[(FooTable.symbol == symbol) in FooTable]" +
                "insert into OutputStream ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.addCallback("OutputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                if (events != null) {
                    for (Event event : events) {
                        eventCount.incrementAndGet();
                        switch (eventCount.intValue()) {
                            case 1:
                                Assert.assertEquals(new Object[]{"WSO2", 5.56, 200}, event.getData());
                                break;
                            case 2:
                                Assert.assertEquals(new Object[]{"IBM", 7.56, 200}, event.getData());
                                break;
                            default:
                                break;
                        }
                    }
                }
            }
        });
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        stockStream.send(new Object[]{"IBM", 75.6F, 100L});
        stockStream.send(new Object[]{"WSO2_2", 57.6F, 100L});
        fooStream.send(new Object[]{"WSO2", 5.56, 200});
        fooStream.send(new Object[]{"IBM", 7.56, 200});
        fooStream.send(new Object[]{"IBM_2", 70.56, 200});
        SiddhiTestHelper.waitForEvents(waitTime, 2, eventCount, timeout);

        siddhiAppRuntime.shutdown();

        Assert.assertEquals(eventCount.intValue(), 2, "Number of success events");
    }

    @Test
    public void containsMongoTableTest2() throws InterruptedException {
        log.info("containsMongoTableTest2 - " +
                "DASC5-912:Configure siddhi to check whether record exist when OutputStream is already exists");

        MongoTableTestUtils.dropCollection("FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@source(type='inMemory') " +
                "define stream FooStream (symbol string, price float, volume long);" +
                "@store(type = 'mongodb' , mongodb.uri='mongodb://admin:admin@127.0.0.1/Foo')" +
                "define table FooTable (symbol string, price float, volume long);" +
                "@source(type='inMemory')" +
                "define stream OutputStream (symbol string, price float, volume long);";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream   " +
                "insert into FooTable ;" +
                "@info(name='query2')" +
                "from FooStream[(FooTable.symbol == symbol) in FooTable]" +
                "insert into OutputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.addCallback("OutputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                if (events != null) {
                    for (Event event : events) {
                        eventCount.incrementAndGet();
                        switch (eventCount.intValue()) {
                            case 1:
                                Assert.assertEquals(new Object[]{"WSO2", 50.56, 200}, event.getData());
                                break;
                            case 2:
                                Assert.assertEquals(new Object[]{"IBM", 70.56, 200}, event.getData());
                                break;
                            default:
                                Assert.assertEquals(new Object[]{}, event.getData());
                                break;
                        }
                    }
                }
            }
        });
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        stockStream.send(new Object[]{"IBM", 75.6F, 100L});
        stockStream.send(new Object[]{"WSO2_2", 57.6F, 100L});
        fooStream.send(new Object[]{"WSO2", 50.56, 200});
        fooStream.send(new Object[]{"IBM", 70.56, 200});
        fooStream.send(new Object[]{"IBM_2", 70.56, 200});
        SiddhiTestHelper.waitForEvents(waitTime, 2, eventCount, timeout);

        siddhiAppRuntime.shutdown();

        Assert.assertEquals(eventCount.intValue(), 2, "Number of success events");
    }
}
