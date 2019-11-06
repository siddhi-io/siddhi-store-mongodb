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
import io.siddhi.core.event.Event;
import io.siddhi.core.query.output.callback.QueryCallback;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.util.SiddhiTestHelper;
import io.siddhi.query.compiler.exception.SiddhiParserException;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class JoinMongoTableTest {

    private static final Logger log = Logger.getLogger(JoinMongoTableTest.class);

    private static String uri = MongoTableTestUtils.resolveBaseUri();
    private AtomicInteger eventCount = new AtomicInteger(0);
    private int waitTime = 50;
    private int timeout = 30000;

    @BeforeClass
    public void init() {
        log.info("== Mongo Table JOIN tests started ==");
    }

    @AfterClass
    public void shutdown() {
        log.info("== Mongo Table JOIN tests completed ==");
    }

    @BeforeMethod
    public void testInit() {
        eventCount.set(0);
    }

    @Test
    public void testMongoTableJoinQuery1() throws InterruptedException {
        log.info("testMongoTableJoinQuery1 -" +
                "DASC5-915:Read events from a MongoDB collection successfully");

        MongoTableTestUtils.dropCollection(uri, "FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream FooStream (symbol string); " +
                "@store(type = 'mongodb' , mongodb.uri='" + uri + "')" +
                "define table FooTable (symbol string, price float, volume long);";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into FooTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from FooStream#window.length(1) join FooTable " +
                "select FooStream.symbol as checkSymbol, FooTable.symbol as symbol, " +
                "FooTable.volume as volume  " +
                "insert into OutputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        eventCount.incrementAndGet();
                        switch (eventCount.intValue()) {
                            case 1:
                                Assert.assertEquals(new Object[]{"WSO2_check", "WSO2", 100L}, event.getData());
                                break;
                            case 2:
                                Assert.assertEquals(new Object[]{"WSO2_check", "IBM", 10L}, event.getData());
                                break;
                            default:
                                break;
                        }
                    }
                }
            }

        });

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 5.6f, 100L});
        stockStream.send(new Object[]{"IBM", 7.6f, 10L});
        fooStream.send(new Object[]{"WSO2_check"});
        SiddhiTestHelper.waitForEvents(waitTime, 2, eventCount, timeout);

        siddhiAppRuntime.shutdown();

        Assert.assertEquals(eventCount.intValue(), 2, "Read events failed");
    }

    @Test(expectedExceptions = SiddhiParserException.class)
    public void testMongoTableJoinQuery2() {
        log.info("testMongoTableJoinQuery2DASC5-916:Read events from a non existing MongoDB collection");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream FooStream (symbol string); " +
                "@store(type = 'mongodb' , mongodb.uri='" + uri + "')" +
                "define table FooTable (symbol string, price float, volume long);";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into FooTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from FooStream#window.length(1) join FooTable123 " +
                "select FooStream.symbol as checkSymbol, FooTable.symbol as symbol, " +
                "FooTable.volume as volume  " +
                "insert into OutputStream ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiParserException.class)
    public void testMongoTableJoinQuery3() {
        log.info("testMongoTableJoinQuery3 - " +
                "DASC5-917:Read events from a MongoDB collection by sending through non existing stream");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream FooStream (symbol string); " +
                "@store(type = 'mongodb' , mongodb.uri='" + uri + "')" +
                "define table FooTable (symbol string, price float, volume long);";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into FooTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from FooStream123#window.length(1) join FooTable " +
                "select FooStream.symbol as checkSymbol, FooTable.symbol as symbol, " +
                "FooTable.volume as volume  " +
                "insert into OutputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testMongoTableJoinQuery4() throws InterruptedException {
        log.info("testMongoTableJoinQuery4 - " +
                "DASC5-918:Read events from a MongoDB collection for less attributes than total attribute list");

        MongoTableTestUtils.dropCollection(uri, "FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream FooStream (symbol string); " +
                "@store(type = 'mongodb' , mongodb.uri='" + uri + "')" +
                "define table FooTable (symbol string, price float, volume long);";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into FooTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from FooStream#window.length(1) join FooTable " +
                "select FooStream.symbol as checkSymbol, FooTable.symbol as symbol " +
                "insert into OutputStream ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        eventCount.incrementAndGet();
                        switch (eventCount.intValue()) {
                            case 1:
                                Assert.assertEquals(new Object[]{"WSO2_check", "WSO2"}, event.getData());
                                break;
                            case 2:
                                Assert.assertEquals(new Object[]{"WSO2_check", "IBM"}, event.getData());
                                break;
                            default:
                                break;
                        }
                    }
                }
            }

        });

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 5.6f, 100L});
        stockStream.send(new Object[]{"IBM", 7.6f, 10L});
        fooStream.send(new Object[]{"WSO2_check"});
        SiddhiTestHelper.waitForEvents(waitTime, 2, eventCount, timeout);

        siddhiAppRuntime.shutdown();

        Assert.assertEquals(eventCount.intValue(), 2, "Read events failed");
    }

    @Test(expectedExceptions = SiddhiParserException.class)
    public void testMongoTableJoinQuery5() {
        log.info("testMongoTableJoinQuery5 - " +
                "DASC5-919:Read events from a MongoDB collection for non existing attributes");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream FooStream (symbol string); " +
                "@store(type = 'mongodb' , mongodb.uri='" + uri + "')" +
                "define table FooTable (symbol string, price float, volume long);";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into FooTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from FooStream123#window.length(1) join FooTable " +
                "select FooStream.hello as checkHello, FooTable.symbol as symbol, " +
                "FooTable.volume as volume  " +
                "insert into OutputStream ;";
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.start();
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testMongoTableJoinQuery6() throws InterruptedException {
        log.info("testMongoTableJoinQuery6");
        //Object reads

        MongoTableTestUtils.dropCollection(uri, "FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, input Object); " +
                "define stream FooStream (symbol string); " +
                "@store(type = 'mongodb' , mongodb.uri='" + uri + "')" +
                "define table FooTable (symbol string, price float, input Object);";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into FooTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from FooStream#window.length(1) join FooTable " +
                "select FooStream.symbol as checkSymbol, FooTable.symbol as symbol, " +
                "FooTable.input as input  " +
                "insert into OutputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        eventCount.incrementAndGet();
                        switch (eventCount.intValue()) {
                            case 1:
                                HashMap<String, String> input = new HashMap<>();
                                input.put("symbol", "IBM");
                                Assert.assertEquals(new Object[]{"WSO2_check", "WSO2", input}, event.getData());
                                break;
                            default:
                                break;
                        }
                    }
                }
            }

        });

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();

        HashMap<String, String> input = new HashMap<>();
        input.put("symbol", "IBM");
        stockStream.send(new Object[]{"WSO2", 5.6f, input});
        fooStream.send(new Object[]{"WSO2_check"});
        SiddhiTestHelper.waitForEvents(waitTime, 1, eventCount, timeout);

        siddhiAppRuntime.shutdown();

        Assert.assertEquals(eventCount.intValue(), 1, "Read events failed");
    }

    @Test
    public void testMongoTableJoinQuery7() throws InterruptedException {
        log.info("testMongoTableJoinQuery7");
        //Object reads

        MongoTableTestUtils.dropCollection(uri, "FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float); " +
                "define stream FooStream (symbol string, volume int, name string); " +
                "@store(type = 'mongodb' , mongodb.uri='" + uri + "')" +
                "define table FooTable (symbol string, price float);";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into FooTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from FooStream as s join FooTable as t " +
                "on s.symbol != t.symbol "+
                "select s.symbol as checkSymbol, t.symbol as tblSymbol, t.price as price, '100' as fixedVal, s.volume as streamVolume, s.name as streamName " +
                "having price < 12 " +
                "order by streamVolume, tblSymbol "+
                "limit 1 "+
                "offset 1 "+
                "insert into OutputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        eventCount.incrementAndGet();
                        switch (eventCount.intValue()) {
                            case 1:
                                Assert.assertEquals(new Object[]{"WSO2","IBM",8.5,100,"10","prabod"}, event.getData());
                                break;
                            default:
                                break;
                        }
                    }
                }
            }

        });

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 6.5f});
        stockStream.send(new Object[]{"WSO2", 6.5f});
        stockStream.send(new Object[]{"IBM", 9.5f});
        stockStream.send(new Object[]{"IBM", 8.5f});
        fooStream.send(new Object[]{"WSO2",10,"prabod"});
        SiddhiTestHelper.waitForEvents(waitTime, 1, eventCount, timeout);

        siddhiAppRuntime.shutdown();

        Assert.assertEquals(eventCount.intValue(), 1, "Read events failed");
    }

    @Test
    public void testMongoTableJoinQuery8() throws InterruptedException {
        log.info("testMongoTableJoinQuery8");

        MongoTableTestUtils.dropCollection(uri, "FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, weight int); " +
                "define stream FooStream (symbol string, volume int); " +
                "@store(type = 'mongodb' , mongodb.uri='" + uri + "')" +
                "define table FooTable (symbol string, price float, weight int);";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into FooTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from FooStream as s join FooTable as t " +
                "on s.symbol != t.symbol "+
                "select t.symbol, avg(t.weight) as avgweight, min(t.price) as minprice, max(t.price) as maxprice " +
                "group by t.price, t.symbol, t.weight " +
                "having maxprice > 5 "+
                "order by maxprice "+
                "limit 5 "+
                "offset 2 "+
                "insert into OutputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        eventCount.incrementAndGet();
                        switch (eventCount.intValue()) {
                            case 1:
                                Assert.assertEquals(new Object[]{"GOOGLE", 22.0, 13.0, 9.5, 12.5}, event.getData());
                                break;
                            default:
                                break;
                        }
                    }
                }
            }

        });

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"GOOGLE", 12.5f, 10});
        stockStream.send(new Object[]{"APPLE", 10.5f, 14});
        stockStream.send(new Object[]{"GOOGLE", 9.5f, 16});
        stockStream.send(new Object[]{"IBM", 8.5f, 18});
        fooStream.send(new Object[]{"WSO2",10});
        SiddhiTestHelper.waitForEvents(waitTime, 1, eventCount, timeout);

        siddhiAppRuntime.shutdown();

        Assert.assertEquals(eventCount.intValue(), 1, "Read events failed");
    }
}
