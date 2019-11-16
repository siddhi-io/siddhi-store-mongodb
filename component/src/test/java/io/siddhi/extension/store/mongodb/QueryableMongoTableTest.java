/*
 * Copyright (c) 2019, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
package io.siddhi.extension.store.mongodb;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.query.output.callback.QueryCallback;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.util.SiddhiTestHelper;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.atomic.AtomicInteger;

public class QueryableMongoTableTest {

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
    public void testMongoTableQuery1() throws InterruptedException {
        log.info("testMongoTableQuery1 : Test selection of store attributes, stream attributes and constants");

        MongoTableTestUtils.dropCollection(uri, "FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, amount int); " +
                "define stream FooStream (symbol string, volume int); " +
                "@store(type = 'mongodb' , mongodb.uri='" + uri + "')" +
                "define table FooTable (symbol string, amount int);";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into FooTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from FooStream as s join FooTable as t " +
                "on s.symbol == t.symbol " +
                "select t.symbol as tblSymbol, t.amount as tblAmount, s.volume as streamVolume, '100' as fixedValue " +
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
                                Assert.assertEquals(new Object[]{"WSO2", 100, 10, 100}, event.getData());
                                break;
                            case 2:
                                Assert.assertEquals(new Object[]{"WSO2", 120, 10, 100}, event.getData());
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

        stockStream.send(new Object[]{"WSO2", 100});
        stockStream.send(new Object[]{"WSO2", 120});
        stockStream.send(new Object[]{"IBM", 160});
        fooStream.send(new Object[]{"WSO2", 10});
        SiddhiTestHelper.waitForEvents(waitTime, 2, eventCount, timeout);

        siddhiAppRuntime.shutdown();

        Assert.assertEquals(eventCount.intValue(), 2, "Read events failed");
    }

    @Test
    public void testMongoTableQuery2() throws InterruptedException {
        log.info("testMongoTableQuery2 : Test inline math operators in select attributes");

        MongoTableTestUtils.dropCollection(uri, "FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float); " +
                "define stream FooStream (symbol string, volume int); " +
                "@store(type = 'mongodb' , mongodb.uri='" + uri + "')" +
                "define table FooTable (symbol string, price float);";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into FooTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from FooStream as s join FooTable as t " +
                "select t.symbol as tblSymbol, t.price * s.volume as totalCost, t.price * 10 / 100 as discount " +
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
                                Assert.assertEquals(new Object[]{"WSO2", 65.0, 0.65}, event.getData());
                                break;
                            case 2:
                                Assert.assertEquals(new Object[]{"IBM", 95.0, 0.95}, event.getData());
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
        stockStream.send(new Object[]{"IBM", 9.5f});
        fooStream.send(new Object[]{"WSO2", 10});
        SiddhiTestHelper.waitForEvents(waitTime, 2, eventCount, timeout);

        siddhiAppRuntime.shutdown();

        Assert.assertEquals(eventCount.intValue(), 2, "Read events failed");
    }

    @Test
    public void testMongoTableQuery3() throws InterruptedException {
        log.info("testMongoTableQuery3 : Test logical operators in select attributes");

        MongoTableTestUtils.dropCollection(uri, "FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, isFraud bool); " +
                "define stream FooStream (symbol string, isChecked bool, isOlderStock bool); " +
                "@store(type = 'mongodb' , mongodb.uri='" + uri + "')" +
                "define table FooTable (symbol string, isFraud bool);";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into FooTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from FooStream as s join FooTable as t " +
                "on s.symbol != t.symbol " +
                "select t.symbol as tblSymbol, (t.isFraud or s.isChecked and s.isOlderStock) as isCheckedData " +
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
                                Assert.assertEquals(new Object[]{"LINUX", true}, event.getData());
                                break;
                            case 2:
                                Assert.assertEquals(new Object[]{"GOOGLE", false}, event.getData());
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

        stockStream.send(new Object[]{"LINUX", true});
        stockStream.send(new Object[]{"GOOGLE", false});
        fooStream.send(new Object[]{"WSO2", true, false});
        SiddhiTestHelper.waitForEvents(waitTime, 2, eventCount, timeout);

        siddhiAppRuntime.shutdown();

        Assert.assertEquals(eventCount.intValue(), 2, "Read events failed");
    }

    @Test
    public void testMongoTableQuery4() throws InterruptedException {
        log.info("testMongoTableQuery4 : Test having condition for store attributes");

        MongoTableTestUtils.dropCollection(uri, "FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, amount int); " +
                "define stream FooStream (symbol string, volume int); " +
                "@store(type = 'mongodb' , mongodb.uri='" + uri + "')" +
                "define table FooTable (symbol string, amount int);";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into FooTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from FooStream as s join FooTable as t " +
                "select t.symbol as tblSymbol, t.amount as tblAmount, s.volume as streamVolume " +
                "having tblAmount > 110 " +
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
                                Assert.assertEquals(new Object[]{"WSO2", 120, 10}, event.getData());
                                break;
                            case 2:
                                Assert.assertEquals(new Object[]{"IBM", 160, 10}, event.getData());
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

        stockStream.send(new Object[]{"WSO2", 100});
        stockStream.send(new Object[]{"WSO2", 120});
        stockStream.send(new Object[]{"IBM", 160});
        fooStream.send(new Object[]{"WSO2", 10});
        SiddhiTestHelper.waitForEvents(waitTime, 2, eventCount, timeout);

        siddhiAppRuntime.shutdown();

        Assert.assertEquals(eventCount.intValue(), 2, "Read events failed");
    }

    @Test
    public void testMongoTableQuery5() throws InterruptedException {
        log.info("testMongoTableQuery5 : Test having condition with logical operators in between");

        MongoTableTestUtils.dropCollection(uri, "FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, amount int); " +
                "define stream FooStream (symbol string, volume int); " +
                "@store(type = 'mongodb' , mongodb.uri='" + uri + "')" +
                "define table FooTable (symbol string, amount int);";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into FooTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from FooStream as s join FooTable as t " +
                "select t.symbol as tblSymbol, t.amount as tblAmount, s.volume as streamVolume " +
                "having tblAmount > 110 and tblAmount < 160 and streamVolume <= 15 " +
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
                                Assert.assertEquals(new Object[]{"WSO2", 120, 10}, event.getData());
                                break;
                            case 2:
                                Assert.assertEquals(new Object[]{"IBM", 150, 10}, event.getData());
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

        stockStream.send(new Object[]{"WSO2", 100});
        stockStream.send(new Object[]{"WSO2", 120});
        stockStream.send(new Object[]{"WSO2", 170});
        stockStream.send(new Object[]{"IBM", 150});
        fooStream.send(new Object[]{"WSO2", 10});
        SiddhiTestHelper.waitForEvents(waitTime, 2, eventCount, timeout);

        siddhiAppRuntime.shutdown();

        Assert.assertEquals(eventCount.intValue(), 2, "Read events failed");
    }

    @Test
    public void testMongoTableQuery6() throws InterruptedException {
        log.info("testMongoTableQuery6 : Test limit condition");

        MongoTableTestUtils.dropCollection(uri, "FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, amount int); " +
                "define stream FooStream (symbol string, volume int); " +
                "@store(type = 'mongodb' , mongodb.uri='" + uri + "')" +
                "define table FooTable (symbol string, amount int);";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into FooTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from FooStream as s join FooTable as t " +
                "select t.symbol as tblSymbol, t.amount as tblAmount, s.volume as streamVolume " +
                "limit 3 " +
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
                                Assert.assertEquals(new Object[]{"WSO2", 100, 10}, event.getData());
                                break;
                            case 2:
                                Assert.assertEquals(new Object[]{"GOOGLE", 120, 10}, event.getData());
                                break;
                            case 3:
                                Assert.assertEquals(new Object[]{"IBM", 160, 10}, event.getData());
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

        stockStream.send(new Object[]{"WSO2", 100});
        stockStream.send(new Object[]{"GOOGLE", 120});
        stockStream.send(new Object[]{"IBM", 160});
        stockStream.send(new Object[]{"WINDOWS", 120});
        stockStream.send(new Object[]{"LINUX", 160});
        fooStream.send(new Object[]{"WSO2", 10});
        SiddhiTestHelper.waitForEvents(waitTime, 3, eventCount, timeout);

        siddhiAppRuntime.shutdown();

        Assert.assertEquals(eventCount.intValue(), 3, "Read events failed");
    }

    @Test
    public void testMongoTableQuery7() throws InterruptedException {
        log.info("testMongoTableQuery7 : Test offset condition");

        MongoTableTestUtils.dropCollection(uri, "FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, amount int); " +
                "define stream FooStream (symbol string, volume int); " +
                "@store(type = 'mongodb' , mongodb.uri='" + uri + "')" +
                "define table FooTable (symbol string, amount int);";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into FooTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from FooStream as s join FooTable as t " +
                "select t.symbol as tblSymbol, t.amount as tblAmount, s.volume as streamVolume " +
                "offset 3 " +
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
                                Assert.assertEquals(new Object[]{"WINDOWS", 120, 10}, event.getData());
                                break;
                            case 2:
                                Assert.assertEquals(new Object[]{"LINUX", 160, 10}, event.getData());
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

        stockStream.send(new Object[]{"WSO2", 100});
        stockStream.send(new Object[]{"GOOGLE", 120});
        stockStream.send(new Object[]{"IBM", 160});
        stockStream.send(new Object[]{"WINDOWS", 120});
        stockStream.send(new Object[]{"LINUX", 160});
        fooStream.send(new Object[]{"WSO2", 10});
        SiddhiTestHelper.waitForEvents(waitTime, 2, eventCount, timeout);

        siddhiAppRuntime.shutdown();

        Assert.assertEquals(eventCount.intValue(), 2, "Read events failed");
    }

    @Test
    public void testMongoTableQuery8() throws InterruptedException {
        log.info("testMongoTableQuery8 : Test orderBy condition using single attribute");

        MongoTableTestUtils.dropCollection(uri, "FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, amount int); " +
                "define stream FooStream (symbol string); " +
                "@store(type = 'mongodb' , mongodb.uri='" + uri + "')" +
                "define table FooTable (symbol string, price float, amount int);";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into FooTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from FooStream as s join FooTable as t " +
                "on s.symbol != t.symbol "+
                "select t.symbol, t.price, t.amount " +
                "order by t.amount DESC " +
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
                                Assert.assertEquals(new Object[]{"IBM", 8.5, 160}, event.getData());
                                break;
                            case 2:
                                Assert.assertEquals(new Object[]{"GOOGLE", 12.5, 120}, event.getData());
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

        stockStream.send(new Object[]{"WSO2", 9.5f, 100});
        stockStream.send(new Object[]{"GOOGLE", 12.5f, 120});
        stockStream.send(new Object[]{"IBM", 8.5f, 160});
        fooStream.send(new Object[]{"WSO2"});
        SiddhiTestHelper.waitForEvents(waitTime, 2, eventCount, timeout);

        siddhiAppRuntime.shutdown();

        Assert.assertEquals(eventCount.intValue(), 2, "Read events failed");
    }

    @Test
    public void testMongoTableQuery9() throws InterruptedException {
        log.info("testMongoTableQuery9 : Test orderBy condition using multiple attributes");

        MongoTableTestUtils.dropCollection(uri, "FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, amount int); " +
                "define stream FooStream (symbol string); " +
                "@store(type = 'mongodb' , mongodb.uri='" + uri + "')" +
                "define table FooTable (symbol string, price float, amount int);";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into FooTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from FooStream as s join FooTable as t " +
                "on s.symbol != t.symbol "+
                "select t.symbol, t.price, t.amount " +
                "order by t.symbol, t.amount DESC " +
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
                                Assert.assertEquals(new Object[]{"GOOGLE", 12.5, 130}, event.getData());
                                break;
                            case 2:
                                Assert.assertEquals(new Object[]{"GOOGLE", 10.5, 120}, event.getData());
                                break;
                            case 3:
                                Assert.assertEquals(new Object[]{"IBM", 8.5, 160}, event.getData());
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

        stockStream.send(new Object[]{"WSO2", 9.5f, 100});
        stockStream.send(new Object[]{"GOOGLE", 10.5f, 120});
        stockStream.send(new Object[]{"IBM", 8.5f, 160});
        stockStream.send(new Object[]{"GOOGLE", 12.5f, 130});
        fooStream.send(new Object[]{"WSO2"});
        SiddhiTestHelper.waitForEvents(waitTime, 3, eventCount, timeout);

        siddhiAppRuntime.shutdown();

        Assert.assertEquals(eventCount.intValue(), 3, "Read events failed");
    }
}
