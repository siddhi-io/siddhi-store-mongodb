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
import io.siddhi.core.util.EventPrinter;
import io.siddhi.core.util.SiddhiTestHelper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class QueryableMongoTableTest {

    private static final Logger log = LogManager.getLogger(JoinMongoTableTest.class);
    private AtomicInteger inEventCount;
    private boolean eventArrived;
    private List<Object[]> inEventsList;
    private static String uri = MongoTableTestUtils.resolveBaseUri();
    private int waitTime = 2000;
    private int timeout = 30000;

    @BeforeClass
    public void init() {
        log.info("== Mongo Table query tests started ==");
        inEventCount = new AtomicInteger();
        eventArrived = false;
        inEventsList = new ArrayList<>();
    }

    @AfterClass
    public void shutdown() {
        log.info("== Mongo Table query tests completed ==");
    }

    @BeforeMethod
    public void testInit() {
        inEventCount.set(0);
    }

    @AfterMethod
    public void testEnd() {
        inEventsList.clear();
    }

    @Test
    public void testMongoTableQuery1() throws InterruptedException {
        log.info("testMongoTableQuery1 : Test selection of store attributes, stream attributes and constants.");

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
                    EventPrinter.print(timeStamp, inEvents, removeEvents);
                    for (Event event : inEvents) {
                        inEventsList.add(event.getData());
                        inEventCount.incrementAndGet();
                    }
                    eventArrived = true;
                }
                eventArrived = true;
            }
        });

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 100});
        stockStream.send(new Object[]{"WSO2", 120});
        stockStream.send(new Object[]{"IBM", 160});
        fooStream.send(new Object[]{"WSO2", 10});
        SiddhiTestHelper.waitForEvents(waitTime, 2, inEventCount, timeout);

        siddhiAppRuntime.shutdown();

        List<Object[]> expected = Arrays.asList(
                new Object[]{"WSO2", 100, 10, 100},
                new Object[]{"WSO2", 120, 10, 100}
        );

        AssertJUnit.assertTrue("Event arrived", eventArrived);
        AssertJUnit.assertEquals("Number of success events", 2, inEventCount.get());
        AssertJUnit.assertTrue("In events matched", SiddhiTestHelper.isUnsortedEventsMatch(inEventsList, expected));
    }

    @Test
    public void testMongoTableQuery2() throws InterruptedException {
        log.info("testMongoTableQuery2 : Test inline math operators in select attributes.");

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
                "select t.symbol as tblSymbol, t.price * s.volume as totalCost, " +
                "t.price - (10 / 100) as discountedPrice " +
                "insert into OutputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                if (inEvents != null) {
                    EventPrinter.print(timeStamp, inEvents, removeEvents);
                    for (Event event : inEvents) {
                        inEventsList.add(event.getData());
                        inEventCount.incrementAndGet();
                    }
                    eventArrived = true;
                }
                eventArrived = true;
            }
        });

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 6.5f});
        stockStream.send(new Object[]{"IBM", 9.5f});
        fooStream.send(new Object[]{"WSO2", 10});
        SiddhiTestHelper.waitForEvents(waitTime, 2, inEventCount, timeout);

        siddhiAppRuntime.shutdown();

        List<Object[]> expected = Arrays.asList(
                new Object[]{"WSO2", 65.0, 6.4},
                new Object[]{"IBM", 95.0, 9.4}
        );

        AssertJUnit.assertTrue("Event arrived", eventArrived);
        AssertJUnit.assertEquals("Number of success events", 2, inEventCount.get());
        AssertJUnit.assertTrue("In events matched", SiddhiTestHelper.isUnsortedEventsMatch(inEventsList, expected));
    }

    @Test
    public void testMongoTableQuery3() throws InterruptedException {
        log.info("testMongoTableQuery3 : Test logical operators in select attributes-1.");

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
                "select t.symbol as tblSymbol, (t.isFraud and s.isChecked) as isCheckedData " +
                "insert into OutputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                if (inEvents != null) {
                    EventPrinter.print(timeStamp, inEvents, removeEvents);
                    for (Event event : inEvents) {
                        inEventsList.add(event.getData());
                        inEventCount.incrementAndGet();
                    }
                    eventArrived = true;
                }
                eventArrived = true;
            }
        });

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"LINUX", true});
        stockStream.send(new Object[]{"GOOGLE", false});
        fooStream.send(new Object[]{"WSO2", true, false});
        SiddhiTestHelper.waitForEvents(waitTime, 2, inEventCount, timeout);

        siddhiAppRuntime.shutdown();

        List<Object[]> expected = Arrays.asList(
                new Object[]{"LINUX", true},
                new Object[]{"GOOGLE", false}
        );

        AssertJUnit.assertTrue("Event arrived", eventArrived);
        AssertJUnit.assertEquals("Number of success events", 2, inEventCount.get());
        AssertJUnit.assertTrue("In events matched", SiddhiTestHelper.isUnsortedEventsMatch(inEventsList, expected));
    }

    @Test
    public void testMongoTableQuery4() throws InterruptedException {
        log.info("testMongoTableQuery4 : Test logical operators in select attributes-2.");

        MongoTableTestUtils.dropCollection(uri, "FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price int, isFraud bool, isNewStock bool); " +
                "define stream FooStream (symbol string, isChecked bool, isOlderStock bool); " +
                "@store(type = 'mongodb' , mongodb.uri='" + uri + "')" +
                "define table FooTable (symbol string, price int, isFraud bool, isNewStock bool);";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into FooTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from FooStream as s join FooTable as t " +
                "on s.symbol != t.symbol " +
                "select t.symbol as tblSymbol, (t.isFraud or s.isChecked) and (s.isOlderStock and t.isNewStock) as " +
                "isCheckedData " +
                "insert into OutputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                if (inEvents != null) {
                    EventPrinter.print(timeStamp, inEvents, removeEvents);
                    for (Event event : inEvents) {
                        inEventsList.add(event.getData());
                        inEventCount.incrementAndGet();
                    }
                    eventArrived = true;
                }
                eventArrived = true;
            }
        });

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"LINUX", 10, true, false});
        stockStream.send(new Object[]{"GOOGLE", 12, false, true});
        fooStream.send(new Object[]{"WSO2", true, false});
        SiddhiTestHelper.waitForEvents(waitTime, 2, inEventCount, timeout);

        siddhiAppRuntime.shutdown();

        List<Object[]> expected = Arrays.asList(
                new Object[]{"LINUX", false},
                new Object[]{"GOOGLE", false}
        );

        AssertJUnit.assertTrue("Event arrived", eventArrived);
        AssertJUnit.assertEquals("Number of success events", 2, inEventCount.get());
        AssertJUnit.assertTrue("In events matched", SiddhiTestHelper.isUnsortedEventsMatch(inEventsList, expected));
    }

    @Test
    public void testMongoTableQuery5() throws InterruptedException {
        log.info("testMongoTableQuery5 : Test having condition for store attributes.");

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
                    EventPrinter.print(timeStamp, inEvents, removeEvents);
                    for (Event event : inEvents) {
                        inEventsList.add(event.getData());
                        inEventCount.incrementAndGet();
                    }
                    eventArrived = true;
                }
                eventArrived = true;
            }
        });

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 100});
        stockStream.send(new Object[]{"WSO2", 120});
        stockStream.send(new Object[]{"IBM", 160});
        fooStream.send(new Object[]{"WSO2", 10});
        SiddhiTestHelper.waitForEvents(waitTime, 2, inEventCount, timeout);

        siddhiAppRuntime.shutdown();

        List<Object[]> expected = Arrays.asList(
                new Object[]{"WSO2", 120, 10},
                new Object[]{"IBM", 160, 10}
        );

        AssertJUnit.assertTrue("Event arrived", eventArrived);
        AssertJUnit.assertEquals("Number of success events", 2, inEventCount.get());
        AssertJUnit.assertTrue("In events matched", SiddhiTestHelper.isUnsortedEventsMatch(inEventsList, expected));
    }

    @Test
    public void testMongoTableQuery6() throws InterruptedException {
        log.info("testMongoTableQuery6 : Test having condition with logical operators in between.");

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
                    EventPrinter.print(timeStamp, inEvents, removeEvents);
                    for (Event event : inEvents) {
                        inEventsList.add(event.getData());
                        inEventCount.incrementAndGet();
                    }
                    eventArrived = true;
                }
                eventArrived = true;
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
        SiddhiTestHelper.waitForEvents(waitTime, 2, inEventCount, timeout);

        siddhiAppRuntime.shutdown();

        List<Object[]> expected = Arrays.asList(
                new Object[]{"WSO2", 120, 10},
                new Object[]{"IBM", 150, 10}
        );

        AssertJUnit.assertTrue("Event arrived", eventArrived);
        AssertJUnit.assertEquals("Number of success events", 2, inEventCount.get());
        AssertJUnit.assertTrue("In events matched", SiddhiTestHelper.isUnsortedEventsMatch(inEventsList, expected));
    }

    @Test
    public void testMongoTableQuery7() throws InterruptedException {
        log.info("testMongoTableQuery7 : Test limit condition.");

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
                    EventPrinter.print(timeStamp, inEvents, removeEvents);
                    for (Event event : inEvents) {
                        inEventsList.add(event.getData());
                        inEventCount.incrementAndGet();
                    }
                    eventArrived = true;
                }
                eventArrived = true;
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
        SiddhiTestHelper.waitForEvents(waitTime, 3, inEventCount, timeout);

        siddhiAppRuntime.shutdown();

        List<Object[]> expected = Arrays.asList(
                new Object[]{"WSO2", 100, 10},
                new Object[]{"GOOGLE", 120, 10},
                new Object[]{"IBM", 160, 10}
        );

        AssertJUnit.assertTrue("Event arrived", eventArrived);
        AssertJUnit.assertEquals("Number of success events", 3, inEventCount.get());
        AssertJUnit.assertTrue("In events matched", SiddhiTestHelper.isUnsortedEventsMatch(inEventsList, expected));
    }

    @Test
    public void testMongoTableQuery8() throws InterruptedException {
        log.info("testMongoTableQuery8 : Test offset condition.");

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
                    EventPrinter.print(timeStamp, inEvents, removeEvents);
                    for (Event event : inEvents) {
                        inEventsList.add(event.getData());
                        inEventCount.incrementAndGet();
                    }
                    eventArrived = true;
                }
                eventArrived = true;
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
        SiddhiTestHelper.waitForEvents(waitTime, 2, inEventCount, timeout);

        siddhiAppRuntime.shutdown();

        List<Object[]> expected = Arrays.asList(
                new Object[]{"WINDOWS", 120, 10},
                new Object[]{"LINUX", 160, 10}
        );

        AssertJUnit.assertTrue("Event arrived", eventArrived);
        AssertJUnit.assertEquals("Number of success events", 2, inEventCount.get());
        AssertJUnit.assertTrue("In events matched", SiddhiTestHelper.isUnsortedEventsMatch(inEventsList, expected));
    }

    @Test
    public void testMongoTableQuery9() throws InterruptedException {
        log.info("testMongoTableQuery9 : Test orderBy condition using single attribute.");

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
                "on s.symbol != t.symbol " +
                "select t.symbol, t.price, t.amount " +
                "order by t.amount DESC " +
                "insert into OutputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                if (inEvents != null) {
                    EventPrinter.print(timeStamp, inEvents, removeEvents);
                    for (Event event : inEvents) {
                        inEventsList.add(event.getData());
                        inEventCount.incrementAndGet();
                    }
                    eventArrived = true;
                }
                eventArrived = true;
            }
        });

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 9.5f, 100});
        stockStream.send(new Object[]{"GOOGLE", 12.5f, 120});
        stockStream.send(new Object[]{"IBM", 8.5f, 160});
        fooStream.send(new Object[]{"WSO2"});
        SiddhiTestHelper.waitForEvents(waitTime, 2, inEventCount, timeout);

        siddhiAppRuntime.shutdown();

        List<Object[]> expected = Arrays.asList(
                new Object[]{"IBM", 8.5, 160},
                new Object[]{"GOOGLE", 12.5, 120}
        );

        AssertJUnit.assertTrue("Event arrived", eventArrived);
        AssertJUnit.assertEquals("Number of success events", 2, inEventCount.get());
        AssertJUnit.assertTrue("In events matched", SiddhiTestHelper.isUnsortedEventsMatch(inEventsList, expected));
    }

    @Test
    public void testMongoTableQuery10() throws InterruptedException {
        log.info("testMongoTableQuery10 : Test orderBy condition using multiple attributes.");

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
                "on s.symbol != t.symbol " +
                "select t.symbol, t.price, t.amount " +
                "order by t.symbol, t.amount DESC " +
                "insert into OutputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                if (inEvents != null) {
                    EventPrinter.print(timeStamp, inEvents, removeEvents);
                    for (Event event : inEvents) {
                        inEventsList.add(event.getData());
                        inEventCount.incrementAndGet();
                    }
                    eventArrived = true;
                }
                eventArrived = true;
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
        SiddhiTestHelper.waitForEvents(waitTime, 3, inEventCount, timeout);

        siddhiAppRuntime.shutdown();

        List<Object[]> expected = Arrays.asList(
                new Object[]{"GOOGLE", 12.5, 130},
                new Object[]{"GOOGLE", 10.5, 120},
                new Object[]{"IBM", 8.5, 160}
        );

        AssertJUnit.assertTrue("Event arrived", eventArrived);
        AssertJUnit.assertEquals("Number of success events", 3, inEventCount.get());
        AssertJUnit.assertTrue("In events matched", SiddhiTestHelper.isUnsortedEventsMatch(inEventsList, expected));
    }

    @Test
    public void testMongoTableQuery11() throws InterruptedException {
        log.info("testMongoTableQuery11 : Test groupBy with sum function.");

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
                "select t.symbol, t.price, sum(t.price) as sumPrice " +
                "group by t.price " +
                "order by t.symbol " +
                "insert into OutputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                if (inEvents != null) {
                    EventPrinter.print(timeStamp, inEvents, removeEvents);
                    for (Event event : inEvents) {
                        inEventsList.add(event.getData());
                        inEventCount.incrementAndGet();
                    }
                    eventArrived = true;
                }
                eventArrived = true;
            }
        });

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"GOOGLE", 12.5f, 10});
        stockStream.send(new Object[]{"APPLE", 10.5f, 12});
        stockStream.send(new Object[]{"IBM", 12.5f, 16});
        fooStream.send(new Object[]{"GOOGLE", 10});
        SiddhiTestHelper.waitForEvents(waitTime, 2, inEventCount, timeout);

        siddhiAppRuntime.shutdown();

        List<Object[]> expected = Arrays.asList(
                new Object[]{"APPLE", 10.5, 10.5},
                new Object[]{"IBM", 12.5, 25.0}
        );

        AssertJUnit.assertTrue("Event arrived", eventArrived);
        AssertJUnit.assertEquals("Number of success events", 2, inEventCount.get());
        AssertJUnit.assertTrue("In events matched", SiddhiTestHelper.isUnsortedEventsMatch(inEventsList, expected));
    }

    @Test
    public void testMongoTableQuery12() throws InterruptedException {
        log.info("testMongoTableQuery12 : Test groupBy with count function.");

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
                "select t.symbol, count() as countRecords " +
                "group by t.price " +
                "order by t.symbol " +
                "insert into OutputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                if (inEvents != null) {
                    EventPrinter.print(timeStamp, inEvents, removeEvents);
                    for (Event event : inEvents) {
                        inEventsList.add(event.getData());
                        inEventCount.incrementAndGet();
                    }
                    eventArrived = true;
                }
                eventArrived = true;
            }
        });

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"GOOGLE", 12.5f, 10});
        stockStream.send(new Object[]{"APPLE", 10.5f, 12});
        stockStream.send(new Object[]{"IBM", 12.5f, 16});
        fooStream.send(new Object[]{"GOOGLE", 10});
        SiddhiTestHelper.waitForEvents(waitTime, 2, inEventCount, timeout);

        siddhiAppRuntime.shutdown();

        List<Object[]> expected = Arrays.asList(
                new Object[]{"APPLE", 1},
                new Object[]{"IBM", 2}
        );

        AssertJUnit.assertTrue("Event arrived", eventArrived);
        AssertJUnit.assertEquals("Number of success events", 2, inEventCount.get());
        AssertJUnit.assertTrue("In events matched", SiddhiTestHelper.isUnsortedEventsMatch(inEventsList, expected));
    }

    @Test
    public void testMongoTableQuery13() throws InterruptedException {
        log.info("testMongoTableQuery13 : Test groupBy using multiple attributes.");

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
                "select t.symbol, avg(t.weight) as avgWeight, count() as countRecords " +
                "group by t.symbol, t.price " +
                "order by t.symbol " +
                "insert into OutputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                if (inEvents != null) {
                    EventPrinter.print(timeStamp, inEvents, removeEvents);
                    for (Event event : inEvents) {
                        inEventsList.add(event.getData());
                        inEventCount.incrementAndGet();
                    }
                    eventArrived = true;
                }
                eventArrived = true;
            }
        });

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"GOOGLE", 12.5f, 10});
        stockStream.send(new Object[]{"APPLE", 10.5f, 12});
        stockStream.send(new Object[]{"IBM", 12.5f, 16});
        stockStream.send(new Object[]{"GOOGLE", 12.5f, 12});
        fooStream.send(new Object[]{"GOOGLE", 10});
        SiddhiTestHelper.waitForEvents(waitTime, 3, inEventCount, timeout);

        siddhiAppRuntime.shutdown();

        List<Object[]> expected = Arrays.asList(
                new Object[]{"APPLE", 12.0, 1},
                new Object[]{"GOOGLE", 11.0, 2},
                new Object[]{"IBM", 16.0, 1}
        );

        AssertJUnit.assertTrue("Event arrived", eventArrived);
        AssertJUnit.assertEquals("Number of success events", 3, inEventCount.get());
        AssertJUnit.assertTrue("In events matched", SiddhiTestHelper.isUnsortedEventsMatch(inEventsList, expected));
    }

    @Test
    public void testMongoTableQuery14() throws InterruptedException {
        log.info("testMongoTableQuery14 : Test isNull for store attributes.");

        MongoTableTestUtils.dropCollection(uri, "FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, isChecked bool); " +
                "define stream FooStream (symbol string); " +
                "@store(type = 'mongodb' , mongodb.uri='" + uri + "')" +
                "define table FooTable (symbol string, isChecked bool);";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into FooTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from FooStream as s join FooTable as t " +
                "select t.symbol as tblSymbol, t.isChecked is null as nullCheck " +
                "insert into OutputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                if (inEvents != null) {
                    EventPrinter.print(timeStamp, inEvents, removeEvents);
                    for (Event event : inEvents) {
                        inEventsList.add(event.getData());
                        inEventCount.incrementAndGet();
                    }
                    eventArrived = true;
                }
                eventArrived = true;
            }
        });

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", null});
        stockStream.send(new Object[]{"GOOGLE", true});
        stockStream.send(new Object[]{"IBM", false});
        stockStream.send(new Object[]{"LINUX", null});
        fooStream.send(new Object[]{"WSO2"});
        SiddhiTestHelper.waitForEvents(waitTime, 4, inEventCount, timeout);

        siddhiAppRuntime.shutdown();

        List<Object[]> expected = Arrays.asList(
                new Object[]{"WSO2", true},
                new Object[]{"GOOGLE", false},
                new Object[]{"IBM", false},
                new Object[]{"LINUX", true}
        );

        AssertJUnit.assertTrue("Event arrived", eventArrived);
        AssertJUnit.assertEquals("Number of success events", 4, inEventCount.get());
        AssertJUnit.assertTrue("In events matched", SiddhiTestHelper.isUnsortedEventsMatch(inEventsList, expected));
    }

    @Test
    public void testMongoTableQuery15() throws InterruptedException {
        log.info("testMongoTableQuery15 : Test comparisons in selection for store attributes.");

        MongoTableTestUtils.dropCollection(uri, "FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, stockOnePrice int, stockTwoPrice int); " +
                "define stream FooStream (symbol string, newStockPrice int); " +
                "@store(type = 'mongodb' , mongodb.uri='" + uri + "')" +
                "define table FooTable (symbol string, stockOnePrice int, stockTwoPrice int);";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into FooTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from FooStream as s join FooTable as t " +
                "select t.symbol as tblSymbol, stockOnePrice < stockTwoPrice as isStockOnePriceLow " +
                "insert into OutputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                if (inEvents != null) {
                    EventPrinter.print(timeStamp, inEvents, removeEvents);
                    for (Event event : inEvents) {
                        inEventsList.add(event.getData());
                        inEventCount.incrementAndGet();
                    }
                    eventArrived = true;
                }
                eventArrived = true;
            }
        });

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 10, 10});
        stockStream.send(new Object[]{"GOOGLE", 10, 20});
        stockStream.send(new Object[]{"IBM", 10, 5});
        fooStream.send(new Object[]{"WSO2", 20});
        SiddhiTestHelper.waitForEvents(waitTime, 3, inEventCount, timeout);

        siddhiAppRuntime.shutdown();

        List<Object[]> expected = Arrays.asList(
                new Object[]{"WSO2", false},
                new Object[]{"GOOGLE", true},
                new Object[]{"IBM", false}
        );

        AssertJUnit.assertTrue("Event arrived", eventArrived);
        AssertJUnit.assertEquals("Number of success events", 3, inEventCount.get());
        AssertJUnit.assertTrue("In events matched", SiddhiTestHelper.isUnsortedEventsMatch(inEventsList, expected));
    }

    @Test
    public void testMongoTableQuery16() throws InterruptedException {
        log.info("testMongoTableQuery16 : Test comparisons in selection for store and stream attributes.");

        MongoTableTestUtils.dropCollection(uri, "FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, stockOnePrice int); " +
                "define stream FooStream (symbol string, newStockPrice int); " +
                "@store(type = 'mongodb' , mongodb.uri='" + uri + "')" +
                "define table FooTable (symbol string, stockOnePrice int);";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into FooTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from FooStream as s join FooTable as t " +
                "select t.symbol as tblSymbol, stockOnePrice >= newStockPrice as isStockOnePriceLow " +
                "insert into OutputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                if (inEvents != null) {
                    EventPrinter.print(timeStamp, inEvents, removeEvents);
                    for (Event event : inEvents) {
                        inEventsList.add(event.getData());
                        inEventCount.incrementAndGet();
                    }
                    eventArrived = true;
                }
                eventArrived = true;
            }
        });

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 10});
        stockStream.send(new Object[]{"GOOGLE", 20});
        stockStream.send(new Object[]{"IBM", 30});
        fooStream.send(new Object[]{"WSO2", 20});
        SiddhiTestHelper.waitForEvents(waitTime, 3, inEventCount, timeout);

        siddhiAppRuntime.shutdown();

        List<Object[]> expected = Arrays.asList(
                new Object[]{"WSO2", false},
                new Object[]{"GOOGLE", true},
                new Object[]{"IBM", true}
        );

        AssertJUnit.assertTrue("Event arrived", eventArrived);
        AssertJUnit.assertEquals("Number of success events", 3, inEventCount.get());
        AssertJUnit.assertTrue("In events matched", SiddhiTestHelper.isUnsortedEventsMatch(inEventsList, expected));
    }

    @Test
    public void testMongoTableQuery17() throws InterruptedException {
        log.info("testMongoTableQuery17 : Test comparisons in selection for store attributes and constants.");

        MongoTableTestUtils.dropCollection(uri, "FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, stockOnePrice int); " +
                "define stream FooStream (symbol string); " +
                "@store(type = 'mongodb' , mongodb.uri='" + uri + "')" +
                "define table FooTable (symbol string, stockOnePrice int);";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into FooTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from FooStream as s join FooTable as t " +
                "select t.symbol as tblSymbol, stockOnePrice <= 20 as isStockOnePriceLow " +
                "insert into OutputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                if (inEvents != null) {
                    EventPrinter.print(timeStamp, inEvents, removeEvents);
                    for (Event event : inEvents) {
                        inEventsList.add(event.getData());
                        inEventCount.incrementAndGet();
                    }
                    eventArrived = true;
                }
                eventArrived = true;
            }
        });

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 10});
        stockStream.send(new Object[]{"GOOGLE", 20});
        stockStream.send(new Object[]{"IBM", 30});
        fooStream.send(new Object[]{"WSO2"});
        SiddhiTestHelper.waitForEvents(waitTime, 3, inEventCount, timeout);

        siddhiAppRuntime.shutdown();

        List<Object[]> expected = Arrays.asList(
                new Object[]{"WSO2", true},
                new Object[]{"GOOGLE", true},
                new Object[]{"IBM", false}
        );

        AssertJUnit.assertTrue("Event arrived", eventArrived);
        AssertJUnit.assertEquals("Number of success events", 3, inEventCount.get());
        AssertJUnit.assertTrue("In events matched", SiddhiTestHelper.isUnsortedEventsMatch(inEventsList, expected));
    }

    @Test
    public void testMongoTableQuery18() throws InterruptedException {
        log.info("testMongoTableQuery18 : Test selection attribute when rename is not used.");

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
                "select t.symbol, t.amount, s.volume " +
                "insert into OutputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                if (inEvents != null) {
                    EventPrinter.print(timeStamp, inEvents, removeEvents);
                    for (Event event : inEvents) {
                        inEventsList.add(event.getData());
                        inEventCount.incrementAndGet();
                    }
                    eventArrived = true;
                }
                eventArrived = true;
            }
        });

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 100});
        stockStream.send(new Object[]{"WSO2", 120});
        stockStream.send(new Object[]{"IBM", 160});
        fooStream.send(new Object[]{"WSO2", 10});
        SiddhiTestHelper.waitForEvents(waitTime, 2, inEventCount, timeout);

        siddhiAppRuntime.shutdown();

        List<Object[]> expected = Arrays.asList(
                new Object[]{"WSO2", 100, 10},
                new Object[]{"WSO2", 120, 10}
        );

        AssertJUnit.assertTrue("Event arrived", eventArrived);
        AssertJUnit.assertEquals("Number of success events", 2, inEventCount.get());
        AssertJUnit.assertTrue("In events matched", SiddhiTestHelper.isUnsortedEventsMatch(inEventsList, expected));
    }

    @Test
    public void testMongoTableQuery19() throws InterruptedException {
        log.info("testMongoTableQuery19 : Test 'not' function in selection-1.");

        MongoTableTestUtils.dropCollection(uri, "FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, oldPrice int, newPrice int); " +
                "define stream FooStream (symbol string, price int); " +
                "@store(type = 'mongodb' , mongodb.uri='" + uri + "')" +
                "define table FooTable (symbol string, oldPrice int, newPrice int);";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into FooTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from FooStream as s join FooTable as t " +
                "select t.symbol as tblSymbol, NOT (t.oldPrice < t.newPrice) as isPriceNotLow " +
                "insert into OutputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                if (inEvents != null) {
                    EventPrinter.print(timeStamp, inEvents, removeEvents);
                    for (Event event : inEvents) {
                        inEventsList.add(event.getData());
                        inEventCount.incrementAndGet();
                    }
                    eventArrived = true;
                }
                eventArrived = true;
            }
        });

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 100, 110});
        stockStream.send(new Object[]{"WSO2", 120, 100});
        stockStream.send(new Object[]{"IBM", 160, 160});
        fooStream.send(new Object[]{"WSO2", 10});
        SiddhiTestHelper.waitForEvents(waitTime, 3, inEventCount, timeout);

        siddhiAppRuntime.shutdown();

        List<Object[]> expected = Arrays.asList(
                new Object[]{"WSO2", false},
                new Object[]{"WSO2", true},
                new Object[]{"IBM", true}
        );

        AssertJUnit.assertTrue("Event arrived", eventArrived);
        AssertJUnit.assertEquals("Number of success events", 3, inEventCount.get());
        AssertJUnit.assertTrue("In events matched", SiddhiTestHelper.isUnsortedEventsMatch(inEventsList, expected));
    }

    @Test
    public void testMongoTableQuery20() throws InterruptedException {
        log.info("testMongoTableQuery20 : Test 'not' function in selection-2.");

        MongoTableTestUtils.dropCollection(uri, "FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, oldPrice int, newPrice int); " +
                "define stream FooStream (symbol string, price int); " +
                "@store(type = 'mongodb' , mongodb.uri='" + uri + "')" +
                "define table FooTable (symbol string, oldPrice int, newPrice int);";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into FooTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from FooStream as s join FooTable as t " +
                "select t.symbol as tblSymbol, (s.price < t.newPrice) and (NOT (t.oldPrice < t.newPrice)) " +
                "as isPriceNotLow " +
                "insert into OutputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                if (inEvents != null) {
                    EventPrinter.print(timeStamp, inEvents, removeEvents);
                    for (Event event : inEvents) {
                        inEventsList.add(event.getData());
                        inEventCount.incrementAndGet();
                    }
                    eventArrived = true;
                }
                eventArrived = true;
            }
        });

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 100, 110});
        stockStream.send(new Object[]{"WSO2", 120, 100});
        stockStream.send(new Object[]{"IBM", 160, 160});
        fooStream.send(new Object[]{"WSO2", 10});
        SiddhiTestHelper.waitForEvents(waitTime, 3, inEventCount, timeout);

        siddhiAppRuntime.shutdown();

        List<Object[]> expected = Arrays.asList(
                new Object[]{"WSO2", false},
                new Object[]{"WSO2", true},
                new Object[]{"IBM", true}
        );

        AssertJUnit.assertTrue("Event arrived", eventArrived);
        AssertJUnit.assertEquals("Number of success events", 3, inEventCount.get());
        AssertJUnit.assertTrue("In events matched", SiddhiTestHelper.isUnsortedEventsMatch(inEventsList, expected));
    }

    @Test
    public void testMongoTableQuery21() throws InterruptedException {
        log.info("testMongoTableQuery21 : Test selection of stream attributes.");

        MongoTableTestUtils.dropCollection(uri, "FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, amount int); " +
                "define stream FooStream (symbol string, name string, price int, stockAvailable bool); " +
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
                "select s.symbol, s.name, s.price as streamPrice, s.stockAvailable " +
                "insert into OutputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                if (inEvents != null) {
                    EventPrinter.print(timeStamp, inEvents, removeEvents);
                    for (Event event : inEvents) {
                        inEventsList.add(event.getData());
                        inEventCount.incrementAndGet();
                    }
                    eventArrived = true;
                }
                eventArrived = true;
            }
        });

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"Services", 100});
        stockStream.send(new Object[]{"Services", 120});
        stockStream.send(new Object[]{"Products", 160});
        fooStream.send(new Object[]{"Services", "API", 300, true});
        SiddhiTestHelper.waitForEvents(waitTime, 2, inEventCount, timeout);

        siddhiAppRuntime.shutdown();

        List<Object[]> expected = Arrays.asList(
                new Object[]{"Services", "API", 300, true},
                new Object[]{"Services", "API", 300, true}
        );

        AssertJUnit.assertTrue("Event arrived", eventArrived);
        AssertJUnit.assertEquals("Number of success events", 2, inEventCount.get());
        AssertJUnit.assertTrue("In events matched", SiddhiTestHelper.isUnsortedEventsMatch(inEventsList, expected));
    }

    @Test
    public void testMongoTableQuery22() throws InterruptedException {
        log.info("testMongoTableQuery22 : Test selection of store attributes.");

        MongoTableTestUtils.dropCollection(uri, "FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, name string, price int, stockAvailable bool); " +
                "define stream FooStream (symbol string); " +
                "@store(type = 'mongodb' , mongodb.uri='" + uri + "')" +
                "define table FooTable (symbol string, name string, price int, stockAvailable bool);";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into FooTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from FooStream as s join FooTable as t " +
                "on s.symbol == t.symbol " +
                "select s.symbol, t.name, t.price, t.stockAvailable " +
                "insert into OutputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                if (inEvents != null) {
                    EventPrinter.print(timeStamp, inEvents, removeEvents);
                    for (Event event : inEvents) {
                        inEventsList.add(event.getData());
                        inEventCount.incrementAndGet();
                    }
                    eventArrived = true;
                }
                eventArrived = true;
            }
        });

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"Services", "API", 300, true});
        stockStream.send(new Object[]{"Services", "CLOUD", 200, false});
        stockStream.send(new Object[]{"Products", 160});
        fooStream.send(new Object[]{"Services"});
        SiddhiTestHelper.waitForEvents(waitTime, 2, inEventCount, timeout);

        siddhiAppRuntime.shutdown();

        List<Object[]> expected = Arrays.asList(
                new Object[]{"Services", "API", 300, true},
                new Object[]{"Services", "CLOUD", 200, false}
        );

        AssertJUnit.assertTrue("Event arrived", eventArrived);
        AssertJUnit.assertEquals("Number of success events", 2, inEventCount.get());
        AssertJUnit.assertTrue("In events matched", SiddhiTestHelper.isUnsortedEventsMatch(inEventsList, expected));
    }

    @Test
    public void testMongoTableQuery23() throws InterruptedException {
        log.info("testMongoTableQuery23 : Test selection of stream attributes with groupBy clause.");

        MongoTableTestUtils.dropCollection(uri, "FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, weight int); " +
                "define stream FooStream (symbol string, volume int, isAvailable bool); " +
                "@store(type = 'mongodb' , mongodb.uri='" + uri + "')" +
                "define table FooTable (symbol string, price float, weight int);";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into FooTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from FooStream as s join FooTable as t " +
                "on s.symbol == t.symbol " +
                "select s.symbol, s.volume, s.isAvailable, t.price, sum(t.price) as sumPrice " +
                "group by t.price " +
                "order by t.price " +
                "insert into OutputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                if (inEvents != null) {
                    EventPrinter.print(timeStamp, inEvents, removeEvents);
                    for (Event event : inEvents) {
                        inEventsList.add(event.getData());
                        inEventCount.incrementAndGet();
                    }
                    eventArrived = true;
                }
                eventArrived = true;
            }
        });

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"GOOGLE", 10.5f, 10});
        stockStream.send(new Object[]{"GOOGLE", 10.5f, 20});
        stockStream.send(new Object[]{"APPLE", 10.5f, 12});
        stockStream.send(new Object[]{"GOOGLE", 12.5f, 16});
        fooStream.send(new Object[]{"GOOGLE", 10, true});
        SiddhiTestHelper.waitForEvents(waitTime, 2, inEventCount, timeout);

        siddhiAppRuntime.shutdown();

        List<Object[]> expected = Arrays.asList(
                new Object[]{"GOOGLE", 10, true, 10.5, 21.0},
                new Object[]{"GOOGLE", 10, true, 12.5, 12.5}
        );

        AssertJUnit.assertTrue("Event arrived", eventArrived);
        AssertJUnit.assertEquals("Number of success events", 2, inEventCount.get());
        AssertJUnit.assertTrue("In events matched", SiddhiTestHelper.isUnsortedEventsMatch(inEventsList, expected));
    }
}
