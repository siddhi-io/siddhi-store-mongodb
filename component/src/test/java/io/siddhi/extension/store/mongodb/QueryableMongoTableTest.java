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
    public void testMongoTableJoinQuery1() throws InterruptedException {
        log.info("testMongoTableJoinQuery1");

        MongoTableTestUtils.dropCollection(uri, "FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, amount int); " +
                "define stream FooStream (symbol string, volume int, name string); " +
                "@store(type = 'mongodb' , mongodb.uri='" + uri + "')" +
                "define table FooTable (symbol string, price float, amount int);";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into FooTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from FooStream as s join FooTable as t " +
                "on s.symbol == t.symbol "+
                "select t.symbol as tblSymbol, t.amount as tblAmount, t.price as tblPrice, s.volume as streamVolume, '100' as fixedVal " +
                "having tblAmount > 110 and tblPrice > 10 and streamVolume <= 10 " +
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
                                Assert.assertEquals(new Object[]{"WSO2",120,12.5,10,100}, event.getData());
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
        stockStream.send(new Object[]{"WSO2", 12.5f, 120});
        stockStream.send(new Object[]{"WSO2", 6.5f, 150});
        stockStream.send(new Object[]{"IBM", 8.5f, 160});
        fooStream.send(new Object[]{"WSO2",10,"siddhi"});
        SiddhiTestHelper.waitForEvents(waitTime, 1, eventCount, timeout);

        siddhiAppRuntime.shutdown();

        Assert.assertEquals(eventCount.intValue(), 1, "Read events failed");
    }

    @Test
    public void testMongoTableJoinQuery2() throws InterruptedException {
        log.info("testMongoTableJoinQuery2");

        MongoTableTestUtils.dropCollection(uri, "FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, amount int); " +
                "define stream FooStream (symbol string, volume int, name string); " +
                "@store(type = 'mongodb' , mongodb.uri='" + uri + "')" +
                "define table FooTable (symbol string, price float, amount int);";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into FooTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from FooStream as s join FooTable as t " +
                "on s.symbol == t.symbol "+
                "select t.symbol, t.amount, s.volume, t.price, '100' as fixedVal " +          //if you do the selection without 't.price' and used 't.price' in the having, then there will be an issue due to pipeline order used.
                "having t.amount > 110 and t.price > 10 " +
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
                                Assert.assertEquals(new Object[]{"WSO2",120,10,12.5,100}, event.getData());
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
        stockStream.send(new Object[]{"WSO2", 12.5f, 120});
        stockStream.send(new Object[]{"WSO2", 6.5f, 150});
        stockStream.send(new Object[]{"IBM", 8.5f, 160});
        fooStream.send(new Object[]{"WSO2",10,"siddhi"});
        SiddhiTestHelper.waitForEvents(waitTime, 1, eventCount, timeout);

        siddhiAppRuntime.shutdown();

        Assert.assertEquals(eventCount.intValue(), 1, "Read events failed");
    }

    @Test
    public void testMongoTableJoinQuery3() throws InterruptedException {
        log.info("testMongoTableJoinQuery3");

        MongoTableTestUtils.dropCollection(uri, "FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, amount int); " +
                "define stream FooStream (symbol string, volume int, name string); " +
                "@store(type = 'mongodb' , mongodb.uri='" + uri + "')" +
                "define table FooTable (symbol string, price float, amount int);";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into FooTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from FooStream as s join FooTable as t " +
                "on s.symbol == t.symbol "+
                "select t.symbol as tblSymbol, t.amount as tblAmount, t.price as tblPrice, s.volume as streamVolume, '100' as fixedVal " +
                "having tblAmount > 110 " +
                "limit 1 "+
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
                                Assert.assertEquals(new Object[]{"WSO2",120,12.5,10,100}, event.getData());
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
        stockStream.send(new Object[]{"WSO2", 12.5f, 120});
        stockStream.send(new Object[]{"WSO2", 6.5f, 150});
        stockStream.send(new Object[]{"IBM", 8.5f, 160});
        fooStream.send(new Object[]{"WSO2",10,"siddhi"});
        SiddhiTestHelper.waitForEvents(waitTime, 1, eventCount, timeout);

        siddhiAppRuntime.shutdown();

        Assert.assertEquals(eventCount.intValue(), 1, "Read events failed");
    }

    @Test
    public void testMongoTableJoinQuery4() throws InterruptedException {
        log.info("testMongoTableJoinQuery4");

        MongoTableTestUtils.dropCollection(uri, "FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, amount int); " +
                "define stream FooStream (symbol string, volume int, name string); " +
                "@store(type = 'mongodb' , mongodb.uri='" + uri + "')" +
                "define table FooTable (symbol string, price float, amount int);";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into FooTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from FooStream as s join FooTable as t " +
                "on s.symbol == t.symbol "+
                "select t.symbol as tblSymbol, t.amount as tblAmount, t.price as tblPrice, s.volume as streamVolume, '100' as fixedVal " +
                "having tblAmount > 110 " +
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
                                Assert.assertEquals(new Object[]{"WSO2",150,6.5,10,100}, event.getData());
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
        stockStream.send(new Object[]{"WSO2", 12.5f, 120});
        stockStream.send(new Object[]{"WSO2", 6.5f, 150});
        stockStream.send(new Object[]{"IBM", 8.5f, 160});
        fooStream.send(new Object[]{"WSO2",10,"siddhi"});
        SiddhiTestHelper.waitForEvents(waitTime, 1, eventCount, timeout);

        siddhiAppRuntime.shutdown();

        Assert.assertEquals(eventCount.intValue(), 1, "Read events failed");
    }

    @Test
    public void testMongoTableJoinQuery5() throws InterruptedException {
        log.info("testMongoTableJoinQuery5");

        MongoTableTestUtils.dropCollection(uri, "FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, amount int); " +
                "define stream FooStream (symbol string, volume int, name string); " +
                "@store(type = 'mongodb' , mongodb.uri='" + uri + "')" +
                "define table FooTable (symbol string, price float, amount int);";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into FooTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from FooStream as s join FooTable as t " +
                "on s.symbol == t.symbol "+
                "select t.symbol, t.price, '100' as fixedVal " +
                "order by t.amount DESC "+
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
                                Assert.assertEquals(new Object[]{"WSO2",12.5,100}, event.getData());
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
        stockStream.send(new Object[]{"WSO2", 12.5f, 120});
        stockStream.send(new Object[]{"WSO2", 6.5f, 150});
        stockStream.send(new Object[]{"IBM", 8.5f, 160});
        fooStream.send(new Object[]{"WSO2",10,"siddhi"});
        SiddhiTestHelper.waitForEvents(waitTime, 1, eventCount, timeout);

        siddhiAppRuntime.shutdown();

        Assert.assertEquals(eventCount.intValue(), 1, "Read events failed");
    }

    @Test
    public void testMongoTableJoinQuery6() throws InterruptedException {
        log.info("testMongoTableJoinQuery6");

        MongoTableTestUtils.dropCollection(uri, "FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, amount int); " +
                "define stream FooStream (symbol string, volume int, name string); " +
                "@store(type = 'mongodb' , mongodb.uri='" + uri + "')" +
                "define table FooTable (symbol string, price float, amount int);";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into FooTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from FooStream as s join FooTable as t " +
                "on s.symbol == t.symbol "+
                "select t.symbol as tblSymbol, t.price as tblPrice, '100' as fixedVal " +
                "order by t.amount DESC, tblPrice DESC "+
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
                                Assert.assertEquals(new Object[]{"WSO2",9.5,100}, event.getData());
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

        stockStream.send(new Object[]{"WSO2", 9.5f, 120});
        stockStream.send(new Object[]{"WSO2", 12.5f, 120});
        stockStream.send(new Object[]{"WSO2", 6.5f, 150});
        fooStream.send(new Object[]{"WSO2",10,"siddhi"});
        SiddhiTestHelper.waitForEvents(waitTime, 1, eventCount, timeout);

        siddhiAppRuntime.shutdown();

        Assert.assertEquals(eventCount.intValue(), 1, "Read events failed");
    }

    @Test
    public void testMongoTableJoinQuery7() throws InterruptedException {
        log.info("testMongoTableJoinQuery7");

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
                "select t.symbol, avg(t.weight) as avgweight, min(t.price) as minprice, max(t.price) as maxprice, " +
                        "sum(t.price) as sumprice " +
                "group by t.symbol, t.price " +
                "order by t.symbol DESC "+
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
                                Assert.assertEquals(new Object[]{"GOOGLE",13.0, 9.5, 12.5, 22.0}, event.getData());
                                break;
                            case 2:
                                Assert.assertEquals(new Object[]{"APPLE",16.0, 8.5, 10.5, 19.0}, event.getData());
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
        stockStream.send(new Object[]{"GOOGLE", 12.5f, 16});
        stockStream.send(new Object[]{"APPLE", 8.5f, 18});
        fooStream.send(new Object[]{"GOOGLE",10});
        SiddhiTestHelper.waitForEvents(waitTime, 2, eventCount, timeout);

        siddhiAppRuntime.shutdown();

        Assert.assertEquals(eventCount.intValue(), 2, "Read events failed");
    }

    @Test
    public void testMongoTableJoinQuery8() throws InterruptedException {
        log.info("testMongoTableJoinQuery8");

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
                "select s.symbol as streamSymbol, t.symbol as tblSymbol, t.price * s.volume as totalCost, " +
                "t.price * 10 / 100 as discount "+
                "limit 2 "+
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
                                Assert.assertEquals(new Object[]{"WSO2","WSO2",65.0,0.65}, event.getData());
                                break;
                            case 2:
                                Assert.assertEquals(new Object[]{"WSO2","IBM",95.0,0.95}, event.getData());
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
        stockStream.send(new Object[]{"GOOGLE", 10.5f});
        fooStream.send(new Object[]{"WSO2",10,"siddhi"});
        SiddhiTestHelper.waitForEvents(waitTime, 2, eventCount, timeout);

        siddhiAppRuntime.shutdown();

        Assert.assertEquals(eventCount.intValue(), 2, "Read events failed");
    }

    @Test
    public void testMongoTableJoinQuery9() throws InterruptedException {
        log.info("testMongoTableJoinQuery9");

        MongoTableTestUtils.dropCollection(uri, "FooTable");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price int, isFraud bool); " +
                "define stream FooStream (symbol string, isChecked bool, isOlderStock bool); " +
                "@store(type = 'mongodb' , mongodb.uri='" + uri + "')" +
                "define table FooTable (symbol string, price int, isFraud bool);";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into FooTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from FooStream as s join FooTable as t " +
                "on s.symbol != t.symbol "+
                "select t.symbol as tblSymbol, (t.isFraud or s.isChecked and s.isOlderStock and true) as isCheckedData " +
                "having isCheckedData == true and tblSymbol != 'GOOGLE' "+
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

        stockStream.send(new Object[]{"LINUX", 10, true});
        stockStream.send(new Object[]{"GOOGLE", 12, false});
        fooStream.send(new Object[]{"WSO2", true, false});
        SiddhiTestHelper.waitForEvents(waitTime, 1, eventCount, timeout);

        siddhiAppRuntime.shutdown();

        Assert.assertEquals(eventCount.intValue(), 1, "Read events failed");
    }

}
