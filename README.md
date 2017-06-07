Siddhi-store-mongodb
======================================
---
##### New version of Siddhi v4.0.0 is built in Java 8.
##### Latest Released Version v4.0.0-M4.

This is an extension for siddhi mongodb event table implementation. This extension can be used to persist events to a MongoDB instance of the users choice.
 

Features Supported
------------------
 - Defining an Event Table
 - Inserting Events
 - Retrieving Events
 - Deleting Events
 - Updating persisted events
 - Filtering Events
 - Insert or Update Events
 
     
 #### Prerequisites for using the feature
 - A MongoDB server instance should be started.
 - User should have the necessary privileges and access rights to connect to the MongoDB data store of choice.

 
 #### Deploying the feature
 Feature can be deploy as a OSGI bundle by putting jar file of the component to DAS_HOME/lib directory of DAS 4.0.0 pack. 
 
 #### Example Siddhi Queries
 ##### Defining an Event Table
 <pre>
 @Store(type="mongodb", mongodb.uri="mongodb://admin:admin@localhost:27017/Foo?ssl=true")
 @PrimaryKey("symbol")
 @IndexBy("symbol 1 {background:true}")
 define table FooTable (symbol string, price float, volume long);</pre>

#### Documentation 

  * https://docs.wso2.com/display/DAS400/Configuring+Event+Tables+to+Store+Data

## How to Contribute
* Please report issues at [Siddhi JIRA] (https://wso2.org/jira/browse/SIDDHI)
* Send your bug fixes pull requests to [master branch] (https://github.com/wso2-extensions/siddhi-io-http/tree/master) 

## Contact us 
Siddhi developers can be contacted via the mailing lists:
  * Carbon Developers List : dev@wso2.org
  * Carbon Architecture List : architecture@wso2.org

### We welcome your feedback and contribution.

Siddhi DAS Team