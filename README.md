Siddhi-store-mongodb
======================================
---
##### Latest Released Version v1.0.0.

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
 - Deployment yaml file has to be inserted in the following manner (properties set are optional), if the user wishes to configure all MongoDB connections made from the extension.
 <pre>
 extensions:
 
   -extension:
    name: 'store'
    namespace: 'mongodb'
    properties:
      applicationName: 'application1'
      cursorFinalizerEnabled: false
      requiredReplicaSetName: 'rs0'
      sslEnabled: false
      keyStore: '${carbon.home}/resources/security/client-truststore.jks'
      keyStorePassword: 'wso2carbon'
      trustStore: '${carbon.home}/resources/security/client-truststore.jks'
      trustStorePassword: 'wso2carbon'
      connectTimeout:1000
      connectionsPerHost: 15
      minConnectionsPerHost: 0
      maxConnectionIdleTime: 0
      maxWaitTime: 12000
      threadsAllowedToBlockForConnectionMultiplier: 5
      maxConnectionLifeTime: 0
      socketKeepAlive: false
      socketTimeout: 0
      writeConcern: acknowledged
      readConcern: default
      readPreference: primary
      localThreshold: 15
      serverSelectionTimeout: 30000
      heartbeatSocketTimeout: 20000
      heartbeatConnectTimeout: 20000
      heartbeatFrequency: 20000
      minHeartbeatFrequency: 500
 </pre>

 
 #### Deploying the feature
 Feature can be deploy as a OSGI bundle by putting jar file of the component to DAS_HOME/lib directory of DAS 4.0.0 pack. 
 
 ##### Example Siddhi Queries
 ###### Defining an Event Table
 <pre>
 @Store(type="mongodb", mongodb.uri="mongodb://admin:admin@localhost:27017/Foo")
 @PrimaryKey("symbol")
 @IndexBy("symbol 1 {background:true}")
 define table FooTable (symbol string, price float, volume long);</pre>

#### Documentation 
* https://docs.wso2.com/display/DAS400/Configuring+Event+Tables+to+Store+Data

## How to Contribute
* Please report issues at [Siddhi Github Issue Tacker](https://github.com/wso2-extensions/siddhi-store-mongodb/issues)
* Send your bug fixes pull requests to [master branch](https://github.com/wso2-extensions/siddhi-store-mongodb/tree/master) 

## Contact us 
Siddhi developers can be contacted via the mailing lists:
  * Carbon Developers List : dev@wso2.org
  * Carbon Architecture List : architecture@wso2.org

### We welcome your feedback and contribution.

Siddhi DAS Team