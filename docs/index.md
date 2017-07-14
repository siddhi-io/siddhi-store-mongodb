Siddhi-store-mongodb
======================================

This is an extension for siddhi mongodb event table implementation. This extension can be used to persist events to a MongoDB instance of the users choice.

Features Supported
------------------
 - Defining an Event Table
 - Inserting Events
 - Retrieving Events
 - Deleting Events
 - Updating persisted events
 - Filtering Events
 - Upserting Events
           
#### Prerequisites for using the feature
 - A MongoDB server instance should be started.
 - User should have the necessary privileges and access rights to connect to the MongoDB data store of choice.
 - Deployment yaml file has to be inserted in the following manner (properties set are optional), if the user wishes to configure all MongoDB connections made from the extension(Optional).
 <pre>
 extensions:
   -extension:
    name: 'store'
    namespace: 'mongodb'
    properties:
      applicationName: 'application1'
      cursorFinalizerEnabled: false
 </pre>

#### Deploying the feature
 Feature can be deployed as an OSGI bundle by putting the jar file of the component to SP_HOME/lib directory of SP 4.0.0 pack. 

## How to Contribute
* Please report issues at [Siddhi Github Issue Tacker](https://github.com/wso2-extensions/siddhi-store-mongodb/issues)
* Send your bug fixes pull requests to [master branch](https://github.com/wso2-extensions/siddhi-store-mongodb/tree/master) 

## Contact us 
Siddhi developers can be contacted via the mailing lists:
  * Carbon Developers List : dev@wso2.org
  * Carbon Architecture List : architecture@wso2.org

**We welcome your feedback and contribution.**

Siddhi SP Team

## API Docs:

1. <a href="./api/1.0.3-SNAPSHOT">1.0.3-SNAPSHOT</a>
