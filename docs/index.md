Siddhi Store MongoDB
===================

  [![Jenkins Build Status](https://wso2.org/jenkins/job/siddhi/job/siddhi-store-mongodb/badge/icon)](https://wso2.org/jenkins/job/siddhi/job/siddhi-store-mongodb/)
  [![GitHub Release](https://img.shields.io/github/release/siddhi-io/siddhi-store-mongodb.svg)](https://github.com/siddhi-io/siddhi-store-mongodb/releases)
  [![GitHub Release Date](https://img.shields.io/github/release-date/siddhi-io/siddhi-store-mongodb.svg)](https://github.com/siddhi-io/siddhi-store-mongodb/releases)
  [![GitHub Open Issues](https://img.shields.io/github/issues-raw/siddhi-io/siddhi-store-mongodb.svg)](https://github.com/siddhi-io/siddhi-store-mongodb/issues)
  [![GitHub Last Commit](https://img.shields.io/github/last-commit/siddhi-io/siddhi-store-mongodb.svg)](https://github.com/siddhi-io/siddhi-store-mongodb/commits/master)
  [![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

The **siddhi-store-mongodb extension** is an extension to <a target="_blank" href="https://wso2.github.io/siddhi">Siddhi</a> that persist and retrieve events to/from MongoDB

For information on <a target="_blank" href="https://siddhi.io/">Siddhi</a> and it's features refer <a target="_blank" href="https://siddhi.io/redirect/docs.html">Siddhi Documentation</a>. 

## Download

* Versions 2.x and above with group id `io.siddhi.extension.*` from <a target="_blank" href="https://mvnrepository.com/artifact/io.siddhi.extension.store.mongodb/siddhi-store-mongodb/">here</a>.
* Versions 1.x and lower with group id `org.wso2.extension.siddhi.*` from <a target="_blank" href="https://mvnrepository.com/artifact/org.wso2.extension.siddhi.store.mongodb/siddhi-store-mongodb">here</a>.

## Latest API Docs 

Latest API Docs is <a target="_blank" href="https://siddhi-io.github.io/siddhi-store-mongodb/api/2.1.1">2.1.1</a>.

## Features

* <a target="_blank" href="https://siddhi-io.github.io/siddhi-store-mongodb/api/2.1.1/#mongodb-store">mongodb</a> *(<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#store">Store</a>)*<br> <div style="padding-left: 1em;"><p><p style="word-wrap: break-word;margin: 0;">Using this extension a MongoDB Event Table can be configured to persist events in a MongoDB of user's choice.</p></p></div>

## Dependencies 

MongoDB connector jar and its dependencies should be added to the runtime (mongodb-java-driver-3.4.2, includes all the dependencies). For installing third party connectors on various Siddhi execution environments refer Siddhi documentation section on <a target="_blank" href="https://siddhi.io/redirect/add-extensions.html">adding third party libraries</a>.

## Installation

For installing this extension on various Siddhi execution environments refer Siddhi documentation section on <a target="_blank" href="https://siddhi.io/redirect/add-extensions.html">adding extensions</a>.

## Running Integration tests in docker containers(Optional)

The MongoDB functionality are tested with the docker base integration test framework. The test framework initializes the docker container according to the given profile before executing the test suite.

1. Install and run docker in daemon mode.

    *  Installing docker on Linux,<br>
       Note:<br>    These commands retrieve content from get.docker.com web in a quiet output-document mode and install.Then we need to stop docker service as it needs to restart docker in daemon mode. After that, we need to export docker daemon host.
       
            wget -qO- https://get.docker.com/ | sh
            sudo service dockerd stop
            export DOCKER_HOST=tcp://172.17.0.1:4326
            docker daemon -H tcp://172.17.0.1:4326

    *  On installing docker on Mac, see <a target="_blank" href="https://docs.docker.com/docker-for-mac/">Get started with Docker for Mac</a>

    *  On installing docker on Windows, see <a target="_blank" href="https://docs.docker.com/docker-for-windows/">Get started with Docker for Windows</a>
   
2. To run the integration tests, issue the following commands.

    * MongoDB 3.4 without SSL connection
    
            mvn verify -P mongod -Ddocker.removeVolumes=true

    * MongoDB 3.4 with SSL connection
           
            mvn verify -P mongod-ssl -Ddocker.removeVolumes=true
    
    * MongoDB 4.2 
            mvn verify -P mongod4 -Ddocker.removeVolumes=true

## Support and Contribution

* We encourage users to ask questions and get support via <a target="_blank" href="https://stackoverflow.com/questions/tagged/siddhi">StackOverflow</a>, make sure to add the `siddhi` tag to the issue for better response.

* If you find any issues related to the extension please report them on <a target="_blank" href="https://github.com/siddhi-io/siddhi-execution-string/issues">the issue tracker</a>.

* For production support and other contribution related information refer <a target="_blank" href="https://siddhi.io/community/">Siddhi Community</a> documentation.

