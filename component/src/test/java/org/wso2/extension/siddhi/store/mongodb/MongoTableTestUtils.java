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

package org.wso2.extension.siddhi.store.mongodb;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoException;
import com.mongodb.client.ListIndexesIterable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class MongoTableTestUtils {

    private static final Log log = LogFactory.getLog(MongoTableTestUtils.class);
    private static final String MONGO_CLIENT_URI =
            "mongodb://{{mongo.username}}:{{mongo.password}}@{{docker.ip}}:{{docker.port}}/{{mongo.database}}";
    private static String databaseName = "Foo";
    private static MongoClient mongoClient;

    private MongoTableTestUtils() {
    }

    private static MongoClient getMongoClient() {
        if (mongoClient != null) {
            return mongoClient;
        } else {
            try {
                String uri = resolveUri();
                return mongoClient = new MongoClient(new MongoClientURI(uri));
            } catch (Exception e) {
                log.debug("Creating Mongo client failed due to " + e.getMessage(), e);
                throw e;
            }

        }
    }

    public static String resolveUri(String uri) {
        return uri
                .replace("{{docker.ip}}", getIpAddressOfContainer())
                .replace("{{docker.port}}", getPortOfContainer())
                .replace("{{mongo.username}}", getMongoUserName())
                .replace("{{mongo.password}}", getMongoPassword())
                .replace("{{mongo.database}}", getMongoDatabase());
    }

    public static String resolveUri() {
        return resolveUri(MONGO_CLIENT_URI);
    }

    private static String getIpAddressOfContainer() {
        String dockerHostIp = System.getProperty("docker.host.ip");
        if (!isEmpty(dockerHostIp)) {
            return dockerHostIp;
        } else {
            return "172.17.0.2";
        }
    }

    private static String getPortOfContainer() {
        String dockerHostPort = System.getProperty("docker.host.port");
        if (!isEmpty(dockerHostPort)) {
            return dockerHostPort;
        } else {
            return "27017";
        }
    }

    private static String getMongoUserName() {
        String mongoUsername = System.getProperty("mongo.username");
        if (!isEmpty(mongoUsername)) {
            return mongoUsername;
        } else {
            return "admin";
        }
    }

    private static String getMongoPassword() {
        String mongoPassword = System.getProperty("mongo.password");
        if (!isEmpty(mongoPassword)) {
            return mongoPassword;
        } else {
            return "admin";
        }
    }

    private static String getMongoDatabase() {
        String mongoDatabase = System.getProperty("mongo.database");
        if (!isEmpty(mongoDatabase)) {
            databaseName = mongoDatabase;
        }
        return databaseName;
    }

    private static boolean isEmpty(String field) {
        return (field == null || field.trim().length() == 0);
    }

    public static void dropCollection(String collectionName) {
        try {
            getMongoClient().getDatabase(databaseName).getCollection(collectionName).drop();
        } catch (MongoException e) {
            log.debug("Clearing DB collection failed due to " + e.getMessage(), e);
            mongoClient.close();
            throw e;
        }
    }

    public static long getDocumentsCount(String collectionName) {
        try {
            return getMongoClient().getDatabase(databaseName).getCollection(collectionName).count();
        } catch (MongoException e) {
            log.debug("Getting rows in DB table failed due to " + e.getMessage(), e);
            mongoClient.close();
            throw e;
        }
    }

    public static boolean doesCollectionExists(String customCollectionName) {
        try {
            for (String collectionName : getMongoClient().getDatabase(databaseName).listCollectionNames()) {
                if (customCollectionName.equals(collectionName)) {
                    return true;
                }
            }
            return false;
        } catch (MongoException e) {
            log.debug("Checking whether collection was created failed due to" + e.getMessage(), e);
            mongoClient.close();
            throw e;
        }
    }

    private static List<Document> getIndexList(String collectionName) {
        try {
            ListIndexesIterable<Document> existingIndexesIterable =
                    getMongoClient().getDatabase(databaseName).getCollection(collectionName).listIndexes();
            List<Document> existingIndexDocuments = new ArrayList<>();
            existingIndexesIterable.forEach((Consumer<? super Document>) existingIndex -> {
                existingIndex.remove("ns");
                existingIndexDocuments.add(existingIndex);
            });
            return existingIndexDocuments;
        } catch (MongoException e) {
            log.debug("Getting indexes in DB table failed due to " + e.getMessage(), e);
            mongoClient.close();
            throw e;
        }
    }

    public static Document getIndex(String collectionName, String indexName) {
        try {
            List<Document> existingIndexList = getIndexList(collectionName);
            for (Document existingIndex : existingIndexList) {
                if (existingIndex.get("name").equals(indexName)) {
                    return existingIndex;
                }
            }
            return null;
        } catch (MongoException e) {
            log.debug("Getting indexes in DB table failed due to " + e.getMessage(), e);
            mongoClient.close();
            throw e;
        }
    }

    public static Document getDocument(String collectionName, String findFilter) {
        try {
            Document findFilterDocument = Document.parse(findFilter);
            Document firstFind = getMongoClient().getDatabase(databaseName).getCollection(collectionName)
                    .find(findFilterDocument).first();
            firstFind.remove("_id");
            return firstFind;
        } catch (MongoException e) {
            log.debug("Getting indexes in DB table failed due to " + e.getMessage(), e);
            mongoClient.close();
            throw e;
        }
    }

    public static void createCollection(String collectionName) {
        dropCollection(collectionName);
        try {
            getMongoClient().getDatabase(databaseName).createCollection(collectionName);
        } catch (MongoException e) {
            log.debug("Getting indexes in DB table failed due to " + e.getMessage(), e);
            mongoClient.close();
            throw e;
        }
    }
}


