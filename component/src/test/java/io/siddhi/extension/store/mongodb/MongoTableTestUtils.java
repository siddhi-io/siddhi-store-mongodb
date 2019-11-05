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
            "mongodb://localhost/admin";
    private static String databaseName = "admin";

    private MongoTableTestUtils() {
    }

    public static String resolveBaseUri(String uri) {
        return uri
                .replace("{{mongo.credentials}}", getMongoCredentials())
                .replace("{{mongo.servers}}", getAddressOfContainers())
                .replace("{{mongo.database}}", getMongoDatabaseName());
    }

    public static String resolveBaseUri() {
        return resolveBaseUri(MONGO_CLIENT_URI);
    }

    private static String getAddressOfContainers() {
        String mongoServers = System.getProperty("mongo.servers");
        if (!isEmpty(mongoServers)) {
            return mongoServers;
        } else {
            return "172.17.0.2:27017";
        }
    }

    private static String getMongoCredentials() {
        String mongoUsername = System.getProperty("mongo.username");
        String mongoPassword = System.getProperty("mongo.password");
        if (!isEmpty(mongoUsername) && !isEmpty(mongoPassword)) {
            return mongoUsername + ":" + mongoPassword + "@";
        } else {
            if (!isEmpty(mongoUsername)) {
                return mongoUsername + "@";
            }
            return "";
        }
    }

    private static String getMongoDatabaseName() {
        String mongoDatabaseName = System.getProperty("mongo.database.name");
        if (!isEmpty(mongoDatabaseName)) {
            databaseName = mongoDatabaseName;
        }
        return databaseName;
    }

    private static boolean isEmpty(String field) {
        return (field == null || field.trim().length() == 0);
    }

    public static void dropCollection(String uri, String collectionName) {
        try (MongoClient mongoClient = new MongoClient(new MongoClientURI(uri))) {
            mongoClient.getDatabase(databaseName).getCollection(collectionName).drop();
        } catch (MongoException e) {
            log.debug("Clearing DB collection failed due to " + e.getMessage(), e);
            throw e;
        }
    }

    public static long getDocumentsCount(String uri, String collectionName) {
        try (MongoClient mongoClient = new MongoClient(new MongoClientURI(uri))) {
            return mongoClient.getDatabase(databaseName).getCollection(collectionName).count();
        } catch (MongoException e) {
            log.debug("Getting rows in DB table failed due to " + e.getMessage(), e);
            throw e;
        }
    }

    public static boolean doesCollectionExists(String uri, String customCollectionName) {
        try (MongoClient mongoClient = new MongoClient(new MongoClientURI(uri))) {
            for (String collectionName : mongoClient.getDatabase(databaseName).listCollectionNames()) {
                if (customCollectionName.equals(collectionName)) {
                    return true;
                }
            }
            return false;
        } catch (MongoException e) {
            log.debug("Checking whether collection was created failed due to" + e.getMessage(), e);
            throw e;
        }
    }

    private static List<Document> getIndexList(String uri, String collectionName) {
        try (MongoClient mongoClient = new MongoClient(new MongoClientURI(uri))) {
            ListIndexesIterable<Document> existingIndexesIterable =
                    mongoClient.getDatabase(databaseName).getCollection(collectionName).listIndexes();
            List<Document> existingIndexDocuments = new ArrayList<>();
            existingIndexesIterable.forEach((Consumer<? super Document>) existingIndex -> {
                existingIndex.remove("ns");
                existingIndexDocuments.add(existingIndex);
            });
            return existingIndexDocuments;
        } catch (MongoException e) {
            log.debug("Getting indexes in DB table failed due to " + e.getMessage(), e);
            throw e;
        }
    }

    public static Document getIndex(String uri, String collectionName, String indexName) {
        try {
            List<Document> existingIndexList = getIndexList(uri, collectionName);
            for (Document existingIndex : existingIndexList) {
                if (existingIndex.get("name").equals(indexName)) {
                    return existingIndex;
                }
            }
            return null;
        } catch (MongoException e) {
            log.debug("Getting indexes in DB table failed due to " + e.getMessage(), e);
            throw e;
        }
    }

    public static Document getDocument(String uri, String collectionName, String findFilter) {
        try (MongoClient mongoClient = new MongoClient(new MongoClientURI(uri))) {
            Document findFilterDocument = Document.parse(findFilter);
            Document firstFind = mongoClient.getDatabase(databaseName).getCollection(collectionName)
                    .find(findFilterDocument).first();
            firstFind.remove("_id");
            return firstFind;
        } catch (MongoException e) {
            log.debug("Getting indexes in DB table failed due to " + e.getMessage(), e);
            throw e;
        }
    }

    public static void createCollection(String uri, String collectionName) {
        dropCollection(uri, collectionName);
        try (MongoClient mongoClient = new MongoClient(new MongoClientURI(uri))) {
            mongoClient.getDatabase(databaseName).createCollection(collectionName);
        } catch (MongoException e) {
            log.debug("Getting indexes in DB table failed due to " + e.getMessage(), e);
            throw e;
        }
    }
}


