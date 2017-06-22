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
    private static final String MONGO_CLIENT_URI = "mongodb://admin:admin@127.0.0.1/Foo";
    private static final String DATABASE_NAME = "Foo";
    private static MongoClient mongoClient;

    private MongoTableTestUtils() {
    }

    private static MongoClient getMongoClient() {
        if (mongoClient != null) {
            return mongoClient;
        } else {
            try {
                return mongoClient = new MongoClient(new MongoClientURI(MONGO_CLIENT_URI));
            } catch (Exception e) {
                log.debug("Creating Mongo client failed due to " + e.getMessage(), e);
                throw e;
            }

        }
    }

    public static void dropCollection(String collectionName) {
        try {
            getMongoClient().getDatabase(DATABASE_NAME).getCollection(collectionName).drop();
        } catch (MongoException e) {
            log.debug("Clearing DB collection failed due to " + e.getMessage(), e);
            mongoClient.close();
            throw e;
        }
    }

    public static long getDocumentsCount(String collectionName) {
        try {
            return getMongoClient().getDatabase(DATABASE_NAME).getCollection(collectionName).count();
        } catch (MongoException e) {
            log.debug("Getting rows in DB table failed due to " + e.getMessage(), e);
            mongoClient.close();
            throw e;
        }
    }

    public static boolean doesCollectionExists(String customCollectionName) {
        try {
            for (String collectionName : getMongoClient().getDatabase(DATABASE_NAME).listCollectionNames()) {
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

    public static List<Document> getIndexList(String collectionName) {
        try {
            ListIndexesIterable<Document> existingIndexesIterable =
                    getMongoClient().getDatabase(DATABASE_NAME).getCollection(collectionName).listIndexes();
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
            Document firstFind = getMongoClient().getDatabase(DATABASE_NAME).getCollection(collectionName)
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
            getMongoClient().getDatabase(DATABASE_NAME).createCollection(collectionName);
        } catch (MongoException e) {
            log.debug("Getting indexes in DB table failed due to " + e.getMessage(), e);
            mongoClient.close();
            throw e;
        }
    }
}


