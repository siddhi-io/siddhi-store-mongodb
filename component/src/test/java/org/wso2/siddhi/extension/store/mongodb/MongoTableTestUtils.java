/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.siddhi.extension.store.mongodb;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoException;
import org.apache.log4j.Logger;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

public class MongoTableTestUtils {

    private static final Logger log = Logger.getLogger(MongoTableTestUtils.class);

    private static final String CONNECTIONURI = "mongodb://admin:admin@127.0.0.1/Foo";
    private static final String DATABASENAME = "Foo";
    private static final String COLLECTIONNAME = "FooTable";

    private MongoTableTestUtils() {

    }

    public static void clearCollection() throws MongoException {
        try (MongoClient mongoClient = new MongoClient(new MongoClientURI(CONNECTIONURI))) {
            mongoClient.getDatabase(DATABASENAME).getCollection(COLLECTIONNAME).drop();
        } catch (MongoException e) {
            log.debug("Clearing DB collection failed due to " + e.getMessage(), e);
        }
    }

    public static long getDocumentsCount() throws MongoException {
        long totalDocumentsInCollection = 0;
        try (MongoClient mongoClient = new MongoClient(new MongoClientURI(CONNECTIONURI))) {
            totalDocumentsInCollection = mongoClient.getDatabase(DATABASENAME).getCollection(COLLECTIONNAME).count();
        } catch (MongoException e) {
            log.debug("Getting rows in DB table failed due to " + e.getMessage(), e);
        }
        return totalDocumentsInCollection;
    }


    public static boolean doesCollectionExists() throws MongoException {
        boolean doesCollectionExists = false;
        try (MongoClient mongoClient = new MongoClient(new MongoClientURI(CONNECTIONURI))) {
            for (String collectionName : mongoClient.getDatabase(DATABASENAME).listCollectionNames()) {
                if (COLLECTIONNAME.equals(collectionName)) {
                    doesCollectionExists = true;
                    break;
                }
            }
        } catch (MongoException e) {
            log.debug("Checking whether collection was created failed due to" + e.getMessage(), e);
        }
        return doesCollectionExists;
    }


    public static List<Document> getIndexList() throws MongoException {
        List<Document> indexesList = new ArrayList<>();
        try (MongoClient mongoClient = new MongoClient(new MongoClientURI(CONNECTIONURI))) {
            for (Document document :
                    mongoClient.getDatabase(DATABASENAME).getCollection(COLLECTIONNAME).listIndexes()) {
                indexesList.add(document);
            }
        } catch (MongoException e) {
            log.debug("Getting indexes in DB table failed due to " + e.getMessage(), e);
        }
        return indexesList;
    }

}
