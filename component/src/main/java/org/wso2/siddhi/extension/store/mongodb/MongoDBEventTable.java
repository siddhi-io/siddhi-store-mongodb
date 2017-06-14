/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
package org.wso2.siddhi.extension.store.mongodb;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.DeleteManyModel;
import com.mongodb.client.model.IndexModel;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.UpdateManyModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.bson.Document;
import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.util.DataType;
import org.wso2.siddhi.core.table.record.AbstractRecordTable;
import org.wso2.siddhi.core.table.record.ConditionBuilder;
import org.wso2.siddhi.core.table.record.RecordIterator;
import org.wso2.siddhi.core.util.collection.operator.CompiledCondition;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.extension.store.mongodb.exception.MongoTableException;
import org.wso2.siddhi.extension.store.mongodb.util.MongoTableConstants;
import org.wso2.siddhi.extension.store.mongodb.util.MongoTableUtils;
import org.wso2.siddhi.query.api.annotation.Annotation;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.TableDefinition;
import org.wso2.siddhi.query.api.util.AnnotationHelper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.wso2.siddhi.core.util.SiddhiConstants.ANNOTATION_INDEX_BY;
import static org.wso2.siddhi.core.util.SiddhiConstants.ANNOTATION_PRIMARY_KEY;
import static org.wso2.siddhi.core.util.SiddhiConstants.ANNOTATION_STORE;


/**
 * Class representing MongoDB Event Table implementation.
 */
@Extension(
        name = "mongodb",
        namespace = "store",
        description = "Using this extension a MongoDB Event Table can be configured to persist events " +
                "in a MongoDB of user's choice.",
        parameters = {
                @Parameter(name = "mongodb.uri",
                        description = "The MongoDB URI for the MongoDB data store.",
                        type = {DataType.STRING}),
                @Parameter(name = "collection.name",
                        description = "The name of the collection in the store this Event Table should" +
                                " be persisted as. If not specified, the collection name will be the same as the" +
                                " Siddhi table.",
                        type = {DataType.STRING}, optional = true)
        },
        examples = {
                @Example(
                        syntax = "@Store(type=\"mongodb\"," +
                                "mongodb.uri=\"mongodb://admin:admin@localhost/Foo?ssl=true\")\n" +
                                "@PrimaryKey(\"symbol\")\n" +
                                "@IndexBy(\"volume 1 {background:true,unique:true}\")\n" +
                                "define table FooTable (symbol string, price float, volume long);",
                        description = "This will create a collection called FooTable for the events to be saved " +
                                "with symbol being saved inside _id as Primary Key and index for the field volume " +
                                "will be created in ascending order with the index option to create the index " +
                                "in the background. Prerequisites : 1. A MongoDB server instance should be started.\n" +
                                "2. User should have the necessary privileges and access rights to connect to " +
                                "the MongoDB data store of choice.\n"
                )
        }
)
public class MongoDBEventTable extends AbstractRecordTable {
    private static final Log log = LogFactory.getLog(MongoDBEventTable.class);

    private MongoClientURI mongoClientURI;
    private MongoClient mongoClient;
    private String databaseName;
    private String collectionName;
    private List<String> attributeNames;
    private Map<Integer, String> attributePositions;

    @Override
    protected void init(TableDefinition tableDefinition, ConfigReader configReader) {
        List<Attribute> attributes = tableDefinition.getAttributeList();
        this.attributeNames = new ArrayList<>();
        this.attributePositions = new HashMap<>();
        int count = 0;
        for (Attribute attribute : attributes) {
            this.attributePositions.put(count, attribute.getName());
            this.attributeNames.add(attribute.getName());
            count++;
        }

        Annotation storeAnnotation = AnnotationHelper
                .getAnnotation(ANNOTATION_STORE, tableDefinition.getAnnotations());
        Annotation primaryKeys = AnnotationHelper
                .getAnnotation(ANNOTATION_PRIMARY_KEY, tableDefinition.getAnnotations());
        Annotation indices = AnnotationHelper
                .getAnnotation(ANNOTATION_INDEX_BY, tableDefinition.getAnnotations());

        this.initializeConnectionParameters(storeAnnotation);

        List<IndexModel> expectedIndexModels = new ArrayList<>();
        IndexModel primaryKey = MongoTableUtils.extractPrimaryKey(primaryKeys, this.attributeNames);
        if (primaryKey != null) {
            expectedIndexModels.add(primaryKey);
        }
        expectedIndexModels.addAll(MongoTableUtils.extractIndexModels(indices, this.attributeNames));

        String customCollectionName = storeAnnotation.getElement(
                MongoTableConstants.ANNOTATION_ELEMENT_COLLECTION_NAME);
        this.collectionName = MongoTableUtils.isEmpty(customCollectionName) ?
                tableDefinition.getId() : customCollectionName;

        if (!this.collectionExists()) {
            try {
                this.getDatabaseObject().createCollection(this.collectionName);
                this.createIndices(expectedIndexModels);
            } catch (MongoException e) {
                this.mongoClient.close();
                throw new MongoTableException("Creating mongo collection '" + this.collectionName
                        + "' is not successful due to " + e.getLocalizedMessage(), e);
            }
        } else {
            MongoCursor<Document> existingIndicesIterator = this.getCollectionObject().listIndexes().iterator();
            MongoTableUtils.checkExistingIndices(expectedIndexModels, existingIndicesIterator);
        }
    }

    /**
     * Method for initializing mongoClientURI and database name.
     *
     * @param storeAnnotation the source annotation which contains the needed parameters.
     * @throws MongoTableException when store annotation does not contain mongodb.uri or contains an illegal
     *                             argument for mongodb.uri
     */
    private void initializeConnectionParameters(Annotation storeAnnotation) {
        String mongoClientURI = storeAnnotation.getElement(MongoTableConstants.ANNOTATION_ELEMENT_URI);
        if (mongoClientURI != null) {
            try {
                this.mongoClientURI = new MongoClientURI(mongoClientURI);
                this.databaseName = this.mongoClientURI.getDatabase();
            } catch (IllegalArgumentException e) {
                throw new MongoTableException("Annotation '" + storeAnnotation.getName() + "' contains illegal " +
                        "value for 'mongodb.uri' as '" + mongoClientURI + "'. Please check your query and try " +
                        "again.", e);
            }
        } else {
            throw new MongoTableException("Annotation '" + storeAnnotation.getName() +
                    "' must contain the element 'mongodb.uri'. Please check your query and try again.");
        }
    }

    /**
     * Method for checking if the collection exists or not.
     *
     * @return <code>true</code> if the collection exists
     * <code>false</code> otherwise
     * @throws MongoTableException if lookup fails.
     */
    private boolean collectionExists() {
        try {
            for (String collectionName : this.getDatabaseObject().listCollectionNames()) {
                if (this.collectionName.equals(collectionName)) {
                    return true;
                }
            }
            return false;
        } catch (MongoException e) {
            throw new MongoTableException("Error in retrieving collection names from the database '"
                    + this.databaseName + "' : " + e.getLocalizedMessage(), e);
        }
    }

    /**
     * Method for returning a database object.
     *
     * @return a new {@link MongoDatabase} instance from the Mongo client.
     */
    private MongoDatabase getDatabaseObject() {
        return this.getConnection().getDatabase(this.databaseName);
    }

    /**
     * Method for returning a collection object.
     *
     * @return a new {@link MongoCollection} instance from the Mongo client.
     */
    private MongoCollection<Document> getCollectionObject() {
        return this.getConnection().getDatabase(this.databaseName).getCollection(this.collectionName);
    }

    /**
     * Method for returning a mongo client.
     *
     * @return a {@link MongoClient} instance.
     * @throws MongoTableException when the mongodb.uri contain illegal value
     */
    private MongoClient getConnection() {
        if (this.mongoClient != null) {
            return this.mongoClient;
        } else {
            try {
                this.mongoClient = new MongoClient(this.mongoClientURI);
            } catch (MongoException e) {
                throw new MongoTableException("Annotation 'Store' contains illegal value for " +
                        "element 'mongodb.uri' as '" + this.mongoClientURI + "'. Please check " +
                        "your query and try again.", e);
            }
            return this.mongoClient;
        }
    }

    /**
     * Method for creating indices on the collection.
     */
    private void createIndices(List<IndexModel> indexModels) {
        if (!indexModels.isEmpty()) {
            this.getCollectionObject().createIndexes(indexModels);
        }
    }

    /**
     * Method for doing bulk write operations on the collection.
     *
     * @param parsedRecords a List of WriteModels to be applied
     * @throws MongoTableException if the write fails
     */
    private void bulkWrite(List<? extends WriteModel<Document>> parsedRecords) {
        try {
            if (!parsedRecords.isEmpty()) {
                this.getCollectionObject().bulkWrite(parsedRecords);
            }
        } catch (MongoBulkWriteException e) {
            List<com.mongodb.bulk.BulkWriteError> writeErrors = e.getWriteErrors();
            int failedIndex;
            Object failedModel;
            for (com.mongodb.bulk.BulkWriteError bulkWriteError : writeErrors) {
                failedIndex = bulkWriteError.getIndex();
                failedModel = parsedRecords.get(failedIndex);
                if (failedModel instanceof UpdateManyModel) {
                    log.error("The update filter '" + ((UpdateManyModel) failedModel).getFilter().toString() +
                            "' failed to update with event '" + ((UpdateManyModel) failedModel).getUpdate().toString() +
                            "' in the MongoDB Event Table due to " + bulkWriteError.getMessage());
                } else {
                    if (failedModel instanceof InsertOneModel) {
                        log.error("The event '" + ((InsertOneModel) failedModel).getDocument().toString() +
                                "' failed to insert into the Mongo Event Table due to " + bulkWriteError.getMessage());
                    } else {

                        log.error("The delete filter '" + ((DeleteManyModel) failedModel).getFilter().toString() +
                                "' failed to delete the events from the MongoDB Event Table due to "
                                + bulkWriteError.getMessage());
                    }
                }
                if (failedIndex + 1 < parsedRecords.size()) {
                    this.bulkWrite(parsedRecords.subList(failedIndex + 1, parsedRecords.size() - 1));
                }
            }
        } catch (MongoException e) {
            throw new MongoTableException("Error in writing to the collection '"
                    + this.collectionName + "' : " + e.getLocalizedMessage(), e);
        }
    }

    @Override
    protected void add(List<Object[]> records) {
        List<InsertOneModel<Document>> parsedRecords = records.stream().map(record -> {
            Map<String, Object> insertMap = MongoTableUtils.mapValuesToAttributes(record, this.attributePositions);
            Document insertDocument = new Document(insertMap);
            if (log.isDebugEnabled()) {
                log.debug("Event formatted as document '" + insertDocument.toJson() + "' is used for building " +
                        "Mongo Insert Model");
            }
            return new InsertOneModel<>(insertDocument);
        }).collect(Collectors.toList());
        this.bulkWrite(parsedRecords);
    }

    @Override
    protected RecordIterator<Object[]> find(Map<String, Object> findConditionParameterMap,
                                            CompiledCondition compiledCondition) {
        try {
            Document findFilter = MongoTableUtils
                    .resolveCondition((MongoCompiledCondition) compiledCondition, findConditionParameterMap);
            MongoCollection<? extends Document> mongoCollection = this.getCollectionObject();
            return new MongoIterator(mongoCollection.find(findFilter), this.attributeNames);
        } catch (MongoException e) {
            throw new MongoTableException("Error in retrieving documents from the collection '"
                    + this.collectionName + "' : " + e.getLocalizedMessage(), e);
        }
    }

    @Override
    protected boolean contains(Map<String, Object> containsConditionParameterMap, CompiledCondition
            compiledCondition) {
        try {
            Document containsFilter = MongoTableUtils
                    .resolveCondition((MongoCompiledCondition) compiledCondition, containsConditionParameterMap);
            return this.getCollectionObject().count(containsFilter) > 0;
        } catch (MongoException e) {
            throw new MongoTableException("Error in retrieving count of documents from the collection '"
                    + this.collectionName + "' : " + e.getLocalizedMessage(), e);
        }
    }

    @Override
    protected void delete(List<Map<String, Object>> deleteConditionParameterMaps, CompiledCondition compiledCondition) {
        List<DeleteManyModel<Document>> parsedRecords = deleteConditionParameterMaps.stream().map(
                (Map<String, Object> conditionParameterMap) -> {
                    Document deleteFilter = MongoTableUtils
                            .resolveCondition((MongoCompiledCondition) compiledCondition, conditionParameterMap);
                    return new DeleteManyModel<Document>(deleteFilter);
                }).collect(Collectors.toList());
        this.bulkWrite(parsedRecords);
    }

    @Override
    protected void update(List<Map<String, Object>> updateConditionParameterMaps,
                          CompiledCondition compiledCondition, List<Map<String, Object>> updateValues) {
        List<UpdateManyModel<Document>> parsedRecords = updateConditionParameterMaps.stream().map(
                conditionParameterMap -> {
                    int ordinal = updateConditionParameterMaps.indexOf(conditionParameterMap);
                    Document updateFilter = MongoTableUtils
                            .resolveCondition((MongoCompiledCondition) compiledCondition, conditionParameterMap);
                    Document updateDocument = new Document()
                            .append("$set", updateValues.get(ordinal));
                    return new UpdateManyModel<Document>(updateFilter, updateDocument);
                }).collect(Collectors.toList());
        this.bulkWrite(parsedRecords);
    }

    @Override
    protected void updateOrAdd(List<Map<String, Object>> updateConditionParameterMaps,
                               CompiledCondition compiledCondition, List<Map<String, Object>> updateValues,
                               List<Object[]> addingRecords) {
        List<UpdateManyModel<Document>> parsedRecords = updateConditionParameterMaps.stream().map(
                conditionParameterMap -> {
                    int ordinal = updateConditionParameterMaps.indexOf(conditionParameterMap);
                    Document updateFilter = MongoTableUtils
                            .resolveCondition((MongoCompiledCondition) compiledCondition, conditionParameterMap);
                    Document updateDocument = new Document()
                            .append("$set", updateValues.get(ordinal));
                    UpdateOptions updateOptions = new UpdateOptions().upsert(true);
                    return new UpdateManyModel<Document>(updateFilter, updateDocument, updateOptions);
                }).collect(Collectors.toList());
        this.bulkWrite(parsedRecords);
    }

    @Override
    protected CompiledCondition compileCondition(ConditionBuilder conditionBuilder) {
        MongoConditionVisitor visitor = new MongoConditionVisitor();
        conditionBuilder.build(visitor);
        return new MongoCompiledCondition(visitor.getCompiledCondition(), visitor.getPlaceholders());
    }
}
