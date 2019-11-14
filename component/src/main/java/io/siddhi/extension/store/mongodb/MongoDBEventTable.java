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
package io.siddhi.extension.store.mongodb;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoException;
import com.mongodb.MongoSocketOpenException;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.DeleteManyModel;
import com.mongodb.client.model.IndexModel;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.UpdateManyModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import io.siddhi.annotation.Example;
import io.siddhi.annotation.Extension;
import io.siddhi.annotation.Parameter;
import io.siddhi.annotation.SystemParameter;
import io.siddhi.annotation.util.DataType;
import io.siddhi.core.exception.ConnectionUnavailableException;
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.table.record.AbstractQueryableRecordTable;
import io.siddhi.core.table.record.ExpressionBuilder;
import io.siddhi.core.table.record.RecordIterator;
import io.siddhi.core.util.collection.operator.CompiledCondition;
import io.siddhi.core.util.collection.operator.CompiledExpression;
import io.siddhi.core.util.collection.operator.CompiledSelection;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.extension.store.mongodb.exception.MongoTableException;
import io.siddhi.extension.store.mongodb.util.MongoTableConstants;
import io.siddhi.extension.store.mongodb.util.MongoTableUtils;
import io.siddhi.query.api.annotation.Annotation;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.definition.TableDefinition;
import io.siddhi.query.api.util.AnnotationHelper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.siddhi.core.util.SiddhiConstants.ANNOTATION_INDEX;
import static io.siddhi.core.util.SiddhiConstants.ANNOTATION_INDEX_BY;
import static io.siddhi.core.util.SiddhiConstants.ANNOTATION_PRIMARY_KEY;
import static io.siddhi.core.util.SiddhiConstants.ANNOTATION_STORE;


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
                        description = "The MongoDB URI for the MongoDB data store. The uri must be of the format \n" +
                                "mongodb://[username:password@]host1[:port1][,hostN[:portN]][/[database][?options]]\n" +
                                "The options specified in the uri will override any connection options specified in " +
                                "the deployment yaml file.\n Note: The user should have read permissions to the admin" +
                                "db as well as read/write permissions to the database accessed.",
                        type = {DataType.STRING}),
                @Parameter(name = "collection.name",
                        description = "The name of the collection in the store this Event Table should" +
                                " be persisted as.",
                        optional = true,
                        defaultValue = "Name of the siddhi event table.",
                        type = {DataType.STRING}),
                @Parameter(name = "secure.connection",
                        description = "Describes enabling the SSL for the mongodb connection",
                        optional = true,
                        defaultValue = "false",
                        type = {DataType.STRING}),
                @Parameter(name = "trust.store",
                        description = "File path to the trust store.",
                        optional = true,
                        defaultValue = "${carbon.home}/resources/security/client-truststore.jks",
                        type = {DataType.STRING}),
                @Parameter(name = "trust.store.password",
                        description = "Password to access the trust store",
                        optional = true,
                        defaultValue = "wso2carbon",
                        type = {DataType.STRING}),
                @Parameter(name = "key.store",
                        description = "File path to the keystore.",
                        optional = true,
                        defaultValue = "${carbon.home}/resources/security/client-truststore.jks",
                        type = {DataType.STRING}),
                @Parameter(name = "key.store.password",
                        description = "Password to access the keystore",
                        optional = true,
                        defaultValue = "wso2carbon",
                        type = {DataType.STRING})
        },
        systemParameter = {
                @SystemParameter(name = "applicationName",
                        description = "Sets the logical name of the application using this MongoClient. The " +
                                "application name may be used by the client to identify the application to " +
                                "the server, for use in server logs, slow query logs, and profile collection.",
                        defaultValue = "null",
                        possibleParameters = "the logical name of the application using this MongoClient. The " +
                                "UTF-8 encoding may not exceed 128 bytes."),
                @SystemParameter(name = "cursorFinalizerEnabled",
                        description = "Sets whether cursor finalizers are enabled.",
                        defaultValue = "true",
                        possibleParameters = {"true", "false"}),
                @SystemParameter(name = "requiredReplicaSetName",
                        description = "The name of the replica set",
                        defaultValue = "null",
                        possibleParameters = "the logical name of the replica set"),
                @SystemParameter(name = "sslEnabled",
                        description = "Sets whether to initiate connection with TSL/SSL enabled. true: Initiate " +
                                "the connection with TLS/SSL. false: Initiate the connection without TLS/SSL.",
                        defaultValue = "false",
                        possibleParameters = {"true", "false"}),
                @SystemParameter(name = "trustStore",
                        description = "File path to the trust store.",
                        defaultValue = "${carbon.home}/resources/security/client-truststore.jks",
                        possibleParameters = "Any valid file path."),
                @SystemParameter(name = "trustStorePassword",
                        description = "Password to access the trust store",
                        defaultValue = "wso2carbon",
                        possibleParameters = "Any valid password."),
                @SystemParameter(name = "keyStore",
                        description = "File path to the keystore.",
                        defaultValue = "${carbon.home}/resources/security/client-truststore.jks",
                        possibleParameters = "Any valid file path."),
                @SystemParameter(name = "keyStorePassword",
                        description = "Password to access the keystore",
                        defaultValue = "wso2carbon",
                        possibleParameters = "Any valid password."),
                @SystemParameter(name = "connectTimeout",
                        description = "The time in milliseconds to attempt a connection before timing out.",
                        defaultValue = "10000",
                        possibleParameters = "Any positive integer"),
                @SystemParameter(name = "connectionsPerHost",
                        description = "The maximum number of connections in the connection pool.",
                        defaultValue = "100",
                        possibleParameters = "Any positive integer"),
                @SystemParameter(name = "minConnectionsPerHost",
                        description = "The minimum number of connections in the connection pool.",
                        defaultValue = "0",
                        possibleParameters = "Any natural number"),
                @SystemParameter(name = "maxConnectionIdleTime",
                        description = "The maximum number of milliseconds that a connection can remain idle in " +
                                "the pool before being removed and closed. A zero value indicates no limit to " +
                                "the idle time.  A pooled connection that has exceeded its idle time will be " +
                                "closed and replaced when necessary by a new connection.",
                        defaultValue = "0",
                        possibleParameters = "Any positive integer"),
                @SystemParameter(name = "maxWaitTime",
                        description = "The maximum wait time in milliseconds that a thread may wait for a connection " +
                                "to become available. A value of 0 means that it will not wait.  A negative value " +
                                "means to wait indefinitely",
                        defaultValue = "120000",
                        possibleParameters = "Any integer"),
                @SystemParameter(name = "threadsAllowedToBlockForConnectionMultiplier",
                        description = "The maximum number of connections allowed per host for this MongoClient " +
                                "instance. Those connections will be kept in a pool when idle. Once the pool " +
                                "is exhausted, any operation requiring a connection will block waiting for an " +
                                "available connection.",
                        defaultValue = "100",
                        possibleParameters = "Any natural number"),
                @SystemParameter(name = "maxConnectionLifeTime",
                        description = "The maximum life time of a pooled connection.  A zero value indicates " +
                                "no limit to the life time.  A pooled connection that has exceeded its life time " +
                                "will be closed and replaced when necessary by a new connection.",
                        defaultValue = "0",
                        possibleParameters = "Any positive integer"),
                @SystemParameter(name = "socketKeepAlive",
                        description = "Sets whether to keep a connection alive through firewalls",
                        defaultValue = "false",
                        possibleParameters = {"true", "false"}),
                @SystemParameter(name = "socketTimeout",
                        description = "The time in milliseconds to attempt a send or receive on a socket " +
                                "before the attempt times out. Default 0 means never to timeout.",
                        defaultValue = "0",
                        possibleParameters = "Any natural integer"),
                @SystemParameter(name = "writeConcern",
                        description = "The write concern to use.",
                        defaultValue = "acknowledged",
                        possibleParameters = {"acknowledged", "w1", "w2", "w3", "unacknowledged", "fsynced",
                                "journaled", "replica_acknowledged", "normal", "safe", "majority", "fsync_safe",
                                "journal_safe", "replicas_safe"}),
                @SystemParameter(name = "readConcern",
                        description = "The level of isolation for the reads from replica sets.",
                        defaultValue = "default",
                        possibleParameters = {"local", "majority", "linearizable"}),
                @SystemParameter(name = "readPreference",
                        description = "Specifies the replica set read preference for the connection.",
                        defaultValue = "primary",
                        possibleParameters = {"primary", "secondary", "secondarypreferred", "primarypreferred",
                                "nearest"}),
                @SystemParameter(name = "localThreshold",
                        description = "The size (in milliseconds) of the latency window for selecting among " +
                                "multiple suitable MongoDB instances.",
                        defaultValue = "15",
                        possibleParameters = "Any natural number"),
                @SystemParameter(name = "serverSelectionTimeout",
                        description = "Specifies how long (in milliseconds) to block for server selection " +
                                "before throwing an exception. A value of 0 means that it will timeout immediately " +
                                "if no server is available.  A negative value means to wait indefinitely.",
                        defaultValue = "30000",
                        possibleParameters = "Any integer"),
                @SystemParameter(name = "heartbeatSocketTimeout",
                        description = "The socket timeout for connections used for the cluster heartbeat. A value of " +
                                "0 means that it will timeout immediately if no cluster member is available.  " +
                                "A negative value means to wait indefinitely.",
                        defaultValue = "20000",
                        possibleParameters = "Any integer"),
                @SystemParameter(name = "heartbeatConnectTimeout",
                        description = "The connect timeout for connections used for the cluster heartbeat. A value " +
                                "of 0 means that it will timeout immediately if no cluster member is available.  " +
                                "A negative value means to wait indefinitely.",
                        defaultValue = "20000",
                        possibleParameters = "Any integer"),
                @SystemParameter(name = "heartbeatFrequency",
                        description = "Specify the interval (in milliseconds) between checks, counted from " +
                                "the end of the previous check until the beginning of the next one.",
                        defaultValue = "10000",
                        possibleParameters = "Any positive integer"),
                @SystemParameter(name = "minHeartbeatFrequency",
                        description = "Sets the minimum heartbeat frequency.  In the event that the driver " +
                                "has to frequently re-check a server's availability, it will wait at least this " +
                                "long since the previous check to avoid wasted effort.",
                        defaultValue = "500",
                        possibleParameters = "Any positive integer")
        },
        examples = {
                @Example(
                        syntax = "@Store(type=\"mongodb\"," +
                                "mongodb.uri=\"mongodb://admin:admin@localhost/Foo\")\n" +
                                "@PrimaryKey(\"symbol\")\n" +
                                "@Index(\"volume:1\", {background:true,unique:true}\")\n" +
                                "define table FooTable (symbol string, price float, volume long);",
                        description = "This will create a collection called FooTable for the events to be saved " +
                                "with symbol as Primary Key(unique index at mongoDB level) and index for the field " +
                                "volume will be created in ascending order with the index option to create the index " +
                                "in the background.\n\n" +
                                "Note: \n" +
                                "@PrimaryKey: This specifies a list of comma-separated values to be treated as " +
                                "unique fields in the table. Each record in the table must have a unique combination " +
                                "of values for the fields specified here.\n\n" +
                                "@Index: This specifies the fields that must be indexed at the database level. " +
                                "You can specify multiple values as a come-separated list. A single value to be in " +
                                "the format,\n`<FieldName>:<SortOrder>`. The last element is optional through which " +
                                "a valid index options can be passed.\n" +
                                "\t\t<SortOrder> : 1 for Ascending & -1 for Descending. " +
                                "Optional, with default value as 1.\n" +
                                "\t\t<IndexOptions> : Index Options must be defined inside curly brackets.\n" +
                                "\t\t\tOptions must follow the standard mongodb index options format.\n" +
                                "\t\t\thttps://docs.mongodb.com/manual/reference/method/db.collection.createIndex/" +
                                "\n\n" +
                                "Example 1: @Index(`'symbol:1'`, `'{\"unique\":true}'`)\n" +
                                "Example 2: @Index(`'symbol'`, `'{\"unique\":true}'`)\n" +
                                "Example 3: @Index(`'symbol:1'`, `'volume:-1'`, `'{\"unique\":true}'`)\n"

                )
        }
)
public class MongoDBEventTable extends AbstractQueryableRecordTable {
    private static final Log log = LogFactory.getLog(MongoDBEventTable.class);

    private MongoClientURI mongoClientURI;
    private MongoClient mongoClient;
    private String databaseName;
    private String collectionName;
    private List<String> attributeNames;
    private ArrayList<IndexModel> expectedIndexModels;
    private boolean initialCollectionTest;

    @Override
    protected void init(TableDefinition tableDefinition, ConfigReader configReader) {
        this.attributeNames =
                tableDefinition.getAttributeList().stream().map(Attribute::getName).collect(Collectors.toList());

        Annotation storeAnnotation = AnnotationHelper
                .getAnnotation(ANNOTATION_STORE, tableDefinition.getAnnotations());
        Annotation primaryKeys = AnnotationHelper
                .getAnnotation(ANNOTATION_PRIMARY_KEY, tableDefinition.getAnnotations());

        this.initializeConnectionParameters(storeAnnotation, configReader);

        String customCollectionName = storeAnnotation.getElement(
                MongoTableConstants.ANNOTATION_ELEMENT_COLLECTION_NAME);
        this.collectionName = MongoTableUtils.isEmpty(customCollectionName) ?
                tableDefinition.getId() : customCollectionName;
        this.initialCollectionTest = false;

        this.expectedIndexModels = new ArrayList<>();
        IndexModel primaryKey = MongoTableUtils.extractPrimaryKey(primaryKeys, this.attributeNames);
        if (primaryKey != null) {
            this.expectedIndexModels.add(primaryKey);
        }

        List<Annotation> indices = AnnotationHelper
                .getAnnotations(ANNOTATION_INDEX, tableDefinition.getAnnotations());
        if (!indices.isEmpty()) {
            this.expectedIndexModels.addAll(MongoTableUtils.extractIndexModels(indices, this.attributeNames,
                    this.collectionName));
        } else {
            Annotation indexBy = AnnotationHelper
                    .getAnnotation(ANNOTATION_INDEX_BY, tableDefinition.getAnnotations());
            this.expectedIndexModels.addAll(MongoTableUtils.extractIndexModels(indexBy, this.attributeNames,
                    this.collectionName));
        }
    }

    /**
     * Method for initializing mongoClientURI and database name.
     *
     * @param storeAnnotation the source annotation which contains the needed parameters.
     * @param configReader    {@link ConfigReader} ConfigurationReader.
     * @throws MongoTableException when store annotation does not contain mongodb.uri or contains an illegal
     *                             argument for mongodb.uri
     */
    private void initializeConnectionParameters(Annotation storeAnnotation, ConfigReader configReader) {
        String mongoClientURI = storeAnnotation.getElement(MongoTableConstants.ANNOTATION_ELEMENT_URI);
        if (mongoClientURI != null) {
            MongoClientOptions.Builder mongoClientOptionsBuilder =
                    MongoTableUtils.extractMongoClientOptionsBuilder(storeAnnotation, configReader);
            try {
                this.mongoClientURI = new MongoClientURI(mongoClientURI, mongoClientOptionsBuilder);
                this.databaseName = this.mongoClientURI.getDatabase();
            } catch (IllegalArgumentException e) {
                throw new SiddhiAppCreationException("Annotation '" + storeAnnotation.getName() + "' contains " +
                        "illegal value for 'mongodb.uri' as '" + mongoClientURI + "'. Please check your query and " +
                        "try again.", e);
            }
        } else {
            throw new SiddhiAppCreationException("Annotation '" + storeAnnotation.getName() +
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
    private boolean collectionExists() throws ConnectionUnavailableException {
        try {
            for (String collectionName : this.getDatabaseObject().listCollectionNames()) {
                if (this.collectionName.equals(collectionName)) {
                    return true;
                }
            }
            return false;
        } catch (MongoSocketOpenException e) {
            throw new ConnectionUnavailableException(e);
        } catch (MongoException e) {
            this.destroy();
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
        if (this.mongoClient == null) {
            try {
                this.mongoClient = new MongoClient(this.mongoClientURI);
            } catch (MongoException e) {
                throw new SiddhiAppCreationException("Annotation 'Store' contains illegal value for " +
                        "element 'mongodb.uri' as '" + this.mongoClientURI + "'. Please check " +
                        "your query and try again.", e);
            }
        }
        return this.mongoClient.getDatabase(this.databaseName);
    }

    /**
     * Method for returning a collection object.
     *
     * @return a new {@link MongoCollection} instance from the Mongo client.
     */
    private MongoCollection<Document> getCollectionObject() {
        return this.getDatabaseObject().getCollection(this.collectionName);
    }

    /**
     * Method for creating indices on the collection.
     */
    private void createIndices(List<IndexModel> indexModels) throws ConnectionUnavailableException {
        if (!indexModels.isEmpty()) {
            try {
                this.getCollectionObject().createIndexes(indexModels);
            } catch (MongoSocketOpenException e) {
                throw new ConnectionUnavailableException(e);
            } catch (MongoException e) {
                this.destroy();
                throw new MongoTableException("Error in creating indices in the database '"
                        + this.collectionName + "' : " + e.getLocalizedMessage(), e);
            }
        }
    }

    /**
     * Method for doing bulk write operations on the collection.
     *
     * @param parsedRecords a List of WriteModels to be applied
     * @throws MongoTableException if the write fails
     */
    private void bulkWrite(List<? extends WriteModel<Document>> parsedRecords) throws ConnectionUnavailableException {
        try {
            if (!parsedRecords.isEmpty()) {
                this.getCollectionObject().bulkWrite(parsedRecords);
            }
        } catch (MongoSocketOpenException e) {
            throw new ConnectionUnavailableException(e);
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
            this.destroy();
            throw new MongoTableException("Error in writing to the collection '"
                    + this.collectionName + "' : " + e.getLocalizedMessage(), e);
        }
    }

    @Override
    protected void add(List<Object[]> records) throws ConnectionUnavailableException {
        List<InsertOneModel<Document>> parsedRecords = records.stream().map(record -> {
            Map<String, Object> insertMap = MongoTableUtils.mapValuesToAttributes(record, this.attributeNames);
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
                                            CompiledCondition compiledCondition)
            throws ConnectionUnavailableException {
        try {
            Document findFilter = MongoTableUtils
                    .resolveCondition((MongoCompiledCondition) compiledCondition, findConditionParameterMap);
            MongoCollection<? extends Document> mongoCollection = this.getCollectionObject();
            return new MongoIterator(mongoCollection.find(findFilter), this.attributeNames);
        } catch (MongoException e) {
            this.destroy();
            throw new MongoTableException("Error in retrieving documents from the collection '"
                    + this.collectionName + "' : " + e.getLocalizedMessage(), e);
        }
    }

    @Override
    protected boolean contains(Map<String, Object> containsConditionParameterMap, CompiledCondition
            compiledCondition) throws ConnectionUnavailableException {
        try {
            Document containsFilter = MongoTableUtils
                    .resolveCondition((MongoCompiledCondition) compiledCondition, containsConditionParameterMap);
            return this.getCollectionObject().count(containsFilter) > 0;
        } catch (MongoException e) {
            this.destroy();
            throw new MongoTableException("Error in retrieving count of documents from the collection '"
                    + this.collectionName + "' : " + e.getLocalizedMessage(), e);
        }
    }

    @Override
    protected void delete(List<Map<String, Object>> deleteConditionParameterMaps, CompiledCondition compiledCondition)
            throws ConnectionUnavailableException {
        List<DeleteManyModel<Document>> parsedRecords = deleteConditionParameterMaps.stream().map(
                (Map<String, Object> conditionParameterMap) -> {
                    Document deleteFilter = MongoTableUtils
                            .resolveCondition((MongoCompiledCondition) compiledCondition, conditionParameterMap);
                    return new DeleteManyModel<Document>(deleteFilter);
                }).collect(Collectors.toList());
        this.bulkWrite(parsedRecords);
    }

    @Override
    protected void update(CompiledCondition compiledCondition,
                          List<Map<String, Object>> list,
                          Map<String, CompiledExpression> map,
                          List<Map<String, Object>> list1) throws ConnectionUnavailableException {
        List<UpdateManyModel<Document>> parsedRecords = list.stream().map(
                conditionParameterMap -> {
                    int ordinal = list.indexOf(conditionParameterMap);
                    Document updateFilter = MongoTableUtils
                            .resolveCondition((MongoCompiledCondition) compiledCondition, conditionParameterMap);
                    Document updateDocument = new Document()
                            .append("$set", list1.get(ordinal));
                    return new UpdateManyModel<Document>(updateFilter, updateDocument);
                }).collect(Collectors.toList());
        this.bulkWrite(parsedRecords);
    }

    @Override
    protected void updateOrAdd(CompiledCondition compiledCondition,
                               List<Map<String, Object>> list,
                               Map<String, CompiledExpression> map,
                               List<Map<String, Object>> list1,
                               List<Object[]> list2) throws ConnectionUnavailableException {
        List<UpdateManyModel<Document>> parsedRecords = list.stream().map(
                conditionParameterMap -> {
                    int ordinal = list.indexOf(conditionParameterMap);
                    Document updateFilter = MongoTableUtils
                            .resolveCondition((MongoCompiledCondition) compiledCondition, conditionParameterMap);
                    Document updateDocument = new Document()
                            .append("$set", list1.get(ordinal));
                    UpdateOptions updateOptions = new UpdateOptions().upsert(true);
                    return new UpdateManyModel<Document>(updateFilter, updateDocument, updateOptions);
                }).collect(Collectors.toList());
        this.bulkWrite(parsedRecords);
    }

    @Override
    protected CompiledCondition compileCondition(ExpressionBuilder expressionBuilder) {
        MongoExpressionVisitor visitor = new MongoExpressionVisitor();
        expressionBuilder.build(visitor);
        return new MongoCompiledCondition(visitor.getCompiledCondition(), visitor.getPlaceholders());
    }

    @Override
    protected CompiledCondition compileSetAttribute(ExpressionBuilder expressionBuilder) {
        MongoSetExpressionVisitor visitor = new MongoSetExpressionVisitor();
        expressionBuilder.build(visitor);
        return new MongoCompiledCondition(visitor.getCompiledCondition(), visitor.getPlaceholders());
    }

    @Override
    protected void connect() throws ConnectionUnavailableException {
        if (!this.initialCollectionTest) {
            if (!this.collectionExists()) {
                try {
                    this.getDatabaseObject().createCollection(this.collectionName);
                    this.createIndices(expectedIndexModels);
                } catch (MongoSocketOpenException e) {
                    throw new ConnectionUnavailableException(e);
                } catch (MongoException e) {
                    this.destroy();
                    throw new MongoTableException("Creating mongo collection '" + this.collectionName
                            + "' is not successful due to " + e.getLocalizedMessage(), e);
                }
            } else {
                MongoCursor<Document> existingIndicesIterator;
                try {
                    existingIndicesIterator = this.getCollectionObject().listIndexes().iterator();
                } catch (MongoSocketOpenException e) {
                    throw new ConnectionUnavailableException(e);
                } catch (MongoException e) {
                    this.destroy();
                    throw new MongoTableException("Retrieving indexes from  mongo collection '" + this.collectionName
                            + "' is not successful due to " + e.getLocalizedMessage(), e);
                }
                MongoTableUtils.checkExistingIndices(expectedIndexModels, existingIndicesIterator);
            }
            this.initialCollectionTest = true;
        } else {
            try {
                this.getDatabaseObject().listCollectionNames();
            } catch (MongoSocketOpenException e) {
                throw new ConnectionUnavailableException(e);
            }
        }
    }

    @Override
    protected void disconnect() {
    }

    @Override
    protected void destroy() {
        if (this.mongoClient != null) {
            this.mongoClient.close();
        }
    }

    @Override
    protected RecordIterator<Object[]> query(Map<String, Object> parameterMap, CompiledCondition compiledCondition,
                                             CompiledSelection compiledSelection, Attribute[] outputAttributes)
            throws ConnectionUnavailableException {

        List<Document> aggregateList = new ArrayList<>();
        List<String> attributeList = new ArrayList<>();

        Document findFilter = MongoTableUtils
                .resolveCondition((MongoCompiledCondition) compiledCondition, parameterMap);
        String selectQuery = ((MongoDBCompileSelection)compiledSelection).getCompileSelectQuery();
        String groupByQuery = ((MongoDBCompileSelection) compiledSelection).getGroupby();
        String havingQuery = ((MongoDBCompileSelection) compiledSelection).getHaving();
        String orderByQuery = ((MongoDBCompileSelection) compiledSelection).getOrderby();
        Long limit = ((MongoDBCompileSelection)compiledSelection).getLimit();
        Long offset = ((MongoDBCompileSelection)compiledSelection).getOffset();

        if(parameterMap.values().size()>0){
            for(int i=0;i<parameterMap.values().size();i++){
                if(selectQuery!=null){
                    selectQuery = selectQuery.replaceAll("\'"+parameterMap.keySet().toArray()[i]+"\'",
                            ""+parameterMap.values().toArray()[i]);
                }
            }
        }
        if(!findFilter.isEmpty()){
            Document matchFilter = new Document("$match",findFilter);
            if (log.isDebugEnabled()) {
                log.debug("On condition query : '" + matchFilter.toJson());
            }
            aggregateList.add(matchFilter);
        }
        if(groupByQuery != null){
            Document groupBy = Document.parse(groupByQuery);
            if (log.isDebugEnabled()) {
                log.debug("GroupBy query : '" + groupBy.toJson());
            }
            aggregateList.add(groupBy);
        }
        if(selectQuery != null){
            Document project = Document.parse(selectQuery);
            if (log.isDebugEnabled()) {
                log.debug("Select query : '" + project.toJson());
            }
            aggregateList.add(project);
        }
        if(havingQuery != null){
            Document having = Document.parse(havingQuery);
            if (log.isDebugEnabled()) {
                log.debug("Having query : '" + having.toJson());
            }
            aggregateList.add(having);
        }
        if(orderByQuery != null){
            Document orderBy = Document.parse(orderByQuery);
            if (log.isDebugEnabled()) {
                log.debug("OrderBy query : '" + orderBy.toJson());
            }
            aggregateList.add(orderBy);
        }
        if(offset != null){
            Document offsetFilter = new Document("$skip",offset);
            if (log.isDebugEnabled()) {
                log.debug("Offset query : '" + offsetFilter.toJson());
            }
            aggregateList.add(offsetFilter);
        }
        if(limit != null){
            Document limitFilter = new Document("$limit",limit);
            if (log.isDebugEnabled()) {
                log.debug("Limit query : '" + limitFilter.toJson());
            }
            aggregateList.add(limitFilter);
        }
        for (int i=0;i<outputAttributes.length;i++){
            attributeList.add(outputAttributes[i].getName());
        }
        AggregateIterable<Document> aggregate = this.getCollectionObject().aggregate(aggregateList);
        if (log.isDebugEnabled()) {
            MongoCursor<Document> iterator = aggregate.iterator();
            while (iterator.hasNext()) {
                log.debug("Result : "+iterator.next().toJson());
            }
        }
        return new MongoIterator(aggregate, attributeList);
    }

    @Override
    protected CompiledSelection compileSelection(List<SelectAttributeBuilder> selectAttributeBuilders,
                                                 List<ExpressionBuilder> groupByExpressionBuilders,
                                                 ExpressionBuilder havingExpressionBuilder,
                                                 List<OrderByAttributeBuilder> orderByAttributeBuilders,
                                                 Long limit, Long offset) {
        String project,groupBy,having,orderBy;
        groupBy = having = orderBy = null;
        if(groupByExpressionBuilders==null){
            project = projectionString(selectAttributeBuilders);
        }else{
            groupBy = groupByString(selectAttributeBuilders, groupByExpressionBuilders);
            String[] arrOfStr = groupBy.split(";", 2);
            project = arrOfStr[1];
            groupBy = arrOfStr[0];
        }
        if(havingExpressionBuilder != null){
            having = havingString(havingExpressionBuilder);
        }
        if(orderByAttributeBuilders != null){
            orderBy = orderByString(orderByAttributeBuilders);
        }
        return new MongoDBCompileSelection(project, groupBy, having , orderBy , limit, offset);
    }

    private String projectionString(List<SelectAttributeBuilder> selectAttributeBuilders){
        List<MongoSelectExpressionVisitor> collect = getSelectAttributesList(selectAttributeBuilders);
        StringBuilder compiledSelectionJSON = new StringBuilder();
        compiledSelectionJSON.append("{$project:{_id:0, ");
        for(int i=0;i<collect.size();i++){
            MongoSelectExpressionVisitor value = collect.get(i);
            String rename = selectAttributeBuilders.get(i).getRename();
            if(value.getStreamVarCount() == 0 && value.getConstantCount()==0) {
                compiledSelectionJSON.append(rename).append(value.getCompiledCondition())
                        .append((collect.indexOf(value) == (collect.size() -1)) ? '}' : ',');
            }else if(value.getStreamVarCount() == 1){
                compiledSelectionJSON.append(rename).append(value.getCompiledCondition())
                        .append((collect.indexOf(value) == (collect.size() -1)) ? '}' : ',');
            }
            else if(value.getConstantCount()==1){
                compiledSelectionJSON.append(rename).append(value.getCompiledCondition())
                        .append((collect.indexOf(value) == (collect.size() -1)) ? '}' : ',');
            }
        }
        compiledSelectionJSON.append('}');
        return compiledSelectionJSON.toString();
    }

    private String groupByString(List<SelectAttributeBuilder> selectAttributeBuilders,
                                 List<ExpressionBuilder> groupByExpressionBuilders){

        List<String> groupByAttributesList = new ArrayList<>();
        StringBuilder compiledGroupByJSON = new StringBuilder();
        StringBuilder selectGroupByString = new StringBuilder();
        StringBuilder compiledProjectionJSON = new StringBuilder();

        List<MongoExpressionVisitor> collectGroupBy =
                groupByExpressionBuilders.stream().map((groupByExpressionBuilder -> {
                    MongoExpressionVisitor visitor = new MongoExpressionVisitor();
                    groupByExpressionBuilder.build(visitor);
                    return visitor;
                })).collect(Collectors.toList());
        compiledGroupByJSON.append((collectGroupBy.size()==1) ? "{$group:{_id:" : "{$group:{_id:{");
        for(MongoExpressionVisitor value : collectGroupBy){
            String groupByAttribute = value.getConditionOperands().get(0);
            groupByAttributesList.add(groupByAttribute);
            if(value.getStreamVarCount() == 0 && value.getConstantCount()==0) {
                if(collectGroupBy.size()==1){
                    compiledGroupByJSON.append("\'$").append(groupByAttribute).append('\'');
                    selectGroupByString.append(groupByAttribute).append(":{$first:\'$").
                            append(groupByAttribute).append("\'}");
                }else{
                    compiledGroupByJSON.append(groupByAttribute+":").append("\'$").append(groupByAttribute);
                    if(collectGroupBy.indexOf(value) == (collectGroupBy.size() -1)){
                        selectGroupByString.append(groupByAttribute).append(":{$first:\'$").append(groupByAttribute).
                                append("\'}");
                        compiledGroupByJSON.append("\'");
                    }else{
                        selectGroupByString.append(groupByAttribute).append(":{$first:\'$").append(groupByAttribute).
                                append("\'},");
                        compiledGroupByJSON.append("\',");
                    }
                }
            }
        }
        compiledGroupByJSON.append((groupByAttributesList.size()==1) ? ',' : "},");

        List<MongoSelectExpressionVisitor> collect = getSelectAttributesList(selectAttributeBuilders);
        compiledProjectionJSON.append("{$project:{_id:0,");
        for(int i=0;i<collect.size();i++){
            MongoSelectExpressionVisitor value = collect.get(i);
            String rename = selectAttributeBuilders.get(i).getRename();
            compiledProjectionJSON.append(rename).append(":1");
            if(!groupByAttributesList.contains(rename)){
                if(value.getStreamVarCount() == 0 && value.getConstantCount()==0) {
                    String[] supportedFunctions = value.getSupportedFunctions();
                    if(Arrays.stream(supportedFunctions).parallel().anyMatch(value.getCompiledCondition()::contains)){
                        compiledGroupByJSON.append(rename).append(value.getCompiledCondition()).append('}');
                    }else{
                        compiledGroupByJSON.append(rename).append(":{$push").append(value.getCompiledCondition())
                                .append('}');
                    }
                    if(collect.indexOf(value) == (collect.size() -1)){
                        compiledGroupByJSON.append(",").append(selectGroupByString).append("}");
                        compiledProjectionJSON.append("}");
                    }else{
                        compiledGroupByJSON.append(',');
                        compiledProjectionJSON.append(",");
                    }
                }
            }else{
                compiledProjectionJSON.append(",");
            }
        }
        compiledGroupByJSON.append("}");
        compiledProjectionJSON.append("}");
        return compiledGroupByJSON.toString()+";"+compiledProjectionJSON;
    }

    private String havingString(ExpressionBuilder havingExpressionBuilder){
            MongoExpressionVisitor visitor = new MongoExpressionVisitor();
            havingExpressionBuilder.build(visitor);
            String having  = visitor.getCompiledCondition();
            return "{$match:"+having+"}";
    }

    private String orderByString(List<OrderByAttributeBuilder> orderByAttributeBuilders){
        StringBuilder compiledOrderByJSON = new StringBuilder();
        List<MongoExpressionVisitor> collectOrderBy =
                orderByAttributeBuilders.stream().map((orderByAttributeBuilder -> {
                    ExpressionBuilder expressionBuilder = orderByAttributeBuilder.getExpressionBuilder();
                    MongoExpressionVisitor orderByVisitor = new MongoExpressionVisitor();
                    expressionBuilder.build(orderByVisitor);
                    return orderByVisitor;
                })).collect(Collectors.toList());
        compiledOrderByJSON.append("{$sort:{");
        for(int i=0;i<collectOrderBy.size();i++) {
            MongoExpressionVisitor value = collectOrderBy.get(i);
            String order = orderByAttributeBuilders.get(i).getOrder().name();
            if(value.getStreamVarCount() == 0) {
                compiledOrderByJSON.append(value.getCompiledCondition()).append((order == "ASC") ? ":1" : ":-1")
                        .append((collectOrderBy.indexOf(value) == (collectOrderBy.size() -1)) ? '}' : ',');
            }
        }
        compiledOrderByJSON.append('}');
        return compiledOrderByJSON.toString();
    }

    private List<MongoSelectExpressionVisitor> getSelectAttributesList(List<SelectAttributeBuilder> selectAttributeBuilders){
        List<MongoSelectExpressionVisitor> collect =
                selectAttributeBuilders.stream().map((selectAttributeBuilder -> {
                    ExpressionBuilder expressionBuilder = selectAttributeBuilder.getExpressionBuilder();
                    MongoSelectExpressionVisitor visitor = new MongoSelectExpressionVisitor();
                    expressionBuilder.build(visitor);
                    return visitor;
                })).collect(Collectors.toList());
        return collect;
    }
}
