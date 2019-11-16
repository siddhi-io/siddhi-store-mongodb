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
package io.siddhi.extension.store.mongodb.util;

import com.mongodb.DBObject;
import com.mongodb.MongoClientOptions;
import com.mongodb.ReadConcern;
import com.mongodb.ReadConcernLevel;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Collation;
import com.mongodb.client.model.CollationAlternate;
import com.mongodb.client.model.CollationCaseFirst;
import com.mongodb.client.model.CollationMaxVariable;
import com.mongodb.client.model.CollationStrength;
import com.mongodb.client.model.IndexModel;
import com.mongodb.client.model.IndexOptions;
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.extension.store.mongodb.MongoCompiledCondition;
import io.siddhi.extension.store.mongodb.exception.MongoTableException;
import io.siddhi.query.api.annotation.Annotation;
import io.siddhi.query.api.definition.Attribute;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.json.JsonParseException;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.net.SocketFactory;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import static io.siddhi.extension.store.mongodb.util.MongoTableConstants.DEFAULT_KEY_STORE_FILE;
import static io.siddhi.extension.store.mongodb.util.MongoTableConstants.DEFAULT_KEY_STORE_PASSWORD;
import static io.siddhi.extension.store.mongodb.util.MongoTableConstants.DEFAULT_TRUST_STORE_FILE;
import static io.siddhi.extension.store.mongodb.util.MongoTableConstants.DEFAULT_TRUST_STORE_PASSWORD;
import static io.siddhi.extension.store.mongodb.util.MongoTableConstants.VARIABLE_CARBON_HOME;

/**
 * Class which holds the utility methods which are used by various units in the MongoDB Event Table implementation.
 */
public class MongoTableUtils {
    private static final Log log = LogFactory.getLog(MongoTableUtils.class);

    private MongoTableUtils() {
        //Prevent Initialization.
    }

    /**
     * Utility method which can be used to check if the given primary key is valid i.e. non empty
     * and is made up of attributes and return an index model when PrimaryKey is valid.
     *
     * @param primaryKey     the PrimaryKey annotation which contains the primary key attributes.
     * @param attributeNames List containing names of the attributes.
     * @return List of String with primary key attributes.
     */
    public static IndexModel extractPrimaryKey(Annotation primaryKey, List<String> attributeNames) {
        if (primaryKey == null) {
            return null;
        }
        Document primaryKeyIndex = new Document();
        primaryKey.getElements().forEach(
                element -> {
                    if (!isEmpty(element.getValue()) && attributeNames.contains(element.getValue())) {
                        primaryKeyIndex.append(element.getValue(), 1);
                    } else {
                        throw new SiddhiAppCreationException("Annotation '" + primaryKey.getName() + "' contains " +
                                "value '" + element.getValue() + "' which is not present in the attributes of the " +
                                "Event Table.");
                    }
                }
        );
        return new IndexModel(primaryKeyIndex, new IndexOptions().unique(true));
    }

    public static List<IndexModel> extractIndexModels(Annotation indices, List<String> attributeNames,
                                                      String tableName) {
        if (indices == null) {
            return new ArrayList<>();
        }
        log.warn("MongoDB definition uses depreciated @IndexBy. Please use @Index annotation");
        Pattern indexBy = Pattern.compile(MongoTableConstants.REG_INDEX_BY);
        return indices.getElements().stream().map(index -> {
            Matcher matcher = indexBy.matcher(index.getValue());
            if (matcher.matches() && attributeNames.contains(matcher.group(1))) {
                Map<String, Integer> indexFields = new HashMap<>();
                if (matcher.groupCount() == 4) {
                    indexFields.put(matcher.group(1), Integer.parseInt(matcher.group(2)));
                    return createIndexModel(indexFields, matcher.group(3).trim(), tableName);
                } else {
                    if (matcher.groupCount() == 3) {
                        if (matcher.group(3) == null) {
                            indexFields.put(matcher.group(1), Integer.parseInt(matcher.group(2)));
                            return createIndexModel(indexFields, null, tableName);
                        } else {
                            indexFields.put(matcher.group(1), 1);
                            return createIndexModel(indexFields, matcher.group(3).trim(), tableName);
                        }
                    } else {
                        indexFields.put(matcher.group(1), 1);
                        return createIndexModel(indexFields, null, tableName);
                    }
                }
            } else {
                throw new SiddhiAppCreationException("Annotation '@IndexBy' in table '" + tableName +
                        "' contains illegal value : '" + index.getValue() + "'. " +
                        "Please check your query and try again.");
            }
        }).collect(Collectors.toList());
    }


    /**
     * Utility method which can be used to check if the given Indices are valid  and return List of
     * MongoDB Index Models when valid.
     *
     * @param indices        the IndexBy annotation which contains the indices definitions.
     * @param attributeNames List containing names of the attributes.
     * @return List of IndexModel.
     */
    public static List<IndexModel> extractIndexModels(List<Annotation> indices, List<String> attributeNames,
                                                      String tableName) {
        if (indices == null || indices.isEmpty()) {
            return new ArrayList<>();
        }

        Pattern indexByOptions = Pattern.compile(MongoTableConstants.REG_INDEX_BY_NEW_OPTIONS);

        return indices.stream().map((indexAnnotation) -> {
            String indexOptions = null;
            int elementsSize = indexAnnotation.getElements().size();
            Matcher optionsMatcher =
                    indexByOptions.matcher(indexAnnotation.getElements().get(elementsSize - 1).getValue());
            if (optionsMatcher.matches()) {
                indexOptions = indexAnnotation.getElements().get(elementsSize - 1).getValue();
                indexAnnotation.getElements().remove(elementsSize - 1);
            }
            Map<String, Integer> indexFields = new HashMap<>();
            indexAnnotation.getElements().forEach((indexElement) -> {
                String[] splitFields = indexElement.getValue().split(":");
                if (splitFields.length != 1 && splitFields.length != 2) {
                    throw new SiddhiAppCreationException("Annotation 'Index' for table '" + tableName +
                            "' contains illegal value : '" + indexElement.getValue() +
                            "'. Please check your query and try again.");
                } else {
                    if (!attributeNames.contains(splitFields[0])) {
                        throw new SiddhiAppCreationException("Annotation 'Index' for table '" + tableName +
                                "' contains unknown attributes : '" + indexElement.getValue() + "'. " +
                                "Please check your query and try again.");
                    }
                    if (splitFields.length == 2) {
                        int sortOrder;
                        try {
                            sortOrder = Integer.parseInt(splitFields[1]);
                            if (sortOrder != 1 && sortOrder != -1) {
                                throw new SiddhiAppCreationException("Annotation 'Index' for table '" + tableName +
                                        "' contains illegal value for <Sort order>: found, '" +
                                        indexElement.getValue() + "' expected 1/-1. Please check your query and " +
                                        "try again.");
                            }
                        } catch (NumberFormatException e) {
                            throw new SiddhiAppCreationException("Annotation 'Index' for table '" + tableName +
                                    "' contains illegal value for <Sort order>: found, '" + indexElement.getValue() +
                                    "' expected 1/-1. Please check your query and try again.");
                        }
                        indexFields.put(splitFields[0], sortOrder);
                    } else {
                        indexFields.put(splitFields[0], 1);
                    }

                }
            });
            return createIndexModel(indexFields, indexOptions, tableName);

        }).collect(Collectors.toList());
    }

    /**
     * Utility method which can be used to create an IndexModel.
     *
     * @param indexFields Hash map containing fields to be indexed mapped to the sort order
     * @param indexOption json string of the options of the index to be created.
     * @return IndexModel.
     */
    private static IndexModel createIndexModel(Map<String, Integer> indexFields, String indexOption,
                                               String tableName) {
        Document indexDocument = new Document();
        indexFields.forEach(indexDocument::put);
        if (indexOption == null) {
            return new IndexModel(indexDocument);
        } else {
            IndexOptions indexOptions = new IndexOptions();
            Document indexOptionDocument;
            try {
                indexOptionDocument = Document.parse(indexOption);
                for (Map.Entry<String, Object> indexEntry : indexOptionDocument.entrySet()) {
                    Object value = indexEntry.getValue();
                    switch (indexEntry.getKey()) {
                        case "unique":
                            indexOptions.unique(Boolean.parseBoolean(value.toString()));
                            break;
                        case "background":
                            indexOptions.background(Boolean.parseBoolean(value.toString()));
                            break;
                        case "name":
                            indexOptions.name(value.toString());
                            break;
                        case "sparse":
                            indexOptions.sparse(Boolean.parseBoolean(value.toString()));
                            break;
                        case "expireAfterSeconds":
                            indexOptions.expireAfter(Long.parseLong(value.toString()), TimeUnit.SECONDS);
                            break;
                        case "version":
                            indexOptions.version(Integer.parseInt(value.toString()));
                            break;
                        case "weights":
                            indexOptions.weights((Bson) value);
                            break;
                        case "languageOverride":
                            indexOptions.languageOverride(value.toString());
                            break;
                        case "defaultLanguage":
                            indexOptions.defaultLanguage(value.toString());
                            break;
                        case "textVersion":
                            indexOptions.textVersion(Integer.parseInt(value.toString()));
                            break;
                        case "sphereVersion":
                            indexOptions.sphereVersion(Integer.parseInt(value.toString()));
                            break;
                        case "bits":
                            indexOptions.bits(Integer.parseInt(value.toString()));
                            break;
                        case "min":
                            indexOptions.min(Double.parseDouble(value.toString()));
                            break;
                        case "max":
                            indexOptions.max(Double.parseDouble(value.toString()));
                            break;
                        case "bucketSize":
                            indexOptions.bucketSize(Double.parseDouble(value.toString()));
                            break;
                        case "partialFilterExpression":
                            indexOptions.partialFilterExpression((Bson) value);
                            break;
                        case "collation":
                            DBObject collationOptions = (DBObject) value;
                            Collation.Builder builder = Collation.builder();
                            for (String collationKey : collationOptions.keySet()) {
                                String collationObj = value.toString();
                                switch (collationKey) {
                                    case "locale":
                                        builder.locale(collationObj);
                                        break;
                                    case "caseLevel":
                                        builder.caseLevel(Boolean.parseBoolean(collationObj));
                                        break;
                                    case "caseFirst":
                                        builder.collationCaseFirst(CollationCaseFirst.fromString(collationObj));
                                        break;
                                    case "strength":
                                        builder.collationStrength(CollationStrength.valueOf(collationObj));
                                        break;
                                    case "numericOrdering":
                                        builder.numericOrdering(Boolean.parseBoolean(collationObj));
                                        break;
                                    case "normalization":
                                        builder.normalization(Boolean.parseBoolean(collationObj));
                                        break;
                                    case "backwards":
                                        builder.backwards(Boolean.parseBoolean(collationObj));
                                        break;
                                    case "alternate":
                                        builder.collationAlternate(CollationAlternate.fromString(collationObj));
                                        break;
                                    case "maxVariable":
                                        builder.collationMaxVariable(CollationMaxVariable.fromString(collationObj));
                                        break;
                                    default:
                                        log.warn("Annotation 'IndexBy' for the table '" + tableName + "' contains " +
                                                "unknown 'Collation' Option key : '" + collationKey + "'. Please " +
                                                "check your query and try again.");
                                        break;
                                }
                            }
                            if (builder.build().getLocale() != null) {
                                indexOptions.collation(builder.build());
                            } else {
                                throw new MongoTableException("Annotation 'IndexBy' for the table '" + tableName + "'" +
                                        " do not contain option for locale. Please check your query and try again.");
                            }
                            break;
                        case "storageEngine":
                            indexOptions.storageEngine((Bson) value);
                            break;
                        default:
                            log.warn("Annotation 'IndexBy' for the table '" + tableName + "' contains unknown option " +
                                    "key : '" + indexEntry.getKey() + "'. Please check your query and try again.");
                            break;
                    }
                }
            } catch (JsonParseException | NumberFormatException e) {
                throw new MongoTableException("Annotation 'IndexBy' for the table '" + tableName + "' contains " +
                        "illegal value(s) for index option. Please check your query and try again.", e);
            }
            return new IndexModel(indexDocument, indexOptions);
        }
    }

    /**
     * Utility method which can be used to resolve the condition with the runtime values and return a Document
     * describing the filter.
     *
     * @param compiledCondition     the compiled condition which was built during compile time and now is being provided
     *                              by the Siddhi runtime.
     * @param conditionParameterMap the map which contains the runtime value(s) for the condition.
     * @return Document.
     */
    public static Document resolveCondition(MongoCompiledCondition compiledCondition,
                                            Map<String, Object> conditionParameterMap) {
        Map<String, Object> parameters = compiledCondition.getPlaceholders();
        String compiledQuery = compiledCondition.getCompiledQuery();
        if (compiledQuery.equalsIgnoreCase("true")) {
            return new Document();
        }
        for (Map.Entry<String, Object> entry : parameters.entrySet()) {
            Object parameter = entry.getValue();
            Attribute variable = (Attribute) parameter;
            if (variable.getType().equals(Attribute.Type.STRING)) {
                compiledQuery = compiledQuery.replaceAll(entry.getKey(), "\"" +
                        conditionParameterMap.get(variable.getName()).toString() + "\"");
            } else {
                compiledQuery = compiledQuery.replaceAll(entry.getKey(),
                        conditionParameterMap.get(variable.getName()).toString());
            }
        }
        if (log.isDebugEnabled()) {
            log.debug("The final compiled query : '" + compiledQuery + "'");
        }
        return Document.parse(compiledQuery);
    }

    /**
     * Utility method which can be used to check if a given string instance is null or empty.
     *
     * @param field the string instance to be checked.
     * @return true if the field is null or empty.
     */
    public static boolean isEmpty(String field) {
        return (field == null || field.trim().length() == 0);
    }


    /**
     * Utility method tp map the values to the respective attributes before database writes.
     *
     * @param record         Object array of the runtime values.
     * @param attributeNames List containing names of the attributes.
     * @return Document
     */
    public static Map<String, Object> mapValuesToAttributes(Object[] record, List<String> attributeNames) {
        Map<String, Object> attributesValuesMap = new HashMap<>();
        for (int i = 0; i < record.length; i++) {
            attributesValuesMap.put(attributeNames.get(i), record[i]);
        }
        return attributesValuesMap;
    }

    /**
     * Utility method which can be used to check if the existing indices contain the expected indices
     * defined by the annotation 'PrimaryKey' and 'IndexBy' and log a warning when indices differs.
     *
     * @param existingIndices List of indices that the collection contains.
     * @param expectedIndices List of indices that are defined by the annotations.
     */
    public static void checkExistingIndices(List<IndexModel> expectedIndices, MongoCursor<Document> existingIndices) {
        Map<String, Object> indexOptionsMap = new HashMap<>();
        List<Document> expectedIndexDocuments = expectedIndices.stream().map(expectedIndex -> {
            IndexOptions expectedIndexOptions = expectedIndex.getOptions();
            indexOptionsMap.put("key", expectedIndex.getKeys());
            // Default value for name of the index
            if (expectedIndexOptions.getName() == null) {
                StringBuilder indexName = new StringBuilder();
                ((Document) expectedIndex.getKeys()).forEach((key, value) ->
                        indexName.append("_").append(key).append("_").append(value));
                indexName.deleteCharAt(0);
                indexOptionsMap.put("name", indexName.toString());
            } else {
                indexOptionsMap.put("name", expectedIndexOptions.getName());
            }
            // Default value for the version
            if (expectedIndexOptions.getVersion() == null) {
                indexOptionsMap.put("v", 2);
            } else {
                indexOptionsMap.put("v", expectedIndexOptions.getVersion());
            }
            indexOptionsMap.put("unique", expectedIndexOptions.isUnique());
            indexOptionsMap.put("background", expectedIndexOptions.isBackground());
            indexOptionsMap.put("sparse", expectedIndexOptions.isSparse());
            indexOptionsMap.put("expireAfterSeconds", expectedIndexOptions.getExpireAfter(TimeUnit.SECONDS));
            indexOptionsMap.put("weights", expectedIndexOptions.getWeights());
            indexOptionsMap.put("languageOverride", expectedIndexOptions.getLanguageOverride());
            indexOptionsMap.put("defaultLanguage", expectedIndexOptions.getDefaultLanguage());
            indexOptionsMap.put("textVersion", expectedIndexOptions.getTextVersion());
            indexOptionsMap.put("sphereVersion", expectedIndexOptions.getSphereVersion());
            indexOptionsMap.put("bits", expectedIndexOptions.getBits());
            indexOptionsMap.put("min", expectedIndexOptions.getMin());
            indexOptionsMap.put("max", expectedIndexOptions.getMax());
            indexOptionsMap.put("bucketSize", expectedIndexOptions.getBucketSize());
            indexOptionsMap.put("partialFilterExpression", expectedIndexOptions.getPartialFilterExpression());
            indexOptionsMap.put("collation", expectedIndexOptions.getCollation());
            indexOptionsMap.put("storageEngine", expectedIndexOptions.getStorageEngine());

            //Remove if Default Values - these would not be in the existingIndexDocument.
            indexOptionsMap.values().removeIf(Objects::isNull);
            indexOptionsMap.remove("unique", false);
            indexOptionsMap.remove("background", false);
            indexOptionsMap.remove("sparse", false);

            return new Document(indexOptionsMap);
        }).collect(Collectors.toList());

        List<Document> existingIndexDocuments = new ArrayList<>();
        existingIndices.forEachRemaining(existingIndex -> {
            existingIndex.remove("ns");
            existingIndexDocuments.add(existingIndex);
        });

        if (!existingIndexDocuments.containsAll(expectedIndexDocuments)) {
            log.warn("Existing indices differs from the expected indices defined by the Annotations 'PrimaryKey' " +
                    "and 'IndexBy'.\nExisting Indices '" + existingIndexDocuments.toString() + "'.\n" +
                    "Expected Indices '" + expectedIndexDocuments.toString() + "'");
        }
    }

    /**
     * Utility method which can be used to create MongoClientOptionsBuilder from values defined in the
     * deployment yaml file.
     *
     * @param storeAnnotation the source annotation which contains the needed parameters.
     * @param configReader    {@link ConfigReader} Configuration Reader
     * @return MongoClientOptions.Builder
     */
    public static MongoClientOptions.Builder extractMongoClientOptionsBuilder
    (Annotation storeAnnotation, ConfigReader configReader) {

        MongoClientOptions.Builder mongoClientOptionsBuilder = MongoClientOptions.builder();
        try {
            mongoClientOptionsBuilder.connectionsPerHost(
                    Integer.parseInt(configReader.readConfig(MongoTableConstants.CONNECTIONS_PER_HOST, "100")));
            mongoClientOptionsBuilder.connectTimeout(
                    Integer.parseInt(configReader.readConfig(MongoTableConstants.CONNECT_TIMEOUT, "10000")));
            mongoClientOptionsBuilder.heartbeatConnectTimeout(
                    Integer.parseInt(configReader.readConfig(MongoTableConstants.HEARTBEAT_CONNECT_TIMEOUT, "20000")));
            mongoClientOptionsBuilder.heartbeatSocketTimeout(
                    Integer.parseInt(configReader.readConfig(MongoTableConstants.HEARTBEAT_SOCKET_TIMEOUT, "20000")));
            mongoClientOptionsBuilder.heartbeatFrequency(
                    Integer.parseInt(configReader.readConfig(MongoTableConstants.HEARTBEAT_FREQUENCY, "10000")));
            mongoClientOptionsBuilder.localThreshold(
                    Integer.parseInt(configReader.readConfig(MongoTableConstants.LOCAL_THRESHOLD, "15")));
            mongoClientOptionsBuilder.maxWaitTime(
                    Integer.parseInt(configReader.readConfig(MongoTableConstants.MAX_WAIT_TIME, "120000")));
            mongoClientOptionsBuilder.minConnectionsPerHost(
                    Integer.parseInt(configReader.readConfig(MongoTableConstants.MIN_CONNECTIONS_PER_HOST, "0")));
            mongoClientOptionsBuilder.minHeartbeatFrequency(
                    Integer.parseInt(configReader.readConfig(MongoTableConstants.MIN_HEARTBEAT_FREQUENCY, "500")));
            mongoClientOptionsBuilder.serverSelectionTimeout(Integer.parseInt(
                    configReader.readConfig(MongoTableConstants.SERVER_SELECTION_TIMEOUT, "30000")));
            mongoClientOptionsBuilder.socketTimeout(
                    Integer.parseInt(configReader.readConfig(MongoTableConstants.SOCKET_TIMEOUT, "0")));
            mongoClientOptionsBuilder.threadsAllowedToBlockForConnectionMultiplier(Integer.parseInt(
                    configReader.readConfig(MongoTableConstants.THREADS_ALLOWED_TO_BLOCK, "5")));
            mongoClientOptionsBuilder.socketKeepAlive(
                    Boolean.parseBoolean(configReader.readConfig(MongoTableConstants.SOCKET_KEEP_ALIVE, "false")));
            mongoClientOptionsBuilder.sslEnabled(
                    Boolean.parseBoolean(configReader.readConfig(MongoTableConstants.SSL_ENABLED, "false")));
            mongoClientOptionsBuilder.cursorFinalizerEnabled(Boolean.parseBoolean(
                    configReader.readConfig(MongoTableConstants.CURSOR_FINALIZER_ENABLED, "true")));
            mongoClientOptionsBuilder.readPreference(
                    ReadPreference.valueOf(configReader.readConfig(MongoTableConstants.READ_PREFERENCE, "primary")));
            mongoClientOptionsBuilder.writeConcern(
                    WriteConcern.valueOf(configReader.readConfig(MongoTableConstants.WRITE_CONCERN, "acknowledged")));

            String readConcern = configReader.readConfig(MongoTableConstants.READ_CONCERN, "DEFAULT");
            if (!readConcern.matches("DEFAULT")) {
                mongoClientOptionsBuilder.readConcern(new ReadConcern(
                        ReadConcernLevel.fromString(readConcern)));
            }

            int maxConnectionIdleTime = Integer.parseInt(
                    configReader.readConfig(MongoTableConstants.MAX_CONNECTION_IDLE_TIME, "0"));
            if (maxConnectionIdleTime != 0) {
                mongoClientOptionsBuilder.maxConnectionIdleTime(maxConnectionIdleTime);
            }

            int maxConnectionLifeTime = Integer.parseInt(
                    configReader.readConfig(MongoTableConstants.MAX_CONNECTION_LIFE_TIME, "0"));
            if (maxConnectionIdleTime != 0) {
                mongoClientOptionsBuilder.maxConnectionLifeTime(maxConnectionLifeTime);
            }

            String requiredReplicaSetName = configReader.readConfig(MongoTableConstants.REQUIRED_REPLICA_SET_NAME, "");
            if (!requiredReplicaSetName.equals("")) {
                mongoClientOptionsBuilder.requiredReplicaSetName(requiredReplicaSetName);
            }

            String applicationName = configReader.readConfig(MongoTableConstants.APPLICATION_NAME, "");
            if (!applicationName.equals("")) {
                mongoClientOptionsBuilder.applicationName(applicationName);
            }

            String secureConnectionEnabled = storeAnnotation.getElement(
                    MongoTableConstants.ANNOTATION_ELEMENT_SECURE_CONNECTION);
            secureConnectionEnabled = secureConnectionEnabled == null ? "false" : secureConnectionEnabled;

            if (secureConnectionEnabled.equalsIgnoreCase("true")) {
                mongoClientOptionsBuilder.sslEnabled(true);
                String trustStore = storeAnnotation.getElement(MongoTableConstants.ANNOTATION_ELEMENT_TRUSTSTORE);
                trustStore = trustStore == null ?
                        configReader.readConfig("trustStore", DEFAULT_TRUST_STORE_FILE) : trustStore;
                trustStore = resolveCarbonHome(trustStore);

                String trustStorePassword =
                        storeAnnotation.getElement(MongoTableConstants.ANNOTATION_ELEMENT_TRUSTSTOREPASS);
                trustStorePassword = trustStorePassword == null ?
                        configReader.readConfig("trustStorePassword", DEFAULT_TRUST_STORE_PASSWORD) :
                        trustStorePassword;

                String keyStore = storeAnnotation.getElement(MongoTableConstants.ANNOTATION_ELEMENT_KEYSTORE);
                keyStore = keyStore == null ?
                        configReader.readConfig("keyStore", DEFAULT_KEY_STORE_FILE) : keyStore;
                keyStore = resolveCarbonHome(keyStore);

                String keyStorePassword = storeAnnotation.getElement(MongoTableConstants.ANNOTATION_ELEMENT_STOREPASS);
                keyStorePassword = keyStorePassword == null ?
                        configReader.readConfig("keyStorePassword", DEFAULT_KEY_STORE_PASSWORD) :
                        keyStorePassword;

                mongoClientOptionsBuilder.socketFactory(MongoTableUtils
                        .extractSocketFactory(trustStore, trustStorePassword, keyStore, keyStorePassword));
            }
            return mongoClientOptionsBuilder;
        } catch (IllegalArgumentException e) {
            throw new MongoTableException("Values Read from config readers have illegal values : ", e);
        }
    }

    private static SocketFactory extractSocketFactory(
            String trustStore, String trustStorePassword, String keyStore, String keyStorePassword) {
        TrustManager[] trustManagers;
        KeyManager[] keyManagers;

        try (InputStream trustStream = new FileInputStream(trustStore)) {
            char[] trustStorePass = trustStorePassword.toCharArray();
            KeyStore trustStoreJKS = KeyStore.getInstance(KeyStore.getDefaultType());
            trustStoreJKS.load(trustStream, trustStorePass);
            TrustManagerFactory trustFactory =
                    TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trustFactory.init(trustStoreJKS);
            trustManagers = trustFactory.getTrustManagers();
        } catch (FileNotFoundException e) {
            throw new MongoTableException("Trust store file not found for secure connections to mongodb. " +
                    "Trust Store file path : '" + trustStore + "'.", e);
        } catch (IOException e) {
            throw new MongoTableException("I/O Exception in creating trust store for secure connections to mongodb. " +
                    "Trust Store file path : '" + trustStore + "'.", e);
        } catch (CertificateException e) {
            throw new MongoTableException("Certificates in the trust store could not be loaded for secure " +
                    "connections to mongodb. Trust Store file path : '" + trustStore + "'.", e);
        } catch (NoSuchAlgorithmException e) {
            throw new MongoTableException("The algorithm used to check the integrity of the trust store cannot be " +
                    "found. Trust Store file path : '" + trustStore + "'.", e);
        } catch (KeyStoreException e) {
            throw new MongoTableException("Exception in creating trust store, no Provider supports aKeyStoreSpi " +
                    "implementation for the specified type. Trust Store file path : '" + trustStore + "'.", e);
        }

        try (InputStream keyStream = new FileInputStream(keyStore)) {
            char[] keyStorePass = keyStorePassword.toCharArray();
            KeyStore keyStoreJKS = KeyStore.getInstance(KeyStore.getDefaultType());
            keyStoreJKS.load(keyStream, keyStorePass);
            KeyManagerFactory keyManagerFactory =
                    KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            keyManagerFactory.init(keyStoreJKS, keyStorePass);
            keyManagers = keyManagerFactory.getKeyManagers();
        } catch (FileNotFoundException e) {
            throw new MongoTableException("Key store file not found for secure connections to mongodb. " +
                    "Key Store file path : '" + keyStore + "'.", e);
        } catch (IOException e) {
            throw new MongoTableException("I/O Exception in creating trust store for secure connections to mongodb. " +
                    "Key Store file path : '" + keyStore + "'.", e);
        } catch (CertificateException e) {
            throw new MongoTableException("Certificates in the trust store could not be loaded for secure " +
                    "connections to mongodb. Key Store file path : '" + keyStore + "'.", e);
        } catch (NoSuchAlgorithmException e) {
            throw new MongoTableException("The algorithm used to check the integrity of the trust store cannot be " +
                    "found. Key Store file path : '" + keyStore + "'.", e);
        } catch (KeyStoreException e) {
            throw new MongoTableException("Exception in creating trust store, no Provider supports aKeyStoreSpi " +
                    "implementation for the specified type. Key Store file path : '" + keyStore + "'.", e);
        } catch (UnrecoverableKeyException e) {
            throw new MongoTableException("Key in the keystore cannot be recovered. " +
                    "Key Store file path : '" + keyStore + "'.", e);
        }

        try {
            SSLContext sslContext = SSLContext.getInstance("SSL");
            sslContext.init(keyManagers, trustManagers, null);
            SSLContext.setDefault(sslContext);
            return sslContext.getSocketFactory();
        } catch (KeyManagementException e) {
            throw new MongoTableException("Error in validating the key in the key store/ trust store. " +
                    "Trust Store file path : '" + trustStore + "'. " +
                    "Key Store file path : '" + keyStore + "'.", e);
        } catch (NoSuchAlgorithmException e) {
            throw new MongoTableException(" SSL Algorithm used to create SSL Socket Factory for mongodb connections " +
                    "is not found.", e);
        }

    }

    private static String resolveCarbonHome(String filePath) {
        String carbonHome = "";
        if (System.getProperty(VARIABLE_CARBON_HOME) != null) {
            carbonHome = System.getProperty(VARIABLE_CARBON_HOME);
        } else if (System.getenv(VARIABLE_CARBON_HOME) != null) {
            carbonHome = System.getenv(VARIABLE_CARBON_HOME);
        }
        return filePath.replaceAll("\\$\\{carbon.home}", carbonHome);
    }

    public static void printDebugLogs(String logDescription, String queryLog){
        if (log.isDebugEnabled()) {
            log.debug(logDescription + queryLog);
        }
    }
}

