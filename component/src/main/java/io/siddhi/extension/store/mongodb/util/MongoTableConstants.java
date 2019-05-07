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

/**
 * Class which holds the constants required by the MongoDB Event Table implementation.
 */
public class MongoTableConstants {

    //Annotation field names
    public static final String ANNOTATION_ELEMENT_URI = "mongodb.uri";
    public static final String ANNOTATION_ELEMENT_COLLECTION_NAME = "collection.name";
    public static final String ANNOTATION_ELEMENT_KEYSTORE = "key.store";
    public static final String ANNOTATION_ELEMENT_STOREPASS = "key.store.password";
    public static final String ANNOTATION_ELEMENT_TRUSTSTORE = "trust.store";
    public static final String ANNOTATION_ELEMENT_TRUSTSTOREPASS = "trust.store.password";
    public static final String ANNOTATION_ELEMENT_SECURE_CONNECTION = "secure.connection";

    //Mongo Operators
    public static final String MONGO_COMPARE_LESS_THAN = "$lt";
    public static final String MONGO_COMPARE_GREATER_THAN = "$gt";
    public static final String MONGO_COMPARE_LESS_THAN_EQUAL = "$lte";
    public static final String MONGO_COMPARE_GREATER_THAN_EQUAL = "$gte";
    public static final String MONGO_COMPARE_EQUAL = "$eq";
    public static final String MONGO_COMPARE_NOT_EQUAL = "$ne";
    public static final String MONGO_NOT = "$not";

    //Regex for comparing operands
    public static final String REG_EXPRESSION = "\\{.*}$";
    public static final String REG_SIMPLE_EXPRESSION = "^\\{(\\S*):\\{.*}}$";
    public static final String REG_STREAMVAR_OR_CONST = "^strVar\\d*|^const\\d*";
    public static final String REG_INDEX_BY = "^(\\S*)(\\s1|\\s-1)?(\\s\\{.*})?$";

    //Mongo filters for condition builder
    public static final String MONGO_AND_FILTER = "{$and:[{{LEFT_OPERAND}},{{RIGHT_OPERAND}}]}";
    public static final String MONGO_OR_FILTER = "{$or:[{{LEFT_OPERAND}},{{RIGHT_OPERAND}}]}";
    public static final String MONGO_NOT_FILTER = "{{{FIELD_NAME}}:{{OPERAND}}}";
    public static final String MONGO_COMPARE_FILTER = "{{{LEFT_OPERAND}}:{{{COMPARE_OPERATOR}}:{{RIGHT_OPERAND}}}}";
    public static final String MONGO_IS_NULL_FILTER = "{{{OPERAND}}:{$eq:null}}";

    //Placeholders for condition replacements
    public static final String PLACEHOLDER_LEFT_OPERAND = "{{LEFT_OPERAND}}";
    public static final String PLACEHOLDER_RIGHT_OPERAND = "{{RIGHT_OPERAND}}";
    public static final String PLACEHOLDER_OPERAND = "{{OPERAND}}";
    public static final String PLACEHOLDER_FIELD_NAME = "{{FIELD_NAME}}";
    public static final String PLACEHOLDER_COMPARE_OPERATOR = "{{COMPARE_OPERATOR}}";

    public static final String CONNECTIONS_PER_HOST = "connectionsPerHost";
    public static final String HEARTBEAT_SOCKET_TIMEOUT = "heartbeatSocketTimeout";
    public static final String CONNECT_TIMEOUT = "connectTimeout";
    public static final String HEARTBEAT_FREQUENCY = "heartbeatFrequency";
    public static final String READ_CONCERN = "readConcern";
    public static final String WRITE_CONCERN = "writeConcern";
    public static final String HEARTBEAT_CONNECT_TIMEOUT = "heartbeatConnectTimeout";
    public static final String LOCAL_THRESHOLD = "localThreshold";
    public static final String MAX_CONNECTION_IDLE_TIME = "maxConnectionIdleTime";
    public static final String MAX_CONNECTION_LIFE_TIME = "maxConnectionLifeTime";
    public static final String MAX_WAIT_TIME = "maxWaitTime";
    public static final String MIN_CONNECTIONS_PER_HOST = "minConnectionsPerHost";
    public static final String MIN_HEARTBEAT_FREQUENCY = "minHeartbeatFrequency";
    public static final String SERVER_SELECTION_TIMEOUT = "serverSelectionTimeout";
    public static final String SOCKET_TIMEOUT = "socketTimeout";
    public static final String THREADS_ALLOWED_TO_BLOCK = "threadsAllowedToBlockForConnectionMultiplier";
    public static final String SOCKET_KEEP_ALIVE = "socketKeepAlive";
    public static final String SSL_ENABLED = "sslEnabled";
    public static final String CURSOR_FINALIZER_ENABLED = "cursorFinalizerEnabled";
    public static final String REQUIRED_REPLICA_SET_NAME = "requiredReplicaSetName";
    public static final String APPLICATION_NAME = "applicationName";
    public static final String READ_PREFERENCE = "readPreference";

    public static final String DEFAULT_TRUST_STORE_FILE = "${carbon.home}/resources/security/client-truststore.jks";
    public static final String DEFAULT_TRUST_STORE_PASSWORD = "wso2carbon";
    public static final String DEFAULT_KEY_STORE_FILE = "${carbon.home}/resources/security/client-truststore.jks";
    public static final String DEFAULT_KEY_STORE_PASSWORD = "wso2carbon";
    public static final String VARIABLE_CARBON_HOME = "carbon.home";

    private MongoTableConstants() {
    }
}
