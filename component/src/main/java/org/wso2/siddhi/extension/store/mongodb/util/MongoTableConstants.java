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
package org.wso2.siddhi.extension.store.mongodb.util;

/**
 * Class which holds the constants required by the MongoDB Event Table implementation.
 */
public class MongoTableConstants {

    //Annotation field names
    public static final String ANNOTATION_ELEMENT_URI = "mongodb.uri";
    public static final String ANNOTATION_ELEMENT_COLLECTION_NAME = "collection.name";

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
    public static final String REG_INDEX_BY = "^(\\S*)\\s(1|-1)\\s(\\{.*})$";

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

    private MongoTableConstants() {
    }
}
