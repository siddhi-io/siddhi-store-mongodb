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

import com.mongodb.DBObject;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Collation;
import com.mongodb.client.model.CollationAlternate;
import com.mongodb.client.model.CollationCaseFirst;
import com.mongodb.client.model.CollationMaxVariable;
import com.mongodb.client.model.CollationStrength;
import com.mongodb.client.model.IndexModel;
import com.mongodb.client.model.IndexOptions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.json.JsonParseException;
import org.wso2.siddhi.extension.store.mongodb.MongoCompiledCondition;
import org.wso2.siddhi.extension.store.mongodb.exception.MongoTableException;
import org.wso2.siddhi.query.api.annotation.Annotation;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.wso2.siddhi.extension.store.mongodb.util.MongoTableConstants.REG_INDEX_BY;


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
     * and is made up of attributes.
     *
     * @param primaryKey the PrimaryKey annotation which contains the primary key attributes.
     * @param attributes list of attributes form the table definition.
     * @return List of String with primary key attributes.
     */
    public static IndexModel extractPrimaryKey(Annotation primaryKey, List<Attribute> attributes) {
        if (primaryKey == null) {
            return null;
        }
        List<String> attributesNames = attributes.stream().map(Attribute::getName).collect(Collectors.toList());
        Document primaryKeyIndex = new Document();
        primaryKey.getElements().forEach(
                element -> {
                    if (!isEmpty(element.getValue()) && attributesNames.contains(element.getValue())) {
                        primaryKeyIndex.append(element.getValue(), 1);
                    } else {
                        throw new MongoTableException("Annotation '" + primaryKey.getName() + "' contains value '" +
                                element.getValue() + "' which is not present in the attributes of the Event Table.");
                    }
                }
        );
        return new IndexModel(primaryKeyIndex, new IndexOptions().unique(true));
    }

    /**
     * Utility method which can be used to check if the given Indices are valid  and return List of
     * MongoDB Index Models when valid.
     *
     * @param indices    the IndexBy annotation which contains the indices definitions.
     * @param attributes list of attributes form the table definition.
     * @return List of IndexModel.
     */
    public static List<IndexModel> extractIndexModels(Annotation indices, List<Attribute> attributes) {
        if (indices == null) {
            return new ArrayList<>();
        }
        Pattern pattern = Pattern.compile(REG_INDEX_BY);
        List<String> attributesNames = attributes.stream().map(Attribute::getName).collect(Collectors.toList());
        return indices.getElements().stream().map(index -> {
            Matcher matcher = pattern.matcher(index.getValue());
            if (matcher.matches()) {
                if (attributesNames.contains(matcher.group(1))) {
                    return createIndexModel(matcher.group(1), Integer.parseInt(matcher.group(2)), matcher.group(3));
                } else {
                    throw new MongoTableException("Annotation '" + indices.getName() + "' contains illegal " +
                            "value(s). Please check your query and try again.");
                }
            } else {
                if (attributesNames.contains(index.getValue())) {
                    return createIndexModel(index.getValue(), 1, null);
                }
                throw new MongoTableException("Annotation '" + indices.getName() + "' contains illegal value(s). " +
                        "Please check your query and try again.");
            }
        }).collect(Collectors.toList());
    }

    /**
     * Utility method which can be used to populate single IndexModel.
     *
     * @param fieldName   the attribute on which the index is to be created.
     * @param sortOrder   the sort order of the index to be created.
     * @param indexOption json string of the options of the index to be created.
     * @return IndexModel.
     */
    private static IndexModel createIndexModel(String fieldName, Integer sortOrder, String indexOption) {
        Document indexDocument = new Document(fieldName, sortOrder);
        if (indexOption == null) {
            return new IndexModel(indexDocument);
        } else {
            IndexOptions indexOptions = new IndexOptions();
            Document indexOptionDocument;
            try {
                indexOptionDocument = Document.parse(indexOption);
                for (Map.Entry<String, Object> indexEntry : indexOptionDocument.entrySet()) {
                    switch (indexEntry.getKey()) {
                        case "unique":
                            indexOptions.unique(Boolean.parseBoolean(indexEntry.getValue().toString()));
                            break;
                        case "background":
                            indexOptions.background(Boolean.parseBoolean(indexEntry.getValue().toString()));
                            break;
                        case "name":
                            indexOptions.name(indexEntry.getValue().toString());
                            break;
                        case "sparse":
                            indexOptions.sparse(Boolean.parseBoolean(indexEntry.getValue().toString()));
                            break;
                        case "expireAfterSeconds":
                            indexOptions.expireAfter(Long.parseLong(indexEntry.getValue().toString()), TimeUnit.SECONDS);
                            break;
                        case "version":
                            indexOptions.version(Integer.parseInt(indexEntry.getValue().toString()));
                            break;
                        case "weights":
                            indexOptions.weights((Bson) indexEntry.getValue());
                            break;
                        case "languageOverride":
                            indexOptions.languageOverride(indexEntry.getValue().toString());
                            break;
                        case "defaultLanguage":
                            indexOptions.defaultLanguage(indexEntry.getValue().toString());
                            break;
                        case "textVersion":
                            indexOptions.textVersion(Integer.parseInt(indexEntry.getValue().toString()));
                            break;
                        case "sphereVersion":
                            indexOptions.sphereVersion(Integer.parseInt(indexEntry.getValue().toString()));
                            break;
                        case "bits":
                            indexOptions.bits(Integer.parseInt(indexEntry.getValue().toString()));
                            break;
                        case "min":
                            indexOptions.min(Double.parseDouble(indexEntry.getValue().toString()));
                            break;
                        case "max":
                            indexOptions.max(Double.parseDouble(indexEntry.getValue().toString()));
                            break;
                        case "bucketSize":
                            indexOptions.bucketSize(Double.parseDouble(indexEntry.getValue().toString()));
                            break;
                        case "partialFilterExpression":
                            indexOptions.partialFilterExpression((Bson) indexEntry.getValue());
                            break;
                        case "collation":
                            DBObject collationOptions = (DBObject) indexEntry.getValue();
                            Collation.Builder builder = Collation.builder();
                            for (String collationKey : collationOptions.keySet()) {
                                String collationObj = indexEntry.getValue().toString();
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
                                        log.warn("Annotation 'IndexBy' for the field '" + fieldName + "' contains " +
                                                "unknown 'Collation' Option key : '" + collationKey + "'. Please check " +
                                                "your query and try again.");
                                        break;
                                }
                            }
                            if (builder.build().getLocale() != null) {
                                indexOptions.collation(builder.build());
                            } else {
                                throw new MongoTableException("Annotation 'IndexBy' for the field '" + fieldName + "' " +
                                        "do not contain option for locale. Please check your query and try again.");
                            }
                            break;
                        case "storageEngine":
                            indexOptions.storageEngine((Bson) indexOptionDocument.get("storageEngine"));
                            break;
                        default:
                            log.warn("Annotation 'IndexBy' for the field '" + fieldName + "' contains unknown option key" +
                                    " : '" + indexEntry.getKey() + "'. Please check your query and try again.");
                            break;
                    }
                }
            } catch (JsonParseException | NumberFormatException e) {
                throw new MongoTableException("Annotation 'IndexBy' for the field '" + fieldName + "' contains illegal " +
                        "value(s) for index option. Please check your query and try again.", e);
            }
            return new IndexModel(indexDocument, indexOptions);
        }
    }

    /**
     * Utility method which can be resolve the condition with the runtime values and return a Document describing the
     * filter.
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
     * Utility method tp map the values to the respective attributes before database operations.
     *
     * @param record              Object array of the runtime values.
     * @param attributesPositions Map containing the attribute position and name.
     * @return Document
     */
    public static Map<String, Object> mapValuestoAttributes(Object[] record, Map<Integer, String> attributesPositions) {
        Map<String, Object> attributesValuesMap = new HashMap<>();
        for (int i = 0; i < record.length; i++) {
            attributesValuesMap.put(attributesPositions.get(i), record[i]);
        }
        return attributesValuesMap;
    }

    /**
     * Utility method which can be used to check if the existing indices contains the expected indices
     * defined by the annotation 'PrimaryKey' and 'IndexBy'
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
            indexOptionsMap.put("textVersion", expectedIndexOptions.getTextVersion());
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
}

