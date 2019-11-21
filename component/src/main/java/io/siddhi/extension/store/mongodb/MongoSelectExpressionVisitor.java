/*
 * Copyright (c) 2019, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

import io.siddhi.core.table.record.BaseExpressionVisitor;
import io.siddhi.extension.store.mongodb.exception.MongoTableException;
import io.siddhi.extension.store.mongodb.util.MongoTableConstants;
import io.siddhi.extension.store.mongodb.util.MongoTableUtils;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.expression.condition.Compare;

import java.util.Arrays;
import java.util.Locale;
import java.util.Stack;

/**
 * Class representing MongoDB select attribute condition implementation.
 */
public class MongoSelectExpressionVisitor extends BaseExpressionVisitor {

    private Stack<String> conditionOperands;
    private int streamVarCount;
    private int constantCount;
    private String[] supportedFunctions = {"sum", "avg", "min", "max", "count"};
    private boolean isAttributeFunctionUsed;
    private boolean isCountFunction;
    private boolean isNullCheck;

    public MongoSelectExpressionVisitor() {
        this.conditionOperands = new Stack<String>();
        this.streamVarCount = 0;
        this.constantCount = 0;
        this.isCountFunction = false;
        this.isNullCheck = false;
        this.isAttributeFunctionUsed = false;
    }

    public String getCompiledCondition() {
        return conditionOperands.pop();
    }

    public int getStreamVarCount() {
        return this.streamVarCount;
    }

    public int getConstantCount() {
        return this.constantCount;
    }

    public boolean isFunctionsPresent() {
        return this.isAttributeFunctionUsed;
    }

    @Override
    public void beginVisitConstant(Object value, Attribute.Type type) {
        String constantAttribute = MongoTableConstants.MONGO_LITERAL_ATTRIBUTE
                .replace(MongoTableConstants.PLACEHOLDER_FIELD_NAME, value.toString());
        conditionOperands.push(constantAttribute);
    }

    @Override
    public void endVisitConstant(Object value, Attribute.Type type) {
    }

    @Override
    public void beginVisitStoreVariable(String storeId, String attributeName, Attribute.Type type) {
        if (isCountFunction) {
            throw new MongoTableException("The MongoDB Event table does not support arguments in count function.");
        }
        String storeAttribute = MongoTableConstants.MONGO_STORE_ATTRIBUTE
                .replace(MongoTableConstants.PLACEHOLDER_FIELD_NAME, attributeName);
        conditionOperands.push(storeAttribute);
    }

    @Override
    public void endVisitStoreVariable(String storeId, String attributeName, Attribute.Type type) {
    }

    @Override
    public void beginVisitStreamVariable(String id, String streamId, String attributeName, Attribute.Type type) {
        if (isCountFunction) {
            throw new MongoTableException("The MongoDB Event table does not support arguments in count function.");
        }
        if (isNullCheck) {
            throw new MongoTableException("The MongoDB Event table does not support 'is null' condition with " +
                    "stream variables.");
        }
        if (type.toString().equalsIgnoreCase("STRING")) {
            String streamAttribute = MongoTableConstants.MONGO_STREAM_STRING_ATTRIBUTE
                    .replace(MongoTableConstants.PLACEHOLDER_FIELD_NAME, id);
            conditionOperands.push(streamAttribute);
        } else {
            String streamAttribute = MongoTableConstants.MONGO_STREAM_ATTRIBUTE
                    .replace(MongoTableConstants.PLACEHOLDER_FIELD_NAME, id);
            conditionOperands.push(streamAttribute);
        }
    }

    @Override
    public void endVisitStreamVariable(String id, String streamId, String attributeName, Attribute.Type type) {
    }

    @Override
    public void beginVisitAttributeFunction(String namespace, String functionName) {
        if (!MongoTableUtils.isEmpty(namespace) ||
                (!Arrays.asList(supportedFunctions).contains(functionName))) {
            throw new MongoTableException("The MongoDB Event table does not support functions other than sum(), " +
                    "avg(), min(), max() and count().");
        }
        if (functionName.equalsIgnoreCase("count")) {
            this.isCountFunction = true;
        }
    }

    @Override
    public void endVisitAttributeFunction(String namespace, String functionName) {
        if (MongoTableUtils.isEmpty(namespace) &&
                (Arrays.asList(supportedFunctions).contains(functionName))) {
            this.isAttributeFunctionUsed = true;
            if (functionName.equalsIgnoreCase("count")) {
                this.isCountFunction = false;
                conditionOperands.push(MongoTableConstants.MONGO_GROUPBY_COUNT_RECORDS);
            } else {
                String functionArgument = conditionOperands.pop();
                String functionFilter = MongoTableConstants.MONGO_FUNCTION_FILTER
                        .replace(MongoTableConstants.PLACEHOLDER_FUNCTION, functionName)
                        .replace(MongoTableConstants.PLACEHOLDER_FUNCTION_ARGUMENT, functionArgument);
                conditionOperands.push(functionFilter);
            }
        }
    }

    @Override
    public void beginVisitParameterAttributeFunction(int index) {
    }

    @Override
    public void endVisitParameterAttributeFunction(int index) {
    }

    @Override
    public void beginVisitMath(MathOperator mathOperator) {
    }

    @Override
    public void endVisitMath(MathOperator mathOperator) {
        String rightOperand = this.conditionOperands.pop();
        String leftOperand = this.conditionOperands.pop();
        String mathOperatorName = mathOperator.name().toLowerCase(Locale.ENGLISH);
        String mathFilter = MongoTableConstants.MONGO_MATH_FILTER
                .replace(MongoTableConstants.MATH_OPERATOR, mathOperatorName)
                .replace(MongoTableConstants.PLACEHOLDER_LEFT_OPERAND, leftOperand)
                .replace(MongoTableConstants.PLACEHOLDER_RIGHT_OPERAND, rightOperand);
        conditionOperands.push(mathFilter);
    }

    @Override
    public void beginVisitMathLeftOperand(MathOperator mathOperator) {
    }

    @Override
    public void endVisitMathLeftOperand(MathOperator mathOperator) {
    }

    @Override
    public void beginVisitMathRightOperand(MathOperator mathOperator) {
    }

    @Override
    public void endVisitMathRightOperand(MathOperator mathOperator) {
    }

    @Override
    public void beginVisitOr() {
    }

    @Override
    public void endVisitOr() {
        String rightOperand = this.conditionOperands.pop();
        String leftOperand = this.conditionOperands.pop();
        String orFilter = MongoTableConstants.MONGO_OR_FILTER
                .replace(MongoTableConstants.PLACEHOLDER_LEFT_OPERAND, leftOperand)
                .replace(MongoTableConstants.PLACEHOLDER_RIGHT_OPERAND, rightOperand);
        this.conditionOperands.push(orFilter);
    }

    @Override
    public void beginVisitOrLeftOperand() {
    }

    @Override
    public void endVisitOrLeftOperand() {
    }

    @Override
    public void beginVisitOrRightOperand() {
    }

    @Override
    public void endVisitOrRightOperand() {
    }

    @Override
    public void beginVisitAnd() {
    }

    @Override
    public void endVisitAnd() {
        String rightOperand = this.conditionOperands.pop();
        String leftOperand = this.conditionOperands.pop();
        String andFilter = MongoTableConstants.MONGO_AND_FILTER
                .replace(MongoTableConstants.PLACEHOLDER_LEFT_OPERAND, leftOperand)
                .replace(MongoTableConstants.PLACEHOLDER_RIGHT_OPERAND, rightOperand);
        conditionOperands.push(andFilter);
    }

    @Override
    public void beginVisitAndLeftOperand() {
    }

    @Override
    public void endVisitAndLeftOperand() {
    }

    @Override
    public void beginVisitAndRightOperand() {
    }

    @Override
    public void endVisitAndRightOperand() {
    }

    @Override
    public void beginVisitIsNull(String streamId) {
        this.isNullCheck = true;
    }

    @Override
    public void endVisitIsNull(String streamId) {
        this.isNullCheck = false;
        String nullCheckAttribute = conditionOperands.pop();
        String compareOperator = MongoTableConstants.MONGO_COMPARE_EQUAL;
        String compareFilter = MongoTableConstants.MONGO_IF_ELSE_CONDITION
                .replace(MongoTableConstants.PLACEHOLDER_COMPARE_OPERATOR, compareOperator)
                .replace(MongoTableConstants.PLACEHOLDER_LEFT_OPERAND, nullCheckAttribute)
                .replace(MongoTableConstants.PLACEHOLDER_RIGHT_OPERAND, "null");
        conditionOperands.push(compareFilter);
    }

    @Override
    public void beginVisitCompare(Compare.Operator operator) {
    }

    @Override
    public void endVisitCompare(Compare.Operator operator) {
        String compareOperator;
        switch (operator) {
            case EQUAL:
                compareOperator = MongoTableConstants.MONGO_COMPARE_EQUAL;
                break;
            case GREATER_THAN:
                compareOperator = MongoTableConstants.MONGO_COMPARE_GREATER_THAN;
                break;
            case GREATER_THAN_EQUAL:
                compareOperator = MongoTableConstants.MONGO_COMPARE_GREATER_THAN_EQUAL;
                break;
            case LESS_THAN:
                compareOperator = MongoTableConstants.MONGO_COMPARE_LESS_THAN;
                break;
            case LESS_THAN_EQUAL:
                compareOperator = MongoTableConstants.MONGO_COMPARE_LESS_THAN_EQUAL;
                break;
            case NOT_EQUAL:
                compareOperator = MongoTableConstants.MONGO_COMPARE_NOT_EQUAL;
                break;
            default:
                throw new MongoTableException("MongoDB Event Table found unknown operator '" + operator + "' for " +
                        "COMPARE operation.");
        }
        String rightOperand = this.conditionOperands.pop();
        String leftOperand = this.conditionOperands.pop();
        String compareFilter = MongoTableConstants.MONGO_IF_ELSE_CONDITION
                .replace(MongoTableConstants.PLACEHOLDER_COMPARE_OPERATOR, compareOperator)
                .replace(MongoTableConstants.PLACEHOLDER_LEFT_OPERAND, leftOperand)
                .replace(MongoTableConstants.PLACEHOLDER_RIGHT_OPERAND, rightOperand);
        conditionOperands.push(compareFilter);
    }

    @Override
    public void beginVisitCompareLeftOperand(Compare.Operator operator) {
    }

    @Override
    public void endVisitCompareLeftOperand(Compare.Operator operator) {
    }

    @Override
    public void beginVisitCompareRightOperand(Compare.Operator operator) {
    }

    @Override
    public void endVisitCompareRightOperand(Compare.Operator operator) {
    }

    @Override
    public void beginVisitNot() {
        throw new MongoTableException("MongoDB Event Table does not support 'NOT' function.");
    }

    @Override
    public void endVisitNot() {
    }

    @Override
    public void beginVisitIn(String storeId) {
        throw new MongoTableException("MongoDB Event Table does not support 'IN' function.");
    }

    @Override
    public void endVisitIn(String storeId) {
    }
}
