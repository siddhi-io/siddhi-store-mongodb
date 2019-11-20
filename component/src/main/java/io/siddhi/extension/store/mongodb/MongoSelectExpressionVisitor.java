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
        this.conditionOperands = new Stack<>();
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

    public boolean checkFunctionsUsed() {
        return this.isAttributeFunctionUsed;
    }

    @Override
    public void beginVisitConstant(Object value, Attribute.Type type) {
        conditionOperands.push("{\'$literal\':" + value + "}");
    }

    @Override
    public void endVisitConstant(Object value, Attribute.Type type) {
    }

    @Override
    public void beginVisitStoreVariable(String storeId, String attributeName, Attribute.Type type) {
        if (isCountFunction) {
            throw new MongoTableException("The MongoDB Event table does not support arguments for count function.");
        }
        conditionOperands.push("\'$" + attributeName + "\'");
    }

    @Override
    public void endVisitStoreVariable(String storeId, String attributeName, Attribute.Type type) {
    }

    @Override
    public void beginVisitStreamVariable(String id, String streamId, String attributeName, Attribute.Type type) {
        if (isCountFunction) {
            throw new MongoTableException("The MongoDB Event table does not support functions arguments for count " +
                    "function.");
        }
        if (isNullCheck) {
            throw new MongoTableException("The MongoDB Event table does not support null check for stream variables.");
        }
        if (type.toString().equalsIgnoreCase("STRING")) {
            conditionOperands.push("{\'$literal\':\'\'" + id + "\'\'}");
        } else {
            conditionOperands.push("{\'$literal\':\'" + id + "\'}");
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
                conditionOperands.push("{$sum:1}");
            } else {
                String functionArgument = conditionOperands.pop();
                conditionOperands.push("{$" + functionName + ":" + functionArgument + "}");
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
        conditionOperands.push("{$" + mathOperator.name().toLowerCase(Locale.ENGLISH) + ":" + "[" + leftOperand + "," +
                rightOperand + "]}");
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
        conditionOperands.push("{$or:" + "[" + leftOperand + "," + rightOperand + "]}");
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
        conditionOperands.push("{$and:" + "[" + leftOperand + "," + rightOperand + "]}");
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
        conditionOperands.push("{ $cond: { if: { $eq: [" + nullCheckAttribute + ", null ] }, then: true, " +
                "else: false } }");
    }

    @Override
    public void beginVisitCompare(Compare.Operator operator) {
    }

    @Override
    public void endVisitCompare(Compare.Operator operator) {
        String compareFilter;
        switch (operator) {
            case EQUAL:
                compareFilter = "$eq";
                break;
            case GREATER_THAN:
                compareFilter = "$gt";
                break;
            case GREATER_THAN_EQUAL:
                compareFilter = "$gte";
                break;
            case LESS_THAN:
                compareFilter = "$lt";
                break;
            case LESS_THAN_EQUAL:
                compareFilter = "$lte";
                break;
            case NOT_EQUAL:
                compareFilter = "$ne";
                break;
            default:
                throw new MongoTableException("MongoDB Event Table found unknown operator '" + operator + "' for " +
                        "COMPARE operation.");
        }
        String rightOperand = this.conditionOperands.pop();
        String leftOperand = this.conditionOperands.pop();
        conditionOperands.push("{ $cond: { if: { " + compareFilter + ": [" + leftOperand + "," + rightOperand +
                " ] }, then: true, else: false } }");
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
        throw new MongoTableException("MongoDB Event Table does not support NOT function.");
    }

    @Override
    public void endVisitNot() {
    }

    @Override
    public void beginVisitIn(String storeId) {
        throw new MongoTableException("MongoDB Event Table does not support IN function.");
    }

    @Override
    public void endVisitIn(String storeId) {
    }
}
