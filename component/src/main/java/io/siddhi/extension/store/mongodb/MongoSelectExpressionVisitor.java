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

import java.util.Arrays;

/**
 * Class representing MongoDB select attribute condition implementation.
 */
public class MongoSelectExpressionVisitor extends BaseExpressionVisitor {

    private int streamVarCount;
    private int constantCount;
    private StringBuilder compileString;
    private String[] supportedFunctions = {"sum", "avg", "min", "max", "count"};
    private int mathOperandCount;
    private int logicalOperatorCount;
    private boolean isCountFunction;
    private boolean isNullCheck;

    public MongoSelectExpressionVisitor() {
        this.streamVarCount = 0;
        this.constantCount = 0;
        this.compileString = new StringBuilder();
        this.mathOperandCount = 0;
        this.logicalOperatorCount = 0;
        this.isCountFunction = false;
        this.isNullCheck = false;
    }

    public String getCompiledCondition() {
        return compileString.toString();
    }

    public int getStreamVarCount() {
        return this.streamVarCount;
    }

    public int getConstantCount() {
        return this.constantCount;
    }

    public String[] getSupportedFunctions() {
        return this.supportedFunctions;
    }


    @Override
    public void beginVisitConstant(Object value, Attribute.Type type) {
        if (mathOperandCount == 0 && logicalOperatorCount == 0) {
            compileString.append(":");
        }
        compileString.append("{\'$literal\':").append(value).append("}");
    }

    @Override
    public void endVisitConstant(Object value, Attribute.Type type) {
        if (logicalOperatorCount > 0) {
            compileString.append(",");
        }
    }

    @Override
    public void beginVisitStoreVariable(String storeId, String attributeName, Attribute.Type type) {
        if (!isCountFunction && !isNullCheck) {
            if (mathOperandCount == 0 && logicalOperatorCount == 0) {
                compileString.append(':');
            }
            compileString.append("\'$").append(attributeName).append("\'");
        } else if (isNullCheck) {
            compileString.append("\'$").append(attributeName).append("\'");
        }
        if (isCountFunction) {
            throw new MongoTableException("The MongoDB Event table does not support arguments \n" +
                    "for count function.");
        }
    }

    @Override
    public void endVisitStoreVariable(String storeId, String attributeName, Attribute.Type type) {
        if (logicalOperatorCount > 0) {
            compileString.append(",");
        }
    }

    @Override
    public void beginVisitStreamVariable(String id, String streamId, String attributeName, Attribute.Type type) {
        if (!isCountFunction) {
            if (logicalOperatorCount > 0 || mathOperandCount > 0) {
                compileString.append("{\'$literal\':\'").append(id).append("\'}");
            } else {
                if (type.toString().equalsIgnoreCase("STRING")) {
                    compileString.append(":{\'$literal\':\'\'").append(id).append("\'\'}");
                } else {
                    compileString.append(":{\'$literal\':\'").append(id).append("\'}");
                }
            }
        } else {
            throw new MongoTableException("The MongoDB Event table does not support functions arguments \" +\n" +
                    " \"for count function.");
        }
    }

    @Override
    public void endVisitStreamVariable(String id, String streamId, String attributeName, Attribute.Type type) {
        if (logicalOperatorCount > 0) {
            compileString.append(",");
        }
    }

    @Override
    public void beginVisitAttributeFunction(String namespace, String functionName) {
        if (MongoTableUtils.isEmpty(namespace) &&
                (Arrays.asList(supportedFunctions).contains(functionName))) {
            if (functionName.equalsIgnoreCase("count")) {
                this.isCountFunction = true;
                compileString.append(":{$sum:1");
            } else {
                compileString.append(":{$").append(functionName);
            }

        } else {
            throw new MongoTableException("The MongoDB Event table does not support functions other than \" +\n" +
                    " \"sum(), avg(), min(), max() and count().");
        }
    }

    @Override
    public void endVisitAttributeFunction(String namespace, String functionName) {
        if (isCountFunction) {
            this.isCountFunction = false;
        }
    }

    @Override
    public void beginVisitMath(MathOperator mathOperator) {
        this.mathOperandCount++;
        String operatorName = mathOperator.name().toLowerCase();
        compileString.append((mathOperandCount == 1) ? ":{$" + operatorName + ":[" : "{$" + operatorName + ":[");
    }

    @Override
    public void endVisitMath(MathOperator mathOperator) {
        compileString.append("]}");
        this.mathOperandCount--;
    }

    @Override
    public void beginVisitMathLeftOperand(MathOperator mathOperator) {
    }

    @Override
    public void endVisitMathLeftOperand(MathOperator mathOperator) {
        if (mathOperandCount > 0) {
            compileString.append(",");
        }
    }

    @Override
    public void beginVisitMathRightOperand(MathOperator mathOperator) {
    }

    @Override
    public void endVisitMathRightOperand(MathOperator mathOperator) {
    }

    @Override
    public void beginVisitAnd() {
        this.logicalOperatorCount++;
        compileString.append((logicalOperatorCount == 1) ? ":{$and:[" : "{$and:[");
    }

    @Override
    public void endVisitAnd() {
        if (compileString.charAt(compileString.length() - 1) == ',') {
            compileString.setLength(compileString.length() - 1);
        }
        compileString.append((logicalOperatorCount == 1) ? "]}" : "]},");
        this.logicalOperatorCount--;
    }

    @Override
    public void beginVisitOr() {
        this.logicalOperatorCount++;
        compileString.append((logicalOperatorCount == 1) ? ":{$or:[" : "{$or:[");
    }

    @Override
    public void endVisitOr() {
        if (compileString.charAt(compileString.length() - 1) == ',') {
            compileString.setLength(compileString.length() - 1);
        }
        compileString.append((logicalOperatorCount == 1) ? "]}" : "]},");
        this.logicalOperatorCount--;
    }

    @Override
    public void beginVisitIsNull(String streamId) {
        this.isNullCheck = true;
        compileString.append(": { $cond: { if: { $eq: [");
    }

    @Override
    public void endVisitIsNull(String streamId) {
        this.isNullCheck = false;
        compileString.append(", null ] }, then: true, else: false } }");
    }
}
