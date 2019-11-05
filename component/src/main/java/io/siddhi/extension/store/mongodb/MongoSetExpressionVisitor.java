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

import io.siddhi.core.table.record.BaseExpressionVisitor;
import io.siddhi.extension.store.mongodb.exception.MongoTableException;
import io.siddhi.extension.store.mongodb.util.Constant;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.expression.condition.Compare;

import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

/**
 * Class which is used by the Siddhi runtime for instructions on converting the SiddhiQL condition to the condition
 * format understood by the MongoDB.
 */
public class MongoSetExpressionVisitor extends BaseExpressionVisitor {
    private Stack<String> conditionOperands;
    private Map<String, Object> placeholders;

    private int streamVarCount;
    private int constantCount;

    public MongoSetExpressionVisitor() {
        this.streamVarCount = 0;
        this.constantCount = 0;
        this.conditionOperands = new Stack<>();
        this.placeholders = new HashMap<>();
    }

    public String getCompiledCondition() {
        String compiledCondition = this.conditionOperands.pop();
        for (Map.Entry<String, Object> entry : this.placeholders.entrySet()) {
            if (entry.getValue() instanceof Constant) {
                Constant constant = (Constant) entry.getValue();
                if (constant.getType().equals(Attribute.Type.STRING)) {
                    compiledCondition = compiledCondition.replaceAll(entry.getKey(),
                            "'" + constant.getValue().toString() + "'");
                } else {
                    compiledCondition = compiledCondition.replaceAll(entry.getKey(),
                            constant.getValue().toString());
                }
                this.placeholders.remove(entry.getKey());
            }
        }
        return compiledCondition;
    }

    public Map<String, Object> getPlaceholders() {
        return placeholders;
    }

    public Stack<String> getConditionOperands(){
        return this.conditionOperands;
    }

    public int getStreamVarCount(){
        return this.streamVarCount;
    }

    public int getConstantCount(){
        return this.constantCount;
    }

    @Override
    public void beginVisitAnd() {
    }

    @Override
    public void endVisitAnd() {
        throw new MongoTableException("MongoDB Event Table does not support SET function.");
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
    public void beginVisitOr() {
    }

    @Override
    public void endVisitOr() {
        throw new MongoTableException("MongoDB Event Table does not support SET function.");
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
    public void beginVisitNot() {
    }

    @Override
    public void endVisitNot() {
        throw new MongoTableException("MongoDB Event Table does not support SET function.");
    }

    @Override
    public void beginVisitCompare(Compare.Operator operator) {
    }

    @Override
    public void endVisitCompare(Compare.Operator operator) {
        throw new MongoTableException("MongoDB Event Table does not support SET function.");
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
    public void beginVisitIsNull(String streamId) {


    }

    @Override
    public void endVisitIsNull(String streamId) {
        throw new MongoTableException("MongoDB Event Table does not support SET function.");
    }

    @Override
    public void beginVisitIn(String storeId) {
        throw new MongoTableException("MongoDB Event Table does not support SET function.");
    }

    @Override
    public void endVisitIn(String storeId) {
    }

    @Override
    public void beginVisitMath(MathOperator mathOperator) {
        throw new MongoTableException("MongoDB Event Table does not support SET function.");
    }

    @Override
    public void endVisitMath(MathOperator mathOperator) {
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
    public void beginVisitAttributeFunction(String namespace, String functionName) {
       throw new MongoTableException("MongoDB Event Table does not support SET function.");
    }


    @Override
    public void endVisitAttributeFunction(String namespace, String functionName) {
    }

    @Override
    public void beginVisitParameterAttributeFunction(int index) {
    }

    @Override
    public void endVisitParameterAttributeFunction(int index) {
    }

    @Override
    public void beginVisitStreamVariable(String id, String streamId, String attributeName, Attribute.Type type) {
        String name = this.generateStreamVarName();
        this.placeholders.put(name, new Attribute(id, type));
        conditionOperands.push(name);
    }

    @Override
    public void endVisitStreamVariable(String id, String streamId, String attributeName, Attribute.Type type) {
    }

    @Override
    public void beginVisitConstant(Object value, Attribute.Type type) {
        String name = this.generateConstantName();
        this.placeholders.put(name, new Constant(value, type));
        conditionOperands.push(name);
    }

    @Override
    public void endVisitConstant(Object value, Attribute.Type type) {
    }

    @Override
    public void beginVisitStoreVariable(String storeId, String attributeName, Attribute.Type type) {
        this.conditionOperands.push(attributeName);
    }

    @Override
    public void endVisitStoreVariable(String storeId, String attributeName, Attribute.Type type) {
    }

    /**
     * Method for generating a temporary placeholder for stream variables.
     *
     * @return a placeholder string of known format.
     */
    private String generateStreamVarName() {
        String name = "strVar" + this.streamVarCount;
        this.streamVarCount++;
        return name;
    }

    /**
     * Method for generating a temporary placeholder for constants.
     *
     * @return a placeholder string of known format.
     */
    private String generateConstantName() {
        String name = "const" + this.constantCount;
        this.constantCount++;
        return name;
    }
}
