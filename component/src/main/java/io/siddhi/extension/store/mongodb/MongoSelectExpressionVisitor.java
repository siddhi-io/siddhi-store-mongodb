package io.siddhi.extension.store.mongodb;

import io.siddhi.core.table.record.BaseExpressionVisitor;
import io.siddhi.extension.store.mongodb.exception.MongoTableException;
import io.siddhi.extension.store.mongodb.util.MongoTableUtils;
import io.siddhi.query.api.definition.Attribute;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

public class MongoSelectExpressionVisitor extends BaseExpressionVisitor {

    private Stack<String> conditionOperands;
    private Map<String, Object> placeholders;

    private int streamVarCount;
    private int constantCount;

    private StringBuilder compileString;

    private String[] supportedFunctions = {"sum", "avg", "min", "max"};

    public MongoSelectExpressionVisitor() {
        this.streamVarCount = 0;
        this.constantCount = 0;
        this.conditionOperands = new Stack<>();
        this.placeholders = new HashMap<>();
        this.compileString = new StringBuilder();
    }

    public String getCompiledCondition() {
        return compileString.toString();
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
    public void beginVisitConstant(Object value, Attribute.Type type) {
        compileString.append(":{\'$literal\':"+value+"}");
    }

    @Override
    public void endVisitConstant(Object value, Attribute.Type type) {
    }

    @Override
    public void beginVisitStoreVariable(String storeId, String attributeName, Attribute.Type type) {
        compileString.append(":\'$"+attributeName+"\'");
    }

    @Override
    public void endVisitStoreVariable(String storeId, String attributeName, Attribute.Type type) {
    }

    @Override
    public void beginVisitStreamVariable(String id, String streamId, String attributeName, Attribute.Type type) {
        compileString.append(":{\'$literal\':\'?\'}");
    }

    @Override
    public void endVisitStreamVariable(String id, String streamId, String attributeName, Attribute.Type type) {
    }

    @Override
    public void beginVisitAttributeFunction(String namespace, String functionName) {
        if(MongoTableUtils.isEmpty(namespace) &&
                (Arrays.stream(supportedFunctions).anyMatch(functionName::equals))){
            compileString.append(":{$"+functionName);
        } else{
            throw new MongoTableException("The RDBMS Event table does not support functions other than \" +\n" +
                    " \"sum(), avg(), min(), max().");
        }
    }


    @Override
    public void endVisitAttributeFunction(String namespace, String functionName) {
    }

}
