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

import io.siddhi.core.util.collection.operator.CompiledSelection;

/**
 * Implementation class of corresponding to projection, groupBy, having, orderBy, limit and offset conditions.
 */
public class MongoDBCompileSelection implements CompiledSelection {

    private MongoCompiledCondition selection;
    private MongoCompiledCondition groupBy;
    private String having;
    private String orderBy;
    private Long limit;
    private Long offset;

    public MongoDBCompileSelection(MongoCompiledCondition project, MongoCompiledCondition groupBy, String having,
                                   String orderBy, Long limit, Long offset) {
        this.selection = project;
        this.groupBy = groupBy;
        this.having = having;
        this.orderBy = orderBy;
        this.limit = limit;
        this.offset = offset;
    }

    public MongoCompiledCondition getSelection() {
        return selection;
    }

    public MongoCompiledCondition getGroupBy() {
        return groupBy;
    }

    public String getHaving() {
        return this.having;
    }

    public String getOrderBy() {
        return this.orderBy;
    }

    public Long getLimit() {
        return this.limit;
    }

    public Long getOffset() {
        return this.offset;
    }
}
