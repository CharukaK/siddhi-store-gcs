/*
 * Copyright (c) 2019, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package io.siddhi.extension.store.gcs.beans;

import io.siddhi.extension.store.gcs.beans.StoreVariable;
import io.siddhi.extension.store.gcs.beans.StreamVariable;
import io.siddhi.query.api.expression.condition.Compare;

public class BasicCompareOperation {

    private Compare.Operator operator;
    private StoreVariable storeVariable;
    private StreamVariable streamVariable;

    public BasicCompareOperation() {
        operator = null;
        storeVariable = null;
        streamVariable = null;
    }

    public Compare.Operator getOperator() {
        return operator;
    }

    public void setOperator(Compare.Operator operator) {
        this.operator = operator;
    }

    public StoreVariable getStoreVariable() {
        return storeVariable;
    }

    public void setStoreVariable(StoreVariable storeVariable) {
        this.storeVariable = storeVariable;
    }

    public StreamVariable getStreamVariable() {
        return streamVariable;
    }

    public void setStreamVariable(StreamVariable streamVariable) {
        this.streamVariable = streamVariable;
    }

    public boolean isValid() {
        return (!(this.operator == null || this.streamVariable == null || this.storeVariable == null));
    }
}
