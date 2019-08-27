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

package io.siddhi.extension.store.gcs;

import io.siddhi.annotation.processor.StoreValidationAnnotationProcessor;
import io.siddhi.core.exception.OperationNotSupportedException;
import io.siddhi.core.table.record.BaseExpressionVisitor;
import io.siddhi.extension.store.gcs.beans.BasicCompareOperation;
import io.siddhi.extension.store.gcs.beans.StoreVariable;
import io.siddhi.extension.store.gcs.beans.StreamVariable;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.expression.condition.Compare;

public class GCSExpressionVisitor extends BaseExpressionVisitor {
    private BasicCompareOperation compareOperation;
    private boolean isVisitingRightCondition;
    private boolean isStoreVariableOnRight;

    public GCSExpressionVisitor() {
        compareOperation = new BasicCompareOperation();
    }

    public BasicCompareOperation getCompareOperation() {
        return compareOperation;
    }

    @Override
    public void beginVisitAnd() {
        throw new OperationNotSupportedException("And operations are not supported by the Google Cloud Storage" +
                " extension");
    }

    @Override
    public void beginVisitOr() {
        throw new OperationNotSupportedException("Or operations are not supported by the Google Cloud Storage" +
                " extension");
    }

    @Override
    public void beginVisitNot() {
        throw new OperationNotSupportedException("Not operations are not supported by the Google Cloud Storage" +
                " extension");
    }

    @Override
    public void beginVisitIsNull(String streamId) {
        throw new OperationNotSupportedException("IsNull operation is not supported by the Google Cloud Storage" +
                " extension");
    }

    @Override
    public void beginVisitIn(String storeId) {
        throw new OperationNotSupportedException("In operations are not supported by the Google Cloud Storage" +
                " extension");
    }

    @Override
    public void beginVisitMath(MathOperator mathOperator) {
        throw new OperationNotSupportedException("Math operations are not supported by the Google Cloud Storage" +
                " extension");
    }

    @Override
    public void beginVisitCompare(Compare.Operator operator) {
        this.compareOperation = new BasicCompareOperation();
    }

    @Override
    public void beginVisitCompareRightOperand(Compare.Operator operator) {
        isVisitingRightCondition = true;
    }

    @Override
    public void endVisitCompareLeftOperand(Compare.Operator operator) {
        super.endVisitCompareLeftOperand(operator);
    }

    @Override
    public void endVisitCompareRightOperand(Compare.Operator operator) {
        if(!isStoreVariableOnRight) {
            if(operator.equals(Compare.Operator.EQUAL)) {
                compareOperation.setOperator(Compare.Operator.EQUAL);
            } else {
                throw new OperationNotSupportedException("Google Cloud Storage extension only supports Equal " +
                        "comparison operator");
            }
        } else {
            isStoreVariableOnRight = false;
            if (operator.equals(Compare.Operator.EQUAL)) {
                compareOperation.setOperator(Compare.Operator.EQUAL);
            } else {
                throw new OperationNotSupportedException("Google Cloud Storage extension only supports Equal " +
                        "comparison operator");
            }
        }
    }

    @Override
    public void beginVisitStreamVariable(String id, String streamId, String attributeName, Attribute.Type type) {
        compareOperation.setStreamVariable(new StreamVariable(id));
    }

    @Override
    public void beginVisitStoreVariable(String storeId, String attributeName, Attribute.Type type) {
        if (isVisitingRightCondition) {
            isStoreVariableOnRight = true;
        }

        compareOperation.setStoreVariable(new StoreVariable(attributeName));
    }

    @Override
    public void endVisitConstant(Object value, Attribute.Type type) {
        StreamVariable streamVariable = new StreamVariable(value.toString());
        compareOperation.setStreamVariable(streamVariable);
    }


}
