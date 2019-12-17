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
 */
package org.wso2.extension.siddhi.store.ignite;

import org.wso2.siddhi.core.table.record.BaseExpressionVisitor;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.expression.condition.Compare;

import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Class which is used by the Siddhi runtime for instructions on converting the SiddhiQL condition to the condition
 * format understood by the Apache ignite store.
 */
public class ApacheIgniteConditionVisitor extends BaseExpressionVisitor {
    private StringBuilder condition;
    private String finalCompiledCondition;
    private Map<String, Object> placeholders;
    private Map<String, Object> placeholdersConstant;
    private SortedMap<Integer, Object> parameters;
    private SortedMap<Integer, Object> parametersConstant;
    private int streamVarCount;
    private int constantCount;

    public ApacheIgniteConditionVisitor() {
        this.condition = new StringBuilder();
        this.streamVarCount = 0;
        this.constantCount = 0;
        this.placeholders = new HashMap<>();
        this.placeholdersConstant = new HashMap<>();
        this.parameters = new TreeMap<>();
        this.parametersConstant = new TreeMap<>();
    }

    public String returnCondition() {
        this.parametrizeCondition();
        return this.finalCompiledCondition.trim();
    }

    public SortedMap<Integer, Object> getParameters() {
        return this.parameters;
    }

    public SortedMap<Integer, Object> getParametersConstant() {
        return this.parametersConstant;
    }

    @Override
    public void beginVisitAnd() {
        condition.append(ApacheIgniteConstants.OPEN_PARENTHESIS);
    }

    @Override
    public void endVisitAnd() {
        condition.append(ApacheIgniteConstants.CLOSE_PARENTHESIS);
    }

    @Override
    public void beginVisitAndLeftOperand() { }

    @Override
    public void endVisitAndLeftOperand() { }

    @Override
    public void beginVisitAndRightOperand() {
        condition.append(ApacheIgniteConstants.SQL_AND).append(ApacheIgniteConstants.WHITESPACE);
    }

    @Override
    public void endVisitAndRightOperand() { }

    @Override
    public void beginVisitOr() {
        condition.append(ApacheIgniteConstants.OPEN_PARENTHESIS);
    }

    @Override
    public void endVisitOr() {
        condition.append(ApacheIgniteConstants.CLOSE_PARENTHESIS);
    }

    @Override
    public void beginVisitOrLeftOperand() { }

    @Override
    public void endVisitOrLeftOperand() { }

    @Override
    public void beginVisitOrRightOperand() {
        condition.append(ApacheIgniteConstants.SQL_OR).append(ApacheIgniteConstants.WHITESPACE);
    }

    @Override
    public void endVisitOrRightOperand() { }

    @Override
    public void beginVisitNot() {
        condition.append(ApacheIgniteConstants.SQL_NOT).append(ApacheIgniteConstants.WHITESPACE);
    }

    @Override
    public void endVisitNot() { }

    @Override
    public void beginVisitCompare(Compare.Operator operator) {
        condition.append(ApacheIgniteConstants.OPEN_PARENTHESIS);
    }

    @Override
    public void endVisitCompare(Compare.Operator operator) {
        condition.append(ApacheIgniteConstants.CLOSE_PARENTHESIS);
    }

    @Override
    public void beginVisitCompareLeftOperand(Compare.Operator operator) { }

    @Override
    public void endVisitCompareLeftOperand(Compare.Operator operator) { }

    @Override
    public void beginVisitCompareRightOperand(Compare.Operator operator) {
        switch (operator) {
            case EQUAL:
                condition.append(ApacheIgniteConstants.SQL_COMPARE_EQUAL);
                break;
            case GREATER_THAN:
                condition.append(ApacheIgniteConstants.SQL_COMPARE_GREATER_THAN);
                break;
            case GREATER_THAN_EQUAL:
                condition.append(ApacheIgniteConstants.SQL_COMPARE_GREATER_THAN_EQUAL);
                break;
            case LESS_THAN:
                condition.append(ApacheIgniteConstants.SQL_COMPARE_LESS_THAN);
                break;
            case LESS_THAN_EQUAL:
                condition.append(ApacheIgniteConstants.SQL_COMPARE_LESS_THAN_EQUAL);
                break;
            case NOT_EQUAL:
                condition.append(ApacheIgniteConstants.SQL_COMPARE_NOT_EQUAL);
                break;
            default:
                throw new ApacheIgniteTableException("Ignite store does not support operator : " + operator);
        }
        condition.append(ApacheIgniteConstants.WHITESPACE);
    }

    @Override
    public void endVisitCompareRightOperand(Compare.Operator operator) { }

    @Override
    public void beginVisitIsNull(String streamId) { }

    @Override
    public void endVisitIsNull(String streamId) {
        condition.append(ApacheIgniteConstants.SQL_IS_NULL).append(ApacheIgniteConstants.WHITESPACE);
    }

    @Override
    public void beginVisitIn(String storeId) {
        condition.append(ApacheIgniteConstants.SQL_IN).append(ApacheIgniteConstants.WHITESPACE);
    }

    @Override
    public void endVisitIn(String storeId) { }

    @Override
    public void beginVisitConstant(Object value, Attribute.Type type) {
        String name = this.generateConstantName();
        this.placeholdersConstant.put(name, value);
        condition.append("[").append(name).append("]").append(ApacheIgniteConstants.WHITESPACE);
    }

    @Override
    public void endVisitConstant(Object value, Attribute.Type type) { }

    @Override
    public void beginVisitMath(MathOperator mathOperator) {
        condition.append(ApacheIgniteConstants.OPEN_PARENTHESIS);
    }

    @Override
    public void endVisitMath(MathOperator mathOperator) {
        condition.append(ApacheIgniteConstants.CLOSE_PARENTHESIS);
    }

    @Override
    public void beginVisitMathLeftOperand(MathOperator mathOperator) { }

    @Override
    public void endVisitMathLeftOperand(MathOperator mathOperator) { }

    @Override
    public void beginVisitMathRightOperand(MathOperator mathOperator) {
        switch (mathOperator) {
            case ADD:
                condition.append(ApacheIgniteConstants.SQL_MATH_ADD);
                break;
            case DIVIDE:
                condition.append(ApacheIgniteConstants.SQL_MATH_DIVIDE);
                break;
            case MOD:
                condition.append(ApacheIgniteConstants.SQL_MATH_MOD);
                break;
            case MULTIPLY:
                condition.append(ApacheIgniteConstants.SQL_MATH_MULTIPLY);
                break;
            case SUBTRACT:
                condition.append(ApacheIgniteConstants.SQL_MATH_SUBTRACT);
                break;
            default:
                throw new ApacheIgniteTableException("Ignite store does not support operation : " + mathOperator);
        }
        condition.append(ApacheIgniteConstants.WHITESPACE);
    }

    @Override
    public void endVisitMathRightOperand(MathOperator mathOperator) { }

    @Override
    public void beginVisitAttributeFunction(String namespace, String functionName) { }

    @Override
    public void endVisitAttributeFunction(String namespace, String functionName) { }

    @Override
    public void beginVisitParameterAttributeFunction(int index) { }

    @Override
    public void endVisitParameterAttributeFunction(int index) { }

    @Override
    public void beginVisitStreamVariable(String id, String streamId, String attributeName, Attribute.Type type) {
        String name = this.generateStreamVarName();
        this.placeholders.put(name, new Attribute(id, type));
        condition.append("[").append(name).append("]").append(ApacheIgniteConstants.WHITESPACE);
    }

    @Override
    public void endVisitStreamVariable(String id, String streamId, String attributeName, Attribute.Type type) { }

    @Override
    public void beginVisitStoreVariable(String storeId, String attributeName, Attribute.Type type) {
        condition.append(attributeName).append(ApacheIgniteConstants.WHITESPACE);
    }

    @Override
    public void endVisitStoreVariable(String storeId, String attributeName, Attribute.Type type) { }

    private void parametrizeCondition() {
        String query = this.condition.toString();
        String[] tokens = query.split("\\[");
        int ordinal = 1;
        int ordinalCon = 1;
        for (String token : tokens) {
            if (token.contains("]")) {
                String candidate = token.substring(0, token.indexOf("]"));
                if (this.placeholders.containsKey(candidate)) {
                    this.parameters.put(ordinal, this.placeholders.get(candidate));
                    ordinal++;
                } else if (this.placeholdersConstant.containsKey(candidate)) {
                    this.parametersConstant.put(ordinalCon, this.placeholdersConstant.get(candidate));
                    ordinalCon++;
                }
            }
        }
        for (String placeholder : this.placeholders.keySet()) {
            query = query.replace("[" + placeholder + "]", "?");
        }
        for (String placeholder : this.placeholdersConstant.keySet()) {
            query = query.replace("[" + placeholder + "]", "#");
        }
        this.finalCompiledCondition = query;
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
