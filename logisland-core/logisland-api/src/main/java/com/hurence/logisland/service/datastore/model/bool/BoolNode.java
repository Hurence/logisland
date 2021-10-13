/**
 * Copyright (C) 2016 Hurence (support@hurence.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hurence.logisland.service.datastore.model.bool;

import java.util.ArrayList;
import java.util.List;

public class BoolNode<T> {
    private T data;
    private BoolNode<T> parent;
    private BoolCondition boolCondition;
    private List<BoolNode<T>> children = new ArrayList<>();

    public boolean isRoot() {
        return parent == null;
    }

    public boolean isLeaf() {
        return children.size() == 0;
    }

    public T getData() {
        return data;
    }

    public BoolNode<T> setData(T data) {
        this.data = data;
        return this;
    }

    public BoolNode<T> getParent() {
        return parent;
    }

    public BoolNode<T> setParent(BoolNode<T> parent) {
        this.parent = parent;
        return this;
    }

    public List<BoolNode<T>> getChildren() {
        return children;
    }

    public BoolNode<T> setChildren(List<BoolNode<T>> children) {
        this.children = children;
        return this;
    }

    public BoolNode<T> addChild(BoolNode<T> child) {
        child.setParent(this);
        this.children.add(child);
        return this;
    }

    public BoolNode<T> addChild(T childData) {
        BoolNode<T> childNode = new BoolNode<T>()
                .setData(childData)
                .setParent(this)
                .setBoolCondition(BoolCondition.MUST);
        this.children.add(childNode);
        return this;
    }

    public BoolCondition getBoolCondition() {
        return boolCondition;
    }

    public BoolNode<T> setBoolCondition(BoolCondition boolCondition) {
        this.boolCondition = boolCondition;
        return this;
    }

}