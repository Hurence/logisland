/**
 * Copyright (C) 2016 Hurence (bailet.thomas@gmail.com)
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
package com.hurence.logisland.service.elasticsearch.multiGet;

import java.util.*;

/**
 * Builder for ElasticSearch MultiGet.
 * The goal if to push all of the requests as triplets (index, type, list of keys)
 * then the build() method return only consolidated queries, removing duplicates.
 *
 * Implementation is backed by a Tree structure to reduce the amount of memory required.
 */
public class MultiGetQueryRecordBuilder {

    private Tree<String> tree = new Tree<String>("/");
    private Set<String> includes = new TreeSet();
    private Set<String> excludes = new TreeSet();

    /**
     * Constructor.
     */
    public MultiGetQueryRecordBuilder() {}

    public MultiGetQueryRecordBuilder includeFields(String... includes) {
        for (String include : includes) {
            this.includes.add(include);
        }
        return this;
    }

    public MultiGetQueryRecordBuilder excludeFields(String... excludes) {
        for (String exclude : excludes) {
            this.excludes.add(exclude);
        }
        return this;
    }

    public MultiGetQueryRecordBuilder add(String indexName, String typeName, String... keys) {

        if (indexName == null || typeName == null || keys == null) {
            throw new NullPointerException();
        }

        Tree<String> t = tree.getOrCreateLeaf(indexName)
        .getOrCreateLeaf(typeName);

        for (String key : keys) {
            t.getOrCreateLeaf(key);
        }
        return this;
    }

    public List<MultiGetQueryRecord> build() throws InvalidMultiGetQueryRecordException {
        ArrayList<MultiGetQueryRecord> res = new ArrayList();

        for (Tree<String> idx : tree.getSubTrees()) {
            for (Tree<String> type : idx.getSubTrees()) {
                List<String> keys = type.getSuccessors();

                MultiGetQueryRecord rec = new MultiGetQueryRecord(idx.getHead(), type.getHead(), keys);
                res.add(rec);
            }
        }

        return res;
    }
}

/**
 * Simple tree implementation.
 */
class Tree<T> {

    private T head;
    private ArrayList<Tree<T>> leafs = new ArrayList();

    public Tree(T head) {
        this.head = head;
    }

    public Tree<T> addLeaf(T leaf) {
        Tree<T> t = new Tree<T>(leaf);
        leafs.add(t);
        return t;
    }

    public T getHead() {
        return head;
    }

    public List<T> getSuccessors() {
        List<T> successors = new ArrayList<T>();
        for (Tree<T> leaf : leafs) {
            successors.add(leaf.head);
        }
        return successors;
    }

    public List<Tree<T>> getSubTrees() {
        return leafs;
    }

    public Tree<T> getOrCreateLeaf(T id) {
        Tree newTree = null;
        for (Tree t : getSubTrees()) {

            if (t.head != null && t.head.equals(id)) {
                newTree = t;
                break;
            }
        }
        if (newTree == null) {
            newTree = addLeaf(id);
        }
        return newTree;
    }

    @Override
    public String toString() {
        return printTree(0);
    }

    private static final int indent = 2;
    private String printTree(int increment) {
        String s = "";
        String inc = "";
        for (int i = 0; i < increment; ++i) {
            inc = inc + " ";
        }
        s = inc + head;
        for (Tree<T> child : leafs) {
            s += "\n" + child.printTree(increment + indent);
        }
        return s;
    }
}