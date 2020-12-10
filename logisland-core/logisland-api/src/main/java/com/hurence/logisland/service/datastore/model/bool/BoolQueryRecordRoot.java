package com.hurence.logisland.service.datastore.model.bool;

import java.util.List;

public class BoolQueryRecordRoot implements BoolQueryRecord {
    BoolNode<BoolQueryRecord> rootNode;

    public BoolQueryRecordRoot() {
        rootNode = new BoolNode<>();
    }

    public List<BoolNode<BoolQueryRecord>> getChildren() {
        return rootNode.getChildren();
    }

    public BoolQueryRecordRoot addBoolQuery(BoolQueryRecord query, BoolCondition condition) {
        BoolNode<BoolQueryRecord> childNode = createBoolNode(query)
                .setBoolCondition(condition);
        rootNode.addChild(childNode);
        return this;
    }

    public BoolQueryRecordRoot addMust(BoolQueryRecord query) {
        return addBoolQuery(query, BoolCondition.MUST);
    }

    public BoolQueryRecordRoot addShould(BoolQueryRecord query) {
        return addBoolQuery(query, BoolCondition.SHOULD);
    }

    public BoolQueryRecordRoot addMustNot(BoolQueryRecord query) {
        return addBoolQuery(query, BoolCondition.MUSTNOT);
    }

    private BoolNode<BoolQueryRecord> createBoolNode(BoolQueryRecord query) {
        return new BoolNode<BoolQueryRecord>()
                .setData(query)
                .setParent(rootNode);
    }
}
