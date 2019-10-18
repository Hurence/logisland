package com.hurence.logisland.processor.state;

/**
 * A Scope represents how a NiFi component's state is to be stored and retrieved when running in a cluster.
 */
public enum Scope {

    /**
     * State is to be treated as "global" across the cluster. I.e., the same component on all nodes will
     * have access to the same state.
     */
    CLUSTER,

    /**
     * State is to be treated local to the node. I.e., the same component will have different state on each
     * node in the cluster.
     */
    LOCAL;

    @Override
    public String toString() {
        return name();
    }
}
