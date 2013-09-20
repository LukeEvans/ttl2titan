package com.reactor.mapred.config;

public enum ImportCounters {

    VERTEX_MAP_FAILED_TRANSACTIONS,				   // Total number of transactions failed
    VERTEX_MAP_SUCCESSFUL_TRANSACTIONS,               // Total of successful transactions
    
    VERTEX_REDUCE_FAILED_TRANSACTIONS,				   // Total number of transactions failed
    VERTEX_REDUCE_SUCCESSFUL_TRANSACTIONS,               // Total of successful transactions
    
    EDGEPROP_MAP_FAILED_TRANSACTIONS,
    EDGEPROP_MAP_SUCCESSFUL_TRANSACTIONS,
    
    EDGEPROP_REDUCE_FAILED_TRANSACTIONS,
    EDGEPROP_REDUCE_SUCCESSFUL_TRANSACTIONS,
    
    VERTICES_ADDED,
    PROPERTIES_ADDED,
    EDGES_ADDED,
    
    VERTEX_FAILED_TRIPLE_BUILD,
    EDGEPROP_FAILED_TRIPLE_BUILD
    
}
