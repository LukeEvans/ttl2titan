package com.reactor.mapred.config;

public enum ImportCounters {

    VERTEX_FAILED_TRANSACTIONS,				   // Total number of transactions failed
    VERTEX_SUCCESSFUL_TRANSACTIONS,               // Total of successful transactions
    
    EDGEPROP_FAILED_TRANSACTIONS,
    EDGEPROP_SUCCESSFUL_TRANSACTIONS,
    
    VERTICES_ADDED,
    PROPERTIES_ADDED,
    EDGES_ADDED,
    
    VERTEX_FAILED_TRIPLE_BUILD,
    EDGEPROP_FAILED_TRIPLE_BUILD
    
}
