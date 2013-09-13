/*
 * Copyright (c) 2009-2011 Scale Unlimited
 * 
 * All rights reserved.
 */

package com.reactor.mapred.config;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.kohsuke.args4j.Option;

public class RDFJobOptions {
    // With 1 reducer, we'll get one result file with sorted ngrams
    // Using more than 1 reducer means you'll get multiple output files,
    public static final int DEFAULT_REDUCERS = 1;
    
    // Key to get the cassandra hosts out of the configuration
    public static final String HOST_LIST_KEY = "hostListKey";
    
    // What hosts should we store our triples to
    // By default we assume we have Titan/Cassandra running locally
    public static final String DEFAULT_CASSANDRA_HOST_LIST = "127.0.0.1";
    
    private String inputFile;
    private String outputDir;
    
    private int numReducers = DEFAULT_REDUCERS;
    private String cassandra_hosts = DEFAULT_CASSANDRA_HOST_LIST;
    
    @Option(name = "-inputfile", usage = "path to file to process", required = true)
    public void setInputFile(String inputFile) {
        this.inputFile = inputFile;
    }

    @Option(name = "-outputdir", usage = "path to directory for results", required = true)
    public void setOutputDir(String outputDir) {
        this.outputDir = outputDir;
    }

    @Option(name = "-numreducers", usage = "number of reducers", required = false)
    public void setNumReducers(int numReducers) {
        this.numReducers = numReducers;
    }

    @Option(name = "-casshosts", usage = "comma separated list of cassandra hosts to import data to", required = false)
    public void setCassandra_hosts(String hosts) {
        this.cassandra_hosts = hosts;
    }
    
    public String getInputFile() {
        return inputFile;
    }

    public String getOuputDir() {
        return outputDir;
    }

    public int getNumReducers() {
        return numReducers;
    }
    
    public String getCassandra_hosts() {
        return cassandra_hosts;
    }

    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this, ToStringStyle.MULTI_LINE_STYLE);
    }

}
