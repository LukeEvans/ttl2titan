/*
 * Copyright (c) 2009-2011 Scale Unlimited
 * 
 * All rights reserved.
 */

package com.reactor.mapred;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import com.reactor.graph.Gremlin;
import com.reactor.mapred.config.RDFJobOptions;

public class ImportJob {

	public static final String RAW_SUBDIR_NAME_VERTEX = "vertexRaw";
	public static final String RAW_SUBDIR_NAME_EDGEPROP = "edgePropsRaw";

	private void runIndexAdder(RDFJobOptions options) {
		String hostList = options.getCassandra_hosts();
		Gremlin gremlin = new Gremlin(hostList);

		gremlin.runFreebaseIndexAdds();
	}

	private void runVertexImport(RDFJobOptions options, boolean printConfig) throws IOException, ClassNotFoundException, InterruptedException {
		// create Hadoop path instances
		Path inputPath = new Path(options.getInputFile());
		Path outputPath = new Path(options.getOuputDir());
		Path tempDirPath = new Path(outputPath, RAW_SUBDIR_NAME_VERTEX);

		// Create the job configuration
		Configuration conf = new Configuration();

		// Set the list of cassandra hosts
		conf.set(RDFJobOptions.HOST_LIST_KEY, options.getCassandra_hosts());

		// get the FileSystem instances for each path
		// this allows for the paths to live on different FileSystems (local, hdfs, s3, etc)
		FileSystem inputFS = inputPath.getFileSystem(conf);
		FileSystem outputFS = tempDirPath.getFileSystem(conf);

		// if input path does not exists, fail
		if (!inputFS.exists(inputPath)) {
			System.out.println("Input file does not exist: " + inputPath);
			System.exit(-1);
		}

		// if output path exists, delete recursively
		if (outputFS.exists(tempDirPath)) {
			outputFS.delete(tempDirPath, true);
		}

		// Create the actual job and run it.
		Job job = new Job(conf, "RDF import job");

		// finds the enclosing jar path
		job.setJarByClass(ImportJob.class);

		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.setInputPaths(job, inputPath);

		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		TextOutputFormat.setOutputPath(job, tempDirPath);

		// our mapper class
		job.setMapperClass(VertexMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		// Set reducer number to zero
		job.setNumReduceTasks(0);

		if (printConfig) {
			System.out.println("starting RDF import job using:");
			System.out.println(" jobtracker      = " + job.getConfiguration().get("mapred.job.tracker"));
			System.out.println(" inputPath       = " + inputPath.makeQualified(inputFS));
			System.out.println(" outputPath      = " + tempDirPath.makeQualified(outputFS));
			System.out.println(" mapper class    = " + job.getMapperClass());
			System.out.println(" reducer class   = " + job.getReducerClass());
			System.out.println(" cassandra hosts = " + job.getConfiguration().get(RDFJobOptions.HOST_LIST_KEY));
			System.out.println("");
		}

		// run job and block until job is done
		job.waitForCompletion(false);
		
		// Print output counters
		Counters counters = job.getCounters();
		System.out.println(counters.toString());
	}


	private void runEdgePropertyImport(RDFJobOptions options, boolean printConfig) throws IOException, ClassNotFoundException, InterruptedException {
		// create Hadoop path instances
		Path inputPath = new Path(options.getInputFile());
		Path outputPath = new Path(options.getOuputDir());
		Path tempDirPath = new Path(outputPath, RAW_SUBDIR_NAME_EDGEPROP);

		// Create the job configuration
		Configuration conf = new Configuration();

		// Set the list of cassandra hosts
		conf.set(RDFJobOptions.HOST_LIST_KEY, options.getCassandra_hosts());

		// get the FileSystem instances for each path
		// this allows for the paths to live on different FileSystems (local, hdfs, s3, etc)
		FileSystem inputFS = inputPath.getFileSystem(conf);
		FileSystem outputFS = tempDirPath.getFileSystem(conf);

		// if input path does not exists, fail
		if (!inputFS.exists(inputPath)) {
			System.out.println("Input file does not exist: " + inputPath);
			System.exit(-1);
		}

		// if output path exists, delete recursively
		if (outputFS.exists(tempDirPath)) {
			outputFS.delete(tempDirPath, true);
		}

		// Create the actual job and run it.
		Job job = new Job(conf, "RDF import job");

		// finds the enclosing jar path
		job.setJarByClass(ImportJob.class);

		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.setInputPaths(job, inputPath);

		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		TextOutputFormat.setOutputPath(job, tempDirPath);

		// our mapper class
		job.setMapperClass(EdgePropMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		// Set reducer number to zero
		job.setNumReduceTasks(0);

		if (printConfig) {
			System.out.println("starting RDF import job using:");
			System.out.println(" jobtracker      = " + job.getConfiguration().get("mapred.job.tracker"));
			System.out.println(" inputPath       = " + inputPath.makeQualified(inputFS));
			System.out.println(" outputPath      = " + tempDirPath.makeQualified(outputFS));
			System.out.println(" mapper class    = " + job.getMapperClass());
			System.out.println(" reducer class   = " + job.getReducerClass());
			System.out.println(" cassandra hosts = " + job.getConfiguration().get(RDFJobOptions.HOST_LIST_KEY));
			System.out.println("");
		}

		// run job and block until job is done
		job.waitForCompletion(false);
		
		// Print output counters
		Counters counters = job.getCounters();
		System.out.println(counters.toString());
	}
	
	public void run(RDFJobOptions options, boolean printConfig) throws IOException {
		// Avoid having Hadoop wind up trying to use the Jaxen parser, which will
		// trigger exceptions that look like "Failed to set setXIncludeAware(true) for parser blah"
		System.setProperty("javax.xml.parsers.DocumentBuilderFactory", "com.sun.org.apache.xerces.internal.jaxp.DocumentBuilderFactoryImpl");

		try {
			System.out.println("Starting Vertex Import...");
			runVertexImport(options, printConfig);
			System.out.println("Finished Vertex Import...");
			
		} catch (Exception e) {
			System.err.println("Exception running job to import RDF data: " + e.getMessage());
			e.printStackTrace(System.err);
			System.exit(-1);
		}
		
		try {
			System.out.println("Starting EdgePropery Import...");
			runEdgePropertyImport(options, printConfig);
			System.out.println("Finished EdgePropery Import...");
			
		} catch (Exception e) {
			System.err.println("Exception running job to import RDF data: " + e.getMessage());
			e.printStackTrace(System.err);
			System.exit(-1);
		}
	}

	private static void printUsageAndExit(CmdLineParser parser) {
		parser.printUsage(System.err);
		System.exit(-1);
	}

	@SuppressWarnings("unchecked")
	private static void setLoggingLevel(Level level) {
		List<Logger> loggers = Collections.<Logger>list(LogManager.getCurrentLoggers());
		loggers.add(LogManager.getRootLogger());
		for ( Logger logger : loggers ) {
		    logger.setLevel(level);
		}
	}
	
	public static void main(String[] args) throws IOException {
		// Avoid having Hadoop wind up trying to use the Jaxen parser, which will
		// trigger exceptions that look like "Failed to set setXIncludeAware(true) for parser blah"
		System.setProperty("javax.xml.parsers.DocumentBuilderFactory", "com.sun.org.apache.xerces.internal.jaxp.DocumentBuilderFactoryImpl");

		RDFJobOptions options = new RDFJobOptions();
		CmdLineParser parser = new CmdLineParser(options);

		try {
			parser.parseArgument(args);
		} catch (CmdLineException e) {
			System.err.println(e.getMessage());
			printUsageAndExit(parser);
		}


		// Set logging level
		setLoggingLevel(Level.ERROR);
		
		ImportJob  job = new ImportJob();

		// Run Freebase Index adds
		job.runIndexAdder(options);
		
		// Run the job
		job.run(options, true);
	}

}
