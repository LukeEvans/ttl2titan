package com.reactor.mapred;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.reactor.graph.Gremlin;
import com.reactor.mapred.config.ImportCounters;
import com.reactor.mapred.config.RDFJobOptions;
import com.reactor.rdf.Triple;

public class VertexMapper extends Mapper<LongWritable, Text, Text, Text> {
	private Gremlin gremlin;

	@Override
	protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
		System.out.println("Setting up vertex mapper... ");
		String hostList = context.getConfiguration().get(RDFJobOptions.HOST_LIST_KEY, RDFJobOptions.DEFAULT_CASSANDRA_HOST_LIST);
		gremlin = new Gremlin(hostList);
	}

	@Override
	protected void map(LongWritable lineNum, Text value, Context context) throws IOException, InterruptedException {

		try {
			String line = value.toString();

			if (line == null || line.length() == 0) {
				return;
			}

			Triple triple = null;
			
			try {
				triple = new Triple(line);
			} catch (Exception e) {
				context.getCounter(ImportCounters.VERTEX_FAILED_TRIPLE_BUILD).increment(1l);
				return;
			}

			if (triple != null && triple.determineValid()) {
				run(triple);

				Text key = new Text(triple.subject);
				Text val = new Text(line);
				context.write(key, val);

				context.getCounter(ImportCounters.VERTEX_SUCCESSFUL_TRANSACTIONS).increment(1l);
			}

		} catch (Exception e) {
			context.getCounter(ImportCounters.VERTEX_FAILED_TRANSACTIONS).increment(1l);
			gremlin.rollback();
			e.printStackTrace();

			throw new IOException(e.getMessage(), e);
		}
	}

	// Run the gremlin queries to add triple
	protected void run(Triple triple) {

		try {
			gremlin.addIDVertex(triple.subject);

			if (!triple.property) {
				gremlin.addIDVertex(triple.objectString());
			}

		} catch (Exception e) {
			// Ignore
		}
	}

	@Override
	protected void cleanup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {

		try { 
			gremlin.commit();
		} catch (Exception e) {
			e.printStackTrace();
			gremlin.rollback();
			context.getCounter(ImportCounters.VERTEX_FAILED_TRANSACTIONS).increment(1l);
		}

		gremlin.shutdown();
	}
}
