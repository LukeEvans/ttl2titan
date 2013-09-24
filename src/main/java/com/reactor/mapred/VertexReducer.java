package com.reactor.mapred;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.reactor.graph.Gremlin;
import com.reactor.mapred.config.ImportCounters;
import com.reactor.mapred.config.RDFJobOptions;

public class VertexReducer extends Reducer<Text, Text, Text, Text> {
	private Gremlin gremlin;
	
	@Override
	protected void setup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
		System.out.println("Setting up vertex reducer... ");
		String hostList = context.getConfiguration().get(RDFJobOptions.HOST_LIST_KEY, RDFJobOptions.DEFAULT_CASSANDRA_HOST_LIST);
		gremlin = new Gremlin(hostList, false);
	}
	
	@Override
	protected void reduce(Text vertexID, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		try {
			
			if (vertexID == null || vertexID.toString() == null) {
				return;
			}
			
			gremlin.addVertex(vertexID.toString());
			context.getCounter(ImportCounters.VERTEX_REDUCE_SUCCESSFUL_TRANSACTIONS).increment(1l);
			
		} catch (Exception e) {
			context.getCounter(ImportCounters.VERTEX_REDUCE_FAILED_TRANSACTIONS).increment(1l);
			gremlin.rollback();
			e.printStackTrace();

			throw new IOException(e.getMessage(), e);
		}
	}
	
	@Override
	protected void cleanup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
		try { 
			System.out.println("Cleaning up vertex reducer... ");
			gremlin.commit();
		} catch (Exception e) {
			e.printStackTrace();
			gremlin.rollback();
			context.getCounter(ImportCounters.VERTEX_REDUCE_FAILED_TRANSACTIONS).increment(1l);
		}

		gremlin.shutdown();
	}
}
