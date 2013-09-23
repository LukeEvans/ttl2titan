package com.reactor.mapred;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.reactor.graph.Gremlin;
import com.reactor.mapred.config.ImportCounters;
import com.reactor.mapred.config.RDFJobOptions;

public class VertexMapper extends Mapper<LongWritable, Text, Text, Text> {
	private Gremlin gremlin;
	private long count;
	
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

			run(line);
			count++;
			
			if (count % 10000 == 0) {
				System.out.println("Checkpoint: " + count);
			}
			
			if (count % 1000 == 0) {
				gremlin.commit();
			}
			
			context.getCounter(ImportCounters.VERTEX_MAP_SUCCESSFUL_TRANSACTIONS).increment(1l);
			
		} catch (Exception e) {
			context.getCounter(ImportCounters.VERTEX_MAP_FAILED_TRANSACTIONS).increment(1l);
			e.printStackTrace();
			gremlin.rollback();
			
			throw new IOException(e.getMessage(), e);
		}
	}

	private void run(String mid) {
		try {
			gremlin.addVertex(mid);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Override
	protected void cleanup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
		try { 
			System.out.println("Cleaning up vertex mapper... ");
			gremlin.commit();
			System.out.println("Cleaned up and commited vertex mapper... ");
		} catch (Exception e) {
			System.out.println("Failed cleanup of Vertex mapper... ");
			e.printStackTrace();
			gremlin.rollback();
			context.getCounter(ImportCounters.VERTEX_REDUCE_FAILED_TRANSACTIONS).increment(1l);
		}

		gremlin.shutdown();
	}
}
