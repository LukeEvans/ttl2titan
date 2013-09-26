package com.reactor.mapred;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.reactor.graph.Gremlin;
import com.reactor.mapred.config.ImportCounters;
import com.reactor.mapred.config.RDFJobOptions;
import com.reactor.rdf.Triple;
import com.tinkerpop.blueprints.Vertex;

public class EdgePropMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	private Gremlin gremlin;
	private long count;
	
	@Override
	protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
		System.out.println("Setting up Edge/Property mapper... ");
		String hostList = context.getConfiguration().get(RDFJobOptions.HOST_LIST_KEY, RDFJobOptions.DEFAULT_CASSANDRA_HOST_LIST);
		boolean batchLoading = true;
		gremlin = new Gremlin(hostList, batchLoading);
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
				context.getCounter(ImportCounters.EDGEPROP_FAILED_TRIPLE_BUILD).increment(1l);
				return;
			}

			if (triple != null && triple.determineValid()) {
				
				run(triple);
				count++;
				
				if (count % 10000 == 0) {
					System.out.println("Checkpoint: " + count);
				}
				
				if (count % 1000 == 0) {
					gremlin.commit();
				}
				
				context.getCounter(ImportCounters.EDGEPROP_MAP_SUCCESSFUL_TRANSACTIONS).increment(1l);
			}

		} catch (Exception e) {
			context.getCounter(ImportCounters.EDGEPROP_MAP_FAILED_TRANSACTIONS).increment(1l);
			e.printStackTrace();
			gremlin.rollback();
			System.out.println("\n------------------");
			System.out.println(e.getMessage());
			System.out.println(e.getLocalizedMessage());
			System.out.println("------------------\n");
			
			
			// Ignore for now
			//throw new IOException(e.getMessage(), e);
		}
	}

	private void run(Triple triple) {
		try {
			Vertex v1 = gremlin.getVertex(triple.subject);
			
			if (!triple.property) {
				Vertex v2 = gremlin.getVertex(triple.objectString());
				gremlin.addEdge(triple.predicate, v1, v2);
			}
			
			else {
				gremlin.addProperty(v1, triple.predicate, triple.object);
			}
			
		} catch (Exception e) {
			
		}
	}
	
	@Override
	protected void cleanup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
		try { 
			System.out.println("Cleaning up edge/property mapper... ");
			gremlin.commit();
			System.out.println("Cleaned up and commited Edge/Prop mapper... ");
		} catch (Exception e) {
			System.out.println("Failed cleanup of Edge/Prop mapper... ");
			e.printStackTrace();
			gremlin.rollback();
			context.getCounter(ImportCounters.EDGEPROP_REDUCE_FAILED_TRANSACTIONS).increment(1l);
		}

		gremlin.shutdown();
	}
}
