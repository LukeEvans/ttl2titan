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
	
    @Override
    protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
    	System.out.println("Setting up edge/property mapper... ");
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

			Triple triple = new Triple(line);

			if (triple != null && triple.determineValid()) {
				run(triple);
			}
			
		} catch (Exception e) {
			context.getCounter(ImportCounters.EDGEPROP_FAILED_TRANSACTIONS).increment(1l);
			gremlin.rollback();
			
			throw new IOException(e.getMessage(), e);
		}
	}
	
    // Run the gremlin queries to add triple
	protected void run(Triple triple) {

		try {
			
			if (!triple.property) {
				Vertex v1 = gremlin.getIDVertex(triple.subject);
				Vertex v2 = gremlin.getIDVertex(triple.objectString());
				gremlin.addEdge(triple.predicate, v1, v2);
			}
			
			else {
				Vertex v1 = gremlin.getIDVertex(triple.subject);
				gremlin.addProperty(v1, triple.predicate, triple.object);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
    @Override
    protected void cleanup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
		try { 
			gremlin.commit();
		} catch (Exception e) {
			e.printStackTrace();
			gremlin.rollback();
			context.getCounter(ImportCounters.EDGEPROP_FAILED_TRANSACTIONS).increment(1l);
		}

		gremlin.shutdown();
    }
}
