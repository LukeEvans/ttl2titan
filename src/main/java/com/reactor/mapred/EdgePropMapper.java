package com.reactor.mapred;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import com.reactor.graph.Gremlin;
import com.reactor.mapred.config.RDFJobOptions;
import com.reactor.rdf.Triple;
import com.tinkerpop.blueprints.Vertex;

public class EdgePropMapper extends Mapper<LongWritable, Text, Text, Text> {
	private static final Logger LOGGER = Logger.getLogger(EdgePropMapper.class);
	private static final long BATCH_SIZE = 1000;
	private Gremlin gremlin;
	private long count;
	
    @Override
    protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
    	System.out.println("Setting up edge/property mapper... ");
    	String hostList = context.getConfiguration().get(RDFJobOptions.HOST_LIST_KEY, RDFJobOptions.DEFAULT_CASSANDRA_HOST_LIST);
    	gremlin = new Gremlin(hostList);
    	count = 0;
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
				
				count++;
				
				if (count % BATCH_SIZE == 0) {
					System.out.println(triple);
					System.out.println("Committing Batch...");
					gremlin.commit();
					System.out.println("Committed Batch... ");
				}
			}
			
		} catch (Exception e) {
			LOGGER.error("Can't parse input line: " + value.toString(), e);
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
    	System.out.println("Committing vertex graph... ");
    	gremlin.commit();
    	System.out.println("Cleaning up vertex mapper... ");
    }
}
