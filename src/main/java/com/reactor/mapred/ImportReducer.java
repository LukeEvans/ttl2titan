
package com.reactor.mapred;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.reactor.graph.Gremlin;
import com.reactor.mapred.config.ImportCounters;
import com.reactor.mapred.config.RDFJobOptions;
import com.reactor.rdf.Triple;
import com.tinkerpop.blueprints.Vertex;

public class ImportReducer extends Reducer<Text, Text, Text, LongWritable> {

    public static final int BATCH_SIZE = 1000;
    private transient Gremlin gremlin;
    
    @Override
    protected void setup(Reducer<Text, Text, Text, LongWritable>.Context context) throws IOException, InterruptedException {
    	String hostList = context.getConfiguration().get(RDFJobOptions.HOST_LIST_KEY, RDFJobOptions.DEFAULT_CASSANDRA_HOST_LIST);
    	gremlin = new Gremlin(hostList);
    }
    
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        long counter = 0;
        Vertex v = getSubjectVertex(key.toString());
        
        for (Text value : values) {
        	if (value == null) {
        		continue;
        	}
        	
            String line = value.toString();
            
            Triple triple = new Triple(line);
            
            context.getCounter(ImportCounters.TRIPLES_TOTAL).increment(1);
            
            // We have a valid triple
            if (triple != null && triple.determineValid()) {
            	context.getCounter(ImportCounters.TRIPLES_VALID).increment(1);
            	
				run(triple, v);
				
				counter++;
				
				if (counter % 1000 == 0) {
					gremlin.commit();
				}
            }
            
            // Invalid Triple
            else {
            	context.getCounter(ImportCounters.TRIPLES_SKIPPED).increment(1);
            }
        }
        
        // Commit
        gremlin.commit();

    }
    
    // Run the gremlin queries to add triple
	protected void run(Triple triple, Vertex v1) {

		try {
			
			if (!triple.property) {
				Vertex v2 = gremlin.addVertex(triple.objectString());
				gremlin.addEdge(triple.predicate, v1, v2);
			}

			else {
				gremlin.addProperty(v1, triple.predicate, triple.object);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	// Get the subject vertex once to save time
	protected Vertex getSubjectVertex(String mid) {
		try {
			return gremlin.addVertex(mid);
			
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}
}
