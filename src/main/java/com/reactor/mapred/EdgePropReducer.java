package com.reactor.mapred;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.reactor.graph.Gremlin;
import com.reactor.mapred.config.ImportCounters;
import com.reactor.mapred.config.RDFJobOptions;
import com.reactor.rdf.Triple;
import com.tinkerpop.blueprints.Vertex;

public class EdgePropReducer extends Reducer<Text, Text, Text, Text> {

	private Gremlin gremlin;

	@Override
	protected void setup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
		System.out.println("Setting up edge/property reducer... ");
		String hostList = context.getConfiguration().get(RDFJobOptions.HOST_LIST_KEY, RDFJobOptions.DEFAULT_CASSANDRA_HOST_LIST);
		gremlin = new Gremlin(hostList, false);
	}

	@Override
	protected void reduce(Text vertexID, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		try {

			Vertex subject = getSubjectVertex(vertexID.toString());

			for (Text v : values) {
				
				Triple triple = null;

				try {
					triple = new Triple(v.toString());
				} catch (Exception e) {
					context.getCounter(ImportCounters.EDGEPROP_FAILED_TRIPLE_BUILD).increment(1l);
					return;
				}

				run(subject, triple);

				context.getCounter(ImportCounters.EDGEPROP_REDUCE_SUCCESSFUL_TRANSACTIONS).increment(1l);
			}

		} catch (Exception e) {
			context.getCounter(ImportCounters.EDGEPROP_REDUCE_FAILED_TRANSACTIONS).increment(1l);
			gremlin.rollback();
			e.printStackTrace();

			throw new IOException(e.getMessage(), e);
		}
	}

	private void run(Vertex v1, Triple triple) {
		try {
			// Add edge
			if (!triple.property) {
				Vertex v2 = gremlin.getVertex(triple.objectString());
				gremlin.addEdge(triple.predicate, v1, v2);
			}

			// Add property
			else {
				gremlin.addProperty(v1, triple.predicate, triple.object);
			}

		} catch (Exception e) {
			// Ignore
		}
	}

	private Vertex getSubjectVertex(String mid) {
		return gremlin.getVertex(mid);
	}

	@Override
	protected void cleanup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
		try { 
			System.out.println("Cleaning up edge/property reducer... ");
			gremlin.commit();
		} catch (Exception e) {
			e.printStackTrace();
			gremlin.rollback();
			context.getCounter(ImportCounters.EDGEPROP_REDUCE_FAILED_TRANSACTIONS).increment(1l);
		}

		gremlin.shutdown();
	}
}
