package com.reactor.mapred;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.reactor.mapred.config.ImportCounters;
import com.reactor.rdf.Triple;

public class EdgePropMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	@Override
	protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
		System.out.println("Setting up Edge/Property mapper... ");
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
				
				Text subject = new Text(triple.subject);
				Text val = new Text(line);
				context.write(subject, val);

				context.getCounter(ImportCounters.EDGEPROP_MAP_SUCCESSFUL_TRANSACTIONS).increment(1l);
			}

		} catch (Exception e) {
			context.getCounter(ImportCounters.EDGEPROP_MAP_FAILED_TRANSACTIONS).increment(1l);
			e.printStackTrace();

			throw new IOException(e.getMessage(), e);
		}
	}

	protected void cleanup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
		System.out.println("Cleaning up EdgeProperty mapper... ");
	}
}
