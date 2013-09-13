package com.reactor.mapred;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import com.reactor.mapred.config.ImportCounters;
import com.reactor.rdf.Triple;

public class ImportMapper extends Mapper<LongWritable, Text, Text, Text> {
	private static final Logger LOGGER = Logger.getLogger(ImportMapper.class);

	@Override
	protected void map(LongWritable lineNum, Text value, Context context) throws IOException, InterruptedException {

		try {
			String line = value.toString();

			if (line == null || line.length() == 0) {
				return;
			}

			Triple triple = new Triple(line);

			if (triple != null && triple.determineValid()) {
				Text k = new Text();
				k.set(triple.subject);
				
				Text v = new Text();
				v.set(line);
				
				context.write(k, v);
			}
			
		} catch (Exception e) {
			context.getCounter(ImportCounters.TRIPLES_FAILED).increment(1);
			LOGGER.error("Can't parse input line: " + value.toString(), e);
		}
	}
}
