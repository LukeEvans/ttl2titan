package com.reactor.graph;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;

import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.core.TypeGroup;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

public class Gremlin {

	TitanGraph graph;

	@SuppressWarnings("serial")
	public static final Map<String, String> LOWERCASE_PROPERTIES = new HashMap<String, String>() {{
		put("ralias", "lower_ralias");
		put("rlabel", "lower_rlabel");
		put("rname", "lower_rname");
		put("rnickname", "lower_rnickname");
	}};

	//================================================================================
	// Constructor
	//================================================================================
	public Gremlin(String hostList, boolean batchLoading) {
		Configuration conf = new BaseConfiguration();
		conf.setProperty("storage.backend","cassandra");
		conf.setProperty("storage.hostname",hostList);
		conf.setProperty("ids.block-size", 100000);
		conf.setProperty("storage.batch-loading", batchLoading);

		graph = TitanFactory.open(conf);
	}

	//================================================================================
	// Define indices for Freebase data
	//================================================================================
	public void runFreebaseIndexAdds() {

		try {
			graph.makeType().name("mid").dataType(String.class).indexed(Vertex.class).unique(Direction.BOTH).makePropertyKey();

			TypeGroup rsearch = TypeGroup.of(2, "rsearch");
			graph.makeType().name("ralias").dataType(String.class).indexed(Vertex.class).group(rsearch).makePropertyKey();
			graph.makeType().name("lower_ralias").dataType(String.class).indexed(Vertex.class).group(rsearch).makePropertyKey();
			graph.makeType().name("rnickname").dataType(String.class).indexed(Vertex.class).group(rsearch).makePropertyKey();
			graph.makeType().name("lower_rnickname").dataType(String.class).indexed(Vertex.class).group(rsearch).makePropertyKey();
			graph.makeType().name("rlabel").dataType(String.class).indexed(Vertex.class).unique(Direction.OUT).group(rsearch).makePropertyKey();
			graph.makeType().name("lower_rlabel").dataType(String.class).indexed(Vertex.class).unique(Direction.OUT).group(rsearch).makePropertyKey();
			graph.makeType().name("rname").dataType(String.class).indexed(Vertex.class).unique(Direction.OUT).group(rsearch).makePropertyKey();
			graph.makeType().name("lower_rname").dataType(String.class).indexed(Vertex.class).unique(Direction.OUT).group(rsearch).makePropertyKey();
			graph.makeType().name("rtype").dataType(String.class).indexed(Vertex.class).makePropertyKey();
			graph.makeType().name("fbtype").dataType(String.class).indexed(Vertex.class).makePropertyKey();
			graph.makeType().name("rdescription").dataType(String.class).indexed(Vertex.class).unique(Direction.OUT).makePropertyKey();
			graph.commit();

			System.out.println("Added Indices");

		} catch (Exception e) {
			// Indices already set up
			System.out.println("Types are already set up.");
		}
	}

	//================================================================================
	// Add Property Key
	//================================================================================
	public boolean addPropertyKey(String name) {
		try {
			graph.createKeyIndex(name,Vertex.class);
			return true;
		} catch (Exception e) {
			//e.printStackTrace();
			// Ignore
			return false;
		}
	}
	
	//================================================================================
	// Add Edge Label
	//================================================================================
	public boolean addEdgeLabel(String name) {
		try {
			graph.makeType().name(name).makeEdgeLabel();
			return true;
		} catch (Exception e) {
			//e.printStackTrace();
			// Ignore
			return false;
		}
	}
	
	//================================================================================
	// Add vertex
	//================================================================================
	public void addVertex(String mid) {
		try {
			Vertex v = getVertex(mid);

			if (v == null) {
				v = graph.addVertex(null);
				v.setProperty("mid", mid);
			}
		}
		catch (Exception e) {
			// Ignore
		}
	}

	//================================================================================
	// Get Vertex
	//================================================================================
	public Vertex getVertex(String mid) {

		try {
			Vertex v = graph.getVertices("mid", mid).iterator().next();
			if (v == null) {
				return null;
			}

			return v;

		} catch (Exception e) {
			return null;
		}
	}

	//================================================================================
	// Add Edge
	//================================================================================
	public Edge addEdge(String edgeLabel, Vertex v1, Vertex v2) {

		try {
			if (v1 == null || v2 == null) {
				return null;
			}

			Edge e = v1.addEdge(edgeLabel, v2);
			return e;

		} catch (Exception e) {
			return null;
		}
	}

	//================================================================================
	// Set Property
	//================================================================================
	public void addProperty(Vertex v, String key, Object value) {

		try {
			if (v == null || key == null || key.length() == 0 || value == null) {
				return;
			}

			TitanVertex tv = (TitanVertex) v;
			tv.addProperty(key, value);

			if (LOWERCASE_PROPERTIES.containsKey(key)) {
				String newKey = LOWERCASE_PROPERTIES.get(key);

				if (value instanceof String) {
					tv.addProperty(newKey, value.toString().toLowerCase());
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
			// Ignore
		}
	}

	//================================================================================
	// HouseKeeping
	//================================================================================
	public boolean properlyConnected() {
		return graph != null;
	}

	//================================================================================
	// Commit Graph
	//================================================================================
	public void commit() {
		graph.commit();
	}

	//================================================================================
	// Graph Rollback
	//================================================================================
	public void rollback() {
		try {
			graph.rollback();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	//================================================================================
	// Graph shutdown
	//================================================================================
	public void shutdown() {
		try {
			graph.shutdown();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}


}
