package com.reactor.rdf;

import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Triple {

	public String subject;
	public String predicate;
	public Object object;
	public boolean property;
	public boolean invalid;

	//================================================================================
	// Constructors
	//================================================================================
	public Triple() {
		// Default
	}

	public Triple(String line) {

		// Guard
		if (line == null || line.length() == 0) {
			return;
		}

		ArrayList<String> parts = parseLine(line);

		if (parts.size() != 3 || !line.endsWith(".")) {
			invalid = true;
			return;
		}

		// Build object
		buildSubject(parts.get(0));
		buildPredicate(parts.get(1));
		buildObject(parts.get(2));

		// Escape string
		escapeStrings();
	}

	//================================================================================
	// Get information from line
	//================================================================================
	public ArrayList<String> parseLine(String line) {
		ArrayList<String> allMatches = new ArrayList<String>();
		Matcher m = Pattern.compile("(\\S*<.*?>)|(\".*?\"@..)|(\".*?\")").matcher(line);
		while (m.find()) {
			String item = m.group();
			item = item.replaceAll("\"","");
			allMatches.add(item);
		}

		return allMatches;
	}

	//================================================================================
	// Escape Strings
	//================================================================================
	public void escapeStrings() {
		subject = escape(subject);
		predicate = escape(predicate);

		if (object instanceof String) {
			object = escape((String) object);
		}
	}

	//================================================================================
	// Escape single string
	//================================================================================
	public String escape(String s) {

		try {
			if (s == null || s.length() == 0) {
				return new String();
			}

			String escaped = s.replaceAll("\\$", "\\\\\\$");
			escaped = escaped.replaceAll("\\^", "\\\\\\^");
			
			return escaped;
			
		} catch (Exception e) {
			e.printStackTrace();
			return s;
		}
	}
	//================================================================================
	// Build subject
	//================================================================================
	public void buildSubject(String s) {
		String localName = grabLocalName(s);
		subject = localName;
	}

	//================================================================================
	// Build Predicate
	//================================================================================
	public void buildPredicate(String p) {
		String localPred = grabLocalName(p);
		predicate = localPred;
	}

	//================================================================================
	// Build Object
	//================================================================================
	public void buildObject(String o) {
		Object obj = grabLiteral(o);
		object = obj;
	}

	//================================================================================
	// Grab local name
	//================================================================================
	public String grabLocalName(String uri) {

		uri = uri.replaceAll("<", "").replaceAll(">", "");

		// Guard
		if (uri == null || uri.length() == 0) {
			return null;
		}

		String local = uri.replaceAll("http://rdf.freebase.com/ns/", "ns:");
		local = local.replaceAll("http://rdf.freebase.com/key/", "key:");
		local = local.replaceAll("http://www.w3.org/2002/07/owl#", "owl:");
		local = local.replaceAll("http://www.w3.org/2000/01/rdf-schema#", "rdfs:");

		return local;
	}

	//================================================================================
	// Grab literal
	//================================================================================
	public Object grabLiteral(String uri) {

		// Guard
		if (uri == null || uri.length() == 0) {
			return null;
		}

		uri = uri.replaceAll("<", "").replaceAll(">", "");
		String localName = grabLocalName(uri);

		if (localName == null || localName.length() == 0) {
			localName = uri;
		}

		// Find out if we have a property
		predicateProperty();

		String[] parts = uri.split("\\^\\^");

		if (parts == null || parts.length != 2) {

			if (uri.charAt(uri.length() - 3) == '@') {
				if (uri.endsWith("@en")) {
					property = true;
				}

				else {
					invalid = true;
				}
			}


			return new String(localName);
		}

		// Set literal value to true
		property = true;

		String value = parts[0].replaceAll("\"", "");
		String type = parts[1].toLowerCase();

		// Double
		if (type.endsWith("#double")) {
			return Double.parseDouble(value);
		}

		// Float
		if (type.endsWith("#float")) {
			return Float.parseFloat(value);
		}

		// String 
		if (type.endsWith("#string")) {
			return new String(value);
		}

		// Integer
		if (type.endsWith("#int") || type.endsWith("#integer")) {
			return Integer.parseInt(value);
		}

		else {
			// Unhandled literal
		}

		// Otherwise, just return the local name
		return new String(value);
	}

	//================================================================================
	// Determine if predicate should called a property
	//================================================================================
	public boolean predicateProperty() {
		property = true;

		if (predicate.equalsIgnoreCase("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")) {
			predicate = "rtype";
			return true;
		}

		if (predicate.equalsIgnoreCase("ns:type.object.type")) {
			predicate = "fbtype";
			return true;
		}
		
		if (predicate.equalsIgnoreCase("http://www.w3.org/2000/01/rdf-schema#comment")) {
			predicate = "rcomment";
			return true;
		}

		if (predicate.equalsIgnoreCase("ns:common.topic.description")) {
			predicate = "rdescription";
			return true;
		}

		if (predicate.equalsIgnoreCase("ns:common.topic.alias")) {
			predicate = "ralias";
			return true;
		}

		if (predicate.equalsIgnoreCase("ns:base.schemastaging.context_name.nickname")) {
			predicate = "rnickname";
			return true;
		}
		
		if (predicate.equalsIgnoreCase("ns:common.notable_for.display_name")) {
			predicate = "rdisplay_name";
		}

		if (predicate.equalsIgnoreCase("ns:type.object.name")) {
			predicate = "rname";
			return true;
		}

		if (predicate.equalsIgnoreCase("rdfs:label")) {
			predicate = "rlabel";
			return true;
		}

		property = false;
		return false;
	}

	//================================================================================
	// House Keeping
	//================================================================================
	public String toString() {
		String s = "";

		if (property) {
			s = subject + ": [" + predicate + ": " + object + "]";
		}

		else {
			s = subject + " -- " + predicate + " -- " + object;
		}

		return s;
	}

	public String objectString() {
		if (object instanceof String) {
			return object.toString();
		}
		
		return null;
	}
	
	public boolean determineValid() {

		if (invalid) {
			return false;
		}

		if (subject == null || predicate == null || object == null) {
			return false;
		}

		// Only take english keys
		if (predicate.startsWith("key:") && !predicate.contains("en")) {
			return false;
		}

		return true;
	}
}