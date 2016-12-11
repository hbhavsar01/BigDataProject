package com.mycompany.app;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.text.StrSubstitutor;

import com.google.common.collect.Lists;

/**
 * The class for Hive Script Parsing
 *
 */
public class HiveScriptParse {

	private String scriptFile;
	private Map<String, String> params;
	private List<String> excludes;
	private StrSubstitutor substitutor;

	/**
	 * Parameterized Constructor for HiveScriptParse class
	 * 
	 * @param scriptFile
	 * @param params
	 * @param excludes
	 */
	public HiveScriptParse(String scriptFile, Map<String, String> params,
			List<String> excludes) {
		this.scriptFile = scriptFile;
		this.params = params;
		this.excludes = excludes;
		if (this.params != null) {
			substitutor = new StrSubstitutor(this.params);
		}
	}

	/**
	 * The method to parse Hive queries
	 * 
	 * @return statement commands
	 */
	public List<String> getStatements() {
		List<String> commands = Lists.newArrayList();
		BufferedReader in = null;
		try {
			in = new BufferedReader(new FileReader(scriptFile));
			String line;
			StringBuilder command = new StringBuilder();
			while ((line = in.readLine()) != null) {
				if (skippableLine(line) || excludeLine(line)) {
					continue;
				}
				if (line.endsWith(";")) {
					command.append(replaceParams(line.replace(";", "")));
					commands.add(command.toString());
					command = new StringBuilder();
				} else {
					command.append(replaceParams(line));
					// need to make sure there is a space between lines
					command.append(" ");
				}
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			if (in != null) {
				try {
					in.close();
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}
		}
		return commands;
	}

	/**
	 * The method for excluding lines
	 * 
	 * @param line
	 * @return boolean
	 */
	private boolean excludeLine(String line) {
		if (excludes == null) {
			return false;
		}
		for (String excludeLine : excludes) {
			if (line.equals(excludeLine)) {
				return true;
			}
		}
		return false;
	}

	/**
	 * The method for skipping line staring with '--'
	 * 
	 * @param line
	 * @return
	 */
	private boolean skippableLine(String line) {
		if (line.isEmpty() || line.startsWith("--")) {
			return true;
		}
		return false;
	}

	/**
	 * The method for replacing parameters
	 * 
	 * @param line
	 * @return
	 */
	private String replaceParams(String line) {
		if (substitutor == null) {
			return line;
		}
		return substitutor.replace(line);
	}

}
