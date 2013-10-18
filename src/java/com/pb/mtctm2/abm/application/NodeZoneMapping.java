package com.pb.mtctm2.abm.application;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class NodeZoneMapping {
	public static final String HIGHWAY_NODE_MAPPING_FILE_PROPERTY = "highway.skim.node.to.taz.file";
	public static final String HIGHWAY_NODE_MAPPING_FILE_PERIOD_TOKEN_PROPERTY = "highway.skim.node.to.taz.period.token";
	public static final String TRANSIT_NODE_MAPPING_FILE_PROPERTY = "transit.skim.node.to.taz.file";
	
	private final Map<TimePeriod,Map<Integer,Integer>> highwayAssignmentNodeToTaz;
	private final Map<TimePeriod,Map<Integer,Integer>> highwayAssignmentTazToNode;
	private final Map<Integer,Integer> transitAssignmentNodeToTaz;
	private final Map<Integer,Integer> transitAssignmentTazToNode;
	
	public NodeZoneMapping(Map<String,String> properties) {
		highwayAssignmentNodeToTaz = new EnumMap<>(TimePeriod.class);
		highwayAssignmentTazToNode = new EnumMap<>(TimePeriod.class);
		String periodToken = properties.get(HIGHWAY_NODE_MAPPING_FILE_PERIOD_TOKEN_PROPERTY);
		String tokenizedPath = properties.get(HIGHWAY_NODE_MAPPING_FILE_PROPERTY);
		for (TimePeriod period: TimePeriod.values()) {
			highwayAssignmentNodeToTaz.put(period,loadNodeData(Paths.get(tokenizedPath.replace(periodToken,period.getShortName())),tazFilter));
			highwayAssignmentTazToNode.put(period,buildReverseMapping(highwayAssignmentNodeToTaz.get(period)));
		}
		transitAssignmentNodeToTaz = loadNodeData(Paths.get(properties.get(TRANSIT_NODE_MAPPING_FILE_PROPERTY)),tapFilter);
		transitAssignmentTazToNode = buildReverseMapping(transitAssignmentNodeToTaz);
	}
	
	private interface ZoneFilter {
		boolean isValidZone(int value);
	}
	
	private ZoneFilter tazFilter = new ZoneFilter() {
		public boolean isValidZone(int value) {
			return (value < 1000000) && ((value % 100000) < 10000);
		}
	};
	
	private ZoneFilter tapFilter = new ZoneFilter() {
		public boolean isValidZone(int value) {
			return (value < 1000000) && ((value % 100000) > 90000);
		}
	};
	
	private Map<Integer,Integer> loadNodeData(Path correspondenceFile, ZoneFilter filter) {
		Map<Integer,Integer> map = new HashMap<>();
		try (BufferedReader reader = new BufferedReader(new FileReader(correspondenceFile.toFile()))) {
			String line;
			while ((line = reader.readLine()) != null) {
				String[] data = line.trim().split("\\s+");
				int ptaz = Integer.parseInt(data[1]);
				if (filter.isValidZone(ptaz))
					map.put(Integer.parseInt(data[0]),ptaz);
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return map;
	}
	
	private Map<Integer,Integer> buildReverseMapping(Map<Integer,Integer> mapping) {
		Map<Integer,Integer> rmap = new HashMap<>();
		for (Entry<Integer,Integer> e : mapping.entrySet())
			rmap.put(e.getValue(),e.getKey());
		return rmap;
	}
	
	public int getHighwayAssignmenNode(TimePeriod period, int taz) {
		return highwayAssignmentTazToNode.get(period).get(taz);
	}
	
	public int getHighwayAssignmenTaz(TimePeriod period, int node) {
		return highwayAssignmentNodeToTaz.get(period).get(node);
	}
	
	public int getTransitAssignmenNode(int taz) {
		return transitAssignmentTazToNode.get(taz);
	}
	
	public int getTransitAssignmenTaz(int node) {
		return transitAssignmentNodeToTaz.get(node);
	}

}
