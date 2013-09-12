package com.pb.mtctm2.abm.visitor;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.ResourceBundle;

import org.apache.log4j.Logger;
import com.pb.mtctm2.abm.application.SandagModelStructure;
import com.pb.mtctm2.abm.application.SandagSummitFile;
import com.pb.mtctm2.abm.ctramp.CtrampApplication;
import com.pb.mtctm2.abm.ctramp.Util;

import com.pb.common.datafile.OLD_CSVFileReader;
import com.pb.common.datafile.TableDataSet;
import com.pb.common.math.MersenneTwister;
import com.pb.common.util.OutTextFile;
import com.pb.common.util.ResourceUtil;

public class VisitorTourManager {

    private static Logger logger = Logger.getLogger("visitorModel");

     private VisitorTour[] tours;
    
    VisitorModelStructure modelStructure;
	SandagModelStructure sandagStructure;
	
	TableDataSet businessTourFrequency;
	TableDataSet personalTourFrequency;
	TableDataSet partySizeFrequency;
	TableDataSet autoAvailableFrequency;
	TableDataSet incomeFrequency;
	
	TableDataSet mgraData;
	
	float occupancyRate;
	float householdRate;
	
	float businessHotelPercent;
	float businessHouseholdPercent;
	
	private boolean seek;
	private int traceId;
	
	private MersenneTwister random;
    
	/**
     * Constructor.  Reads properties file and opens/stores all probability
     * distributions for sampling.  Estimates number of airport travel parties and 
     * initializes parties[].
     * 
     * @param resourceFile  Property file.
     * 
     * Creates the array of cross-border tours.
     */
    public VisitorTourManager(HashMap<String,String> rbMap){
    	
		modelStructure = new VisitorModelStructure();
		sandagStructure = new SandagModelStructure();
		
		String directory = Util.getStringValueFromPropertyMap(rbMap,"Project.Directory");
        String mgraFile =  Util.getStringValueFromPropertyMap(rbMap,"mgra.socec.file");
        mgraFile = directory + mgraFile;
        
        occupancyRate = new Float(Util.getStringValueFromPropertyMap(rbMap, "visitor.hotel.occupancyRate"));
        householdRate = new Float(Util.getStringValueFromPropertyMap(rbMap, "visitor.household.occupancyRate"));
        
        businessHotelPercent = new Float(Util.getStringValueFromPropertyMap(rbMap, "visitor.hotel.businessPercent"));
        businessHouseholdPercent = new Float(Util.getStringValueFromPropertyMap(rbMap, "visitor.household.businessPercent"));

        String businessTourFile = Util.getStringValueFromPropertyMap(rbMap,"visitor.business.tour.file");
        businessTourFile = directory + businessTourFile;
        
        String personalTourFile = Util.getStringValueFromPropertyMap(rbMap,"visitor.personal.tour.file");
        personalTourFile = directory + personalTourFile;
      
        String partySizeFile = Util.getStringValueFromPropertyMap(rbMap,"visitor.partySize.file");
        partySizeFile = directory + partySizeFile;

        String autoAvailableFile = Util.getStringValueFromPropertyMap(rbMap,"visitor.autoAvailable.file");
        autoAvailableFile = directory + autoAvailableFile;

        String incomeFile = Util.getStringValueFromPropertyMap(rbMap,"visitor.income.file");
        incomeFile = directory + incomeFile;

        businessTourFrequency = readFile(businessTourFile);
        personalTourFrequency = readFile(personalTourFile);
        partySizeFrequency = readFile(partySizeFile);
        autoAvailableFrequency = readFile(autoAvailableFile);
        incomeFrequency = readFile(incomeFile);
              
        mgraData = readFile(mgraFile);
        
        seek = new Boolean(Util.getStringValueFromPropertyMap(rbMap,"visitor.seek"));
        traceId = new Integer(Util.getStringValueFromPropertyMap(rbMap,"visitor.trace"));
        
        random = new MersenneTwister(1000001);

    }
    
    /**
     * Read the file and return the TableDataSet.
     * 
     * @param fileName
     * @return data
     */
    private TableDataSet readFile(String fileName){
    	
    	logger.info("Begin reading the data in file " + fileName);
	    TableDataSet data;	
        try {
        	OLD_CSVFileReader csvFile = new OLD_CSVFileReader();
        	data = csvFile.readFile(new File(fileName));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        
        logger.info("End reading the data in file " + fileName);
        return data;
    }

    /**
     * Generate and attribute visitor tours
     */
	public void generateVisitorTours(){

        //calculate total number of cross border tours
		ArrayList<VisitorTour> tourList = new ArrayList<VisitorTour>();
        
        int rows = mgraData.getRowCount();
        
        int tourCount = 0;
        float personalCount = 0;
        float businessCount = 0;
        float hotelVisitorsCount = 0;
        float householdVisitorsCount = 0;
        
        float hotelRoomsCount = 0;
        float householdsCount = 0;
        
        for(int i =1;i<=rows;++i){
        	
        	float hotelRooms = mgraData.getValueAt(i,"HotelRoomTotal");
        	float households = mgraData.getValueAt(i,"hh");
        	int mgraNumber = (int) mgraData.getValueAt(i,"mgra");
        	
        	float hotelVisitorParties = hotelRooms * occupancyRate;
        	float householdVisitorParties = households * householdRate;
        	
        	float businessParties = (hotelVisitorParties * businessHotelPercent);
        	float personalParties = (hotelVisitorParties * (1.0f-businessHotelPercent));
        	
        	businessParties += (householdVisitorParties * businessHouseholdPercent);
        	personalParties += (householdVisitorParties * (1.0f-businessHouseholdPercent));
        	
        	personalCount += personalParties;
        	businessCount += businessParties;
        	
        	hotelVisitorsCount += hotelVisitorParties;
        	householdVisitorsCount += householdVisitorParties;
        	
        	hotelRoomsCount += hotelRooms;
        	householdsCount += households;
        	
        	//generate a tour for each business party
        	for(int j = 0; j< Math.round(businessParties);++j){
        		
        		int[] tourPurposes = simulateTours(businessTourFrequency);
        		
        		for(int k = 0; k< tourPurposes.length;++k){
        			VisitorTour tour = new VisitorTour(tourCount+1000001);
        			tour.setID(tourCount+1);
        			tour.setOriginMGRA(mgraNumber);
        			tour.setSegment(modelStructure.BUSINESS);
        			tour.setPurpose((byte) tourPurposes[k]);
        			calculateSize(tour);
        			calculateAutoAvailability(tour);
        			calculateIncome(tour);
        			tourList.add(tour);
        			++tourCount;
        		}
        	}
 
        	//generate a tour for each personal party
        	for(int j = 0; j< Math.round(personalParties);++j){
 
        		int[] tourPurposes = simulateTours(personalTourFrequency);
        		
        		for(int k = 0; k< tourPurposes.length;++k){
        			VisitorTour tour = new VisitorTour(tourCount+1000001);
        			tour.setID(tourCount+1);
        			tour.setOriginMGRA(mgraNumber);
        			tour.setSegment(modelStructure.PERSONAL);
        			tour.setPurpose((byte) tourPurposes[k]);
        			calculateSize(tour);
        			calculateAutoAvailability(tour);
        			calculateIncome(tour);
        			tourList.add(tour);
        			++tourCount;
        		}
        	}
        
        }
        
        if(tourList.isEmpty()){
        	logger.error("Visitor tour list is empty!!");
        	throw new RuntimeException();
        }
        
        tours = new VisitorTour[tourList.size()];
        for(int i=0;i<tours.length;++i)
        	tours[i] = tourList.get(i);
        
        logger.info("Total personal parties: "+Math.round(personalCount));
        logger.info("Total business parties: "+Math.round(businessCount));
        
        logger.info("Total hotel rooms: "+hotelRoomsCount);
        logger.info("Total households: "+householdsCount);
        logger.info("Total hotel visitors: "+Math.round(hotelVisitorsCount));
        logger.info("Total household vistors: "+Math.round(householdVisitorsCount));
        
        logger.info("Total visitor tours: "+tourCount);

      
	}
    
	/**
	 * Calculate the number of tours for this travel party, by purpose.  Return an array
	 * whose length equals the number of tours, where each element is the purpose of the tour.
	 * 
	 * @param tourFrequency A tableDataSet with the following fields WorkTours,RecreationTours,OtherTours,Percent
	 * @return  An array dimensioned to number of tours to generate, with the purpose of each.
	 */
	private int[] simulateTours(TableDataSet tourFrequency){
		
		int[] tourPurposes;
		double rand = random.nextDouble();
		
		double cumProb = 0.0;
		int row = -1;
		for(int i = 0; i<tourFrequency.getRowCount();++i){
			
			float percent = tourFrequency.getValueAt(i+1, "Percent");
			cumProb += percent;
			if(rand<cumProb){
				row = i+1;
				break;
			}
		}
		int workTours = (int) tourFrequency.getValueAt(row, "WorkTours");
		int recTours = (int) tourFrequency.getValueAt(row,"RecreationTours");
		int otherTours = (int) tourFrequency.getValueAt(row,"OtherTours");
			
		int totalTours = workTours + recTours + otherTours;
		tourPurposes = new int[totalTours];
			
		int workSet=0;
		int recSet=0;
		int otherSet=0;
		for(int j = 0; j<tourPurposes.length;++j){
			
			if(workTours>0 && workSet<workTours){
				tourPurposes[j] = modelStructure.WORK;
				++workSet;
			}else if(recTours>0 && recSet<recTours){
				tourPurposes[j] = modelStructure.RECREATION;
				++recSet;
			}else if(otherTours>0 && otherSet<otherTours){
				tourPurposes[j] = modelStructure.OTHER;
				++otherSet;
			}
		}
		return tourPurposes;
	}
	
		
	/**
	 * Calculate the size of the tour and store in tour object.
	 * @param tour
	 */
	private void calculateSize(VisitorTour tour){
		
		byte purp = tour.getPurpose();
		String purpString = modelStructure.VISITOR_PURPOSES[purp];
		String columnName = purpString.toLowerCase();
		
		double cumProb = 0;
		double rand = tour.getRandom();
		byte size = -1;
		int rowCount = partySizeFrequency.getRowCount();
		for(int i=1;i<=rowCount;++i){
			cumProb += partySizeFrequency.getValueAt(i,columnName);
			if(rand<cumProb){
				size = (byte) partySizeFrequency.getValueAt(i,"PartySize");
				break;
			}
		}
		if(size==-1){
			logger.error("Error attempting to choose party size for visitor tour "+tour.getID());
			throw new RuntimeException();
		}
		tour.setNumberOfParticipants(size);
	}
	
	/**
	 * Calculate whether autos are available for this tour.
	 * @param tour
	 */
	private void calculateAutoAvailability(VisitorTour tour){
		
		byte purp = tour.getPurpose();
		String purpString = modelStructure.VISITOR_PURPOSES[purp];
		String columnName = purpString.toLowerCase();
		
		double rand = tour.getRandom();
		boolean autoAvailable = false;
		double probability = autoAvailableFrequency.getValueAt(1,columnName);
		if(rand<probability)
				autoAvailable=true;
		
		tour.setAutoAvailable(autoAvailable ? 1 : 0);
	}
	
	
	/**
	 * Calculate the income of the tour
	 * @param tour
	 */
	private void calculateIncome(VisitorTour tour){
		byte segment = tour.getSegment();
		String segmentString = modelStructure.VISITOR_SEGMENTS[segment];
		String columnName = segmentString.toLowerCase();
		
		double rand = tour.getRandom();
		int income = -1;
		double cumProb=0;
		int rowCount = incomeFrequency.getRowCount();
		for(int i=1;i<=rowCount;++i){
			cumProb += incomeFrequency.getValueAt(i,columnName);
			if(rand<cumProb){
				income =(int) incomeFrequency.getValueAt(i,"Income");
				break;
			}
		}
		if(income==-1){
			logger.error("Error attempting to choose party size for visitor tour "+tour.getID());
			throw new RuntimeException();
		}
		tour.setIncome(income);
		
		}
	
	
	/**
	 * Create a text file and write all records to the file.
	 * 
	 */
	public void writeOutputFile(HashMap<String,String> rbMap){
		
		//Open file and print header
		
		String directory = Util.getStringValueFromPropertyMap(rbMap,"Project.Directory");
        String tourFileName = directory+Util.getStringValueFromPropertyMap(rbMap,"visitor.tour.output.file");
        String tripFileName = directory+Util.getStringValueFromPropertyMap(rbMap,"visitor.trip.output.file");

		logger.info("Writing visitor tours to file "+tourFileName);
		logger.info("Writing visitor trips to file "+tripFileName);

		PrintWriter tourWriter = null;
		try {
			tourWriter = new PrintWriter(
		                    new BufferedWriter(
		                            new FileWriter(tourFileName)));
		    } catch (IOException e) {
		    	logger.fatal("Could not open file " + tourFileName + " for writing\n");
		    	throw new RuntimeException();
		    }
		String tourHeaderString = new String("id,segment,purpose,autoAvailable,partySize,income,departTime,arriveTime,originMGRA,destinationMGRA,tourMode,outboundStops,inboundStops\n");
		tourWriter.print(tourHeaderString);
		
		PrintWriter tripWriter = null;
		try {
			tripWriter = new PrintWriter(
		                    new BufferedWriter(
		                            new FileWriter(tripFileName)));
		    } catch (IOException e) {
		    	logger.fatal("Could not open file " + tripFileName + " for writing\n");
		    	throw new RuntimeException();
		    }
		String tripHeaderString = new String("tourID,tripID,originPurp,destPurp,originMGRA,destinationMGRA,inbound,originIsTourDestination,destinationIsTourDestination,period,tripMode,boardingTap,alightingTap\n");
		tripWriter.print(tripHeaderString);
		
		//Iterate through the array, printing records to the file
		for(int i = 0; i < tours.length;++i){
			
			VisitorTour tour = tours[i];

    		if(seek && tour.getID()!=traceId)
    			continue;
    		
			VisitorTrip[] trips = tours[i].getTrips();
			
			if(trips==null)
				continue;

			writeTour(tour,tourWriter);

			
			for(int j=0;j<trips.length;++j){
				writeTrip(tour,trips[j],j+1,tripWriter);
			}
		}
			
		tourWriter.close();
		tripWriter.close();
		
	}

	/**
	 * Write the tour to the PrintWriter
	 * @param tour
	 * @param writer
	 */
	private void writeTour(VisitorTour tour, PrintWriter writer){
		String record = new String(
				tour.getID() + "," +	
				tour.getSegment() + "," +	
				tour.getPurpose() + "," + 
				tour.getAutoAvailable() + "," + 
				tour.getNumberOfParticipants() + "," + 
				tour.getIncome() + "," +
				tour.getDepartTime() + "," +
				tour.getArriveTime() + "," +
				tour.getOriginMGRA() + "," +
				tour.getDestinationMGRA() + "," +
				tour.getTourMode() +  "," +
				tour.getNumberOutboundStops() + "," +
				tour.getNumberInboundStops() + "\n"
			);
			writer.print(record);

	}
	
	/**
	 * Write the trip to the PrintWriter
	 * @param tour
	 * @param trip
	 * @param tripNumber
	 * @param writer
	 */
	private void writeTrip(VisitorTour tour, VisitorTrip trip, int tripNumber, PrintWriter writer){

		int[] taps = getTapPair(trip);

		String record = new String(
				tour.getID() + "," +	
				tripNumber + "," +	
				trip.getOriginPurpose() + "," +
				trip.getDestinationPurpose()  + "," +
				trip.getOriginMgra()  + "," +
				trip.getDestinationMgra()  + "," +
				trip.isInbound() + "," +
				trip.isOriginIsTourDestination() + "," +
				trip.isDestinationIsTourDestination() + "," +
				trip.getPeriod()  + "," +
				trip.getTripMode() + "," +
				taps[0] + "," +
				taps[1]  + "\n"
			);
			writer.print(record);
	}
	
	/**
	 * A helper method that returns an array containing boarding tap (element 0) and alighting tap (element 1)
	 * for the given trip mode.  Returns an array of zeroes if the trip modes are not transit.
	 * 
	 * @param party  The trip
	 * @return       An array containing boarding TAP and alighting TAP
	 */
	public int[] getTapPair(VisitorTrip trip){
		
		int[] taps = new int[2];
		
		//ride mode will be -1 if not transit
		int tripMode = trip.getTripMode();
		int rideMode = sandagStructure.getRideModeIndexForTripMode(tripMode);

		if(sandagStructure.getTripModeIsWalkTransit(tripMode))
			taps =  trip.getWtwTapPair(rideMode);
		else if(sandagStructure.getTripModeIsKnrTransit(tripMode))
			if(trip.isInbound())
				taps = trip.getWtdTapPair(rideMode);
			else
				taps = trip.getDtwTapPair(rideMode);
		
		return taps;
	}

	/**
	 * @return the parties
	 */
	public VisitorTour[] getTours() {
		return tours;
	}
	

	public static void main(String args[]){
		
		String propertiesFile = null;
        HashMap<String,String> pMap;
		
		logger.info(String.format("SERPM Activity Based Model using CT-RAMP version %s",
	                CtrampApplication.VERSION));

		//logger.info(String.format("Running Cross Border Model Tour Manager"));

		if (args.length == 0)
	    {
			logger.error( String.format("no properties file base name (without .properties extension) was specified as an argument.") );
	        return;
	    } else
	    	propertiesFile = args[0];
        
        pMap = ResourceUtil.getResourceBundleAsHashMap ( propertiesFile );
		VisitorTourManager apm = new VisitorTourManager(pMap);
		apm.generateVisitorTours();
		apm.writeOutputFile(pMap);
		
		//logger.info("Cross-Border Tour Manager successfully completed!");
	
	}
}
