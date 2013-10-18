package com.pb.mtctm2.abm.visitor;

import gnu.cajo.invoke.Remote;
import gnu.cajo.utils.ItemServer;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.UnknownHostException;
import java.nio.file.Paths;
import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.MissingResourceException;
import java.util.ResourceBundle;
import java.util.StringTokenizer;

import org.apache.log4j.Logger;

import com.pb.mtctm2.abm.application.SandagModelStructure;
import com.pb.mtctm2.abm.ctramp.CtrampApplication;
import com.pb.mtctm2.abm.ctramp.MatrixDataServer;
import com.pb.mtctm2.abm.ctramp.MatrixDataServerRmi;
import com.pb.mtctm2.abm.ctramp.TazDataHandler;
import com.pb.mtctm2.abm.ctramp.Util;
import com.pb.mtctm2.abm.ctramp.MgraDataManager;
import com.pb.mtctm2.abm.ctramp.TapDataManager;
import com.pb.mtctm2.abm.ctramp.TazDataManager;
import com.pb.common.calculator.MatrixDataManager;
import com.pb.common.datafile.CSVFileReader;
import com.pb.common.datafile.OLD_CSVFileReader;
import com.pb.common.datafile.TableDataSet;
import com.pb.common.matrix.Matrix;
import com.pb.common.matrix.MatrixType;
import com.pb.common.summit.*;
import com.pb.common.util.ResourceUtil;

public class VisitorTripTables {

	private static Logger logger = Logger.getLogger("tripTables");
    public static final int                            MATRIX_DATA_SERVER_PORT                                   = 1171;
    public static final int TAXI_CODE = 27;
    
    private  TableDataSet tripData;

    //Some parameters
    private int[] modeIndex;  // an index array, dimensioned by number of total modes, returns 0=auto modes, 1=non-motor, 2=transit, 3= other
    private int[] matrixIndex; // an index array, dimensioned by number of modes, returns the element of the matrix array to store value
    
    //array modes: AUTO, NON-MOTORIZED, TRANSIT,  OTHER
	private int autoModes=0;
	private int tranModes=0;
	private int nmotModes=0;
	private int othrModes=0;
    
    //one file per time period
    private int numberOfPeriods;
   
    private HashMap<String,String> rbMap;
    
    // matrices are indexed by modes
    private Matrix[][] matrix;
     
    private ResourceBundle rb;
    private MgraDataManager mgraManager;
    private TazDataManager tazManager;
    private TapDataManager tapManager;
    private VisitorModelStructure modelStructure;
    
    private MatrixDataServerRmi ms;
    private float sampleRate;
        
     public VisitorTripTables(HashMap<String,String> rbMap){

     	this.rbMap = rbMap;
        tazManager = TazDataManager.getInstance(rbMap);
		tapManager = TapDataManager.getInstance(rbMap);
		mgraManager = MgraDataManager.getInstance(rbMap);
        
		modelStructure = new VisitorModelStructure();
		
		//Time period limits
		numberOfPeriods = modelStructure.getNumberModelPeriods();
	
		//number of modes
		modeIndex = new int[modelStructure.MAXIMUM_TOUR_MODE_ALT_INDEX];
		matrixIndex = new int[modeIndex.length];
		
		//set the mode arrays
		for(int i=1;i<modeIndex.length;++i){
			if(modelStructure.getTourModeIsSovOrHov(i)){
				modeIndex[i] = 0;
				matrixIndex[i]= autoModes;
				++autoModes;
			}else if(modelStructure.getTourModeIsNonMotorized(i)){
				modeIndex[i] = 1;
				matrixIndex[i]= nmotModes;
				++nmotModes;
			}else if(modelStructure.getTourModeIsWalkTransit(i)|| modelStructure.getTourModeIsDriveTransit(i)){
				modeIndex[i] = 2;
				matrixIndex[i]= tranModes;
				++tranModes;
			}else{
				modeIndex[i] = 3;
				matrixIndex[i]= othrModes;
				++othrModes;
			}
		}
	}
	
	/**
	 * Initialize all the matrices for the given time period.
	 *
	 * @param periodName  The name of the time period.
	 */
	public void initializeMatrices(String periodName){
		
		/*
		 * This won't work because external stations aren't listed in the MGRA file
		int[] tazIndex = tazManager.getTazsOneBased();
        int tazs = tazIndex.length-1;
     */
		//Instead, use maximum taz number
		int maxTaz = tazManager.getMaxTaz();
	    int[] tazIndex = new int[maxTaz+1];
	        
	    //assume zone numbers are sequential
	    for(int i=1;i<tazIndex.length;++i)
	    	tazIndex[i] = i;
        
        // get the tap index
		int[] tapIndex = tapManager.getTaps();
		int taps = tapIndex.length - 1;

		//Initialize matrices; one for each mode group (auto, non-mot, tran, other)
		//All matrices will be dimensioned by TAZs except for transit, which is
		//dimensioned by TAPs
		int numberOfModes = 4;
		matrix = new Matrix[numberOfModes][];
		for(int i = 0; i < numberOfModes; ++ i){
			
			String modeName;
			
			if(i==0){
				matrix[i] = new Matrix[autoModes];
				for(int j=0;j<autoModes;++j){
					modeName = modelStructure.getModeName(j+1);
					matrix[i][j] = new Matrix(modeName+"_"+periodName,"",maxTaz,maxTaz);
					matrix[i][j].setExternalNumbers(tazIndex);	
				}
			}else if(i==1){
				matrix[i] = new Matrix[nmotModes];
				for(int j=0;j<nmotModes;++j){
					modeName = modelStructure.getModeName(j+1+autoModes);
					matrix[i][j] = new Matrix(modeName+"_"+periodName,"",maxTaz,maxTaz);
					matrix[i][j].setExternalNumbers(tazIndex);	
				}
			}else if(i==2){
				matrix[i] = new Matrix[tranModes];
				for(int j=0;j<tranModes;++j){
					modeName = modelStructure.getModeName(j+1+autoModes+nmotModes);
					matrix[i][j] = new Matrix(modeName+"_"+periodName,"",taps,taps);
					matrix[i][j].setExternalNumbers(tapIndex);	
				}
			}else{
				matrix[i] = new Matrix[othrModes];
				for(int j=0;j<othrModes;++j){
					modeName = modelStructure.getModeName(j+1+autoModes+nmotModes+tranModes);
					matrix[i][j] = new Matrix(modeName+"_"+periodName,"",maxTaz,maxTaz);
					matrix[i][j].setExternalNumbers(tazIndex);	
				}
			}
		}
	}
	
	
	/**
	 * Create trip tables for all time periods and modes.
	 * This is the main entry point into the class; it should be called
	 * after instantiating the SandagTripTables object.
	 * 
	 */
	public void createTripTables(MatrixType mt){
		
		String directory = Util.getStringValueFromPropertyMap(rbMap,"Project.Directory");

		//Open the individual trip file 
		String tripFile = Paths.get(directory,Util.getStringValueFromPropertyMap(rbMap,"visitor.trip.output.file")).toString();
		
		//Iterate through periods so that we don't have to keep
		//trip tables for all periods in memory.
		for(int i=0;i<numberOfPeriods;++i){
			
			//Initialize the matrices
			initializeMatrices(modelStructure.getModelPeriodLabel(i));
	
			//process trips
			processTrips(i, tripData);
			
			
			logger.info("Begin writing matrices");
	        writeTrips(i, mt);
	        logger.info("End writingMatrices");

		}
        
	}
	
	/**
	 * Open a trip file and return the Tabledataset.
	 * 
	 * @fileName  The name of the trip file
	 * @return The tabledataset
	 */
	public TableDataSet openTripFile(String fileName){
		
	    logger.info("Begin reading the data in file " + fileName);
	    TableDataSet tripData;
	    
        try {
        	OLD_CSVFileReader csvFile = new OLD_CSVFileReader();
            tripData = csvFile.readFile(new File(fileName));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        
        logger.info("End reading the data in file " + fileName);
        return tripData;
	}
	
	/**
	 * This is the main workhorse method in this class.  It iterates over records in the trip file.  
	 * Attributes for the trip record are read, and the trip record is accumulated in the relevant
	 * matrix.
	 * 
	 * @param timePeriod  The time period to process
	 * @param tripData  The trip data file to process
	 */
	public void processTrips(int timePeriod, TableDataSet tripData){
		
        logger.info("Begin processing trips for period "+ timePeriod);

      //iterate through the trip data and save trips in arrays
        for(int i = 1; i <= tripData.getRowCount(); ++i){
        
            if(i<=5 || i % 1000==0)
            	logger.info("Reading record "+i);

        	int departTime = (int) tripData.getValueAt(i,"period");
        	int period = modelStructure.getModelPeriodIndex(departTime);
            if(period!=timePeriod)
            	continue;
            
        	int originMGRA = (int) tripData.getValueAt(i, "originMGRA");
        	int destinationMGRA = (int) tripData.getValueAt(i, "destinationMGRA");
        	int tripMode = (int) tripData.getValueAt(i,"tripMode");

        	//save taxi trips as shared-2
        	if(tripMode == TAXI_CODE){
        		tripMode=3;
        	}
        	int originTAZ = mgraManager.getTaz(originMGRA);
        	int destinationTAZ = mgraManager.getTaz(destinationMGRA);
        	boolean inbound =  tripData.getBooleanValueAt(i,"inbound");
        	
          	//transit trip - get boarding and alighting tap
        	int boardTap=0;
        	int alightTap=0;
        	
        	if(modelStructure.getTourModeIsWalkTransit(tripMode)||modelStructure.getTourModeIsDriveTransit(tripMode)){
        		boardTap=(int) tripData.getValueAt(i,"boardingTAP");
        		alightTap = (int) tripData.getValueAt(i,"alightingTAP");
        	}
        	
        	// all person trips are 1 per party (for now)
        	float personTrips=1.0f/sampleRate;
        	
        	// all auto trips are 1 per party
        	float vehicleTrips = 1.0f/sampleRate;
        	
        	//Store in matrix
        	int mode = modeIndex[tripMode];
    		int mat = matrixIndex[tripMode];
        	if(mode==0){
        		float value = matrix[mode][mat].getValueAt(originTAZ, destinationTAZ);
        		matrix[mode][mat].setValueAt(originTAZ, destinationTAZ, (value + vehicleTrips));
        	}else if(mode==1){
        		float value = matrix[mode][mat].getValueAt(originTAZ, destinationTAZ);
        		matrix[mode][mat].setValueAt(originTAZ, destinationTAZ, (value + personTrips));
        	}else if(mode==2){
        		
        		if(boardTap==0||alightTap==0)
        			continue;
        		
        		float value = matrix[mode][mat].getValueAt(boardTap, alightTap);
        		matrix[mode][mat].setValueAt(boardTap, alightTap, (value + personTrips));

        		//Store PNR transit trips in SOV free mode skim (mode 0 mat 0)
        		if(modelStructure.getTourModeIsDriveTransit(tripMode)){
        			
        			// add the vehicle trip portion to the trip table
    				if(inbound){ //from origin to lot (boarding tap)
        				int PNRTAZ = tapManager.getTazForTap(boardTap);
    					value = matrix[0][0].getValueAt(originTAZ, PNRTAZ);
    					matrix[0][0].setValueAt(originTAZ,PNRTAZ,(value+vehicleTrips));
    					
    				}else{  // from lot (alighting tap) to destination 
        				int PNRTAZ = tapManager.getTazForTap(alightTap);
    					value = matrix[0][0].getValueAt(PNRTAZ, destinationTAZ);
    					matrix[0][0].setValueAt(PNRTAZ, destinationTAZ,(value+vehicleTrips));
    				}
       			
        		}
        	}else{
        		float value = matrix[mode][mat].getValueAt(originTAZ, destinationTAZ);
        		matrix[mode][mat].setValueAt(originTAZ, destinationTAZ, (value + personTrips));
    		}
 
       
        logger.info("End creating trip tables for period "+ timePeriod);
        }
	}
	
	/**
	 * Get the output trip table file names from the properties file,
	 * and write trip tables for all modes for the given time period.
	 * 
	 * @param period  Time period, which will be used to find the
	 * period time string to append to each trip table matrix file
	 */
	public void writeTrips(int period, MatrixType mt){
		

		String directory = Util.getStringValueFromPropertyMap(rbMap,"scenario.path");
		//String per = modelStructure.getModelPeriodLabel(period);
		String end = ".mat";
		String[] fileName = new String[4];
		
		fileName[0] = Paths.get(directory,Util.getStringValueFromPropertyMap(rbMap,"visitor.results.autoTripMatrix")).toString();
		fileName[1] = Paths.get(directory,Util.getStringValueFromPropertyMap(rbMap,"visitor.results.nMotTripMatrix")).toString();
		fileName[2] = Paths.get(directory,Util.getStringValueFromPropertyMap(rbMap,"visitor.results.tranTripMatrix")).toString(); 
		fileName[3] = Paths.get(directory,Util.getStringValueFromPropertyMap(rbMap,"visitor.results.othrTripMatrix")).toString(); 
		
		for(int i=0;i<4;++i)
		{
			for (int j=0; j<matrix[i].length; j++) {
				
				String matFileName = fileName[i] + "_" + matrix[i][j].getName() + end;
				Matrix[] temp = new Matrix[1];
				temp[0] = matrix[i][j];
				ms.writeMatrixFile(matFileName, temp);
				
				logger.info( matrix[i][j].getName() + " has " + matrix[i][j].getRowCount() + " rows, " + matrix[i][j].getColumnCount() + " cols, and a total of " + matrix[i][j].getSum() );
				logger.info("Writing " + matFileName);
			}
			//ms.writeMatrixFile(fileName[i], matrix[i]);
		}
		
 	}
	
//	/**
//	 * Utility method to write a set of matrices to disk.
//	 * 
//	 * @param fileName The file name to write to.
//	 * @param m  An array of matrices
//	 */
//	public void writeMatrixFile(String fileName, Matrix[] m){
//		
//		//auto trips		
//		MatrixWriter writer = MatrixWriter.createWriter(fileName); 
//		String[] names = new String[m.length];
//		
//		for (int i=0; i<m.length; i++) {
//			names[i] = m[i].getName();
//			logger.info( m[i].getName() + " has " + m[i].getRowCount() + " rows, " + m[i].getColumnCount() + " cols, and a total of " + m[i].getSum() );
//		}
//
//		writer.writeMatrices(names, m); 
//	}
	

	
	/**
	 * @param args
	 */
	public static void main(String[] args) {

        HashMap<String,String> pMap;
		String propertiesFile = null;
		boolean is64bit = false;
		 
		logger.info(String.format("SERPM Visitor Model Trip Table Generation Program using CT-RAMP version %s",
                CtrampApplication.VERSION));

		if (args.length == 0)
    	{
			logger.error( String.format("no properties file base name (without .properties extension) was specified as an argument.") );
        	return;
    	} else
    		propertiesFile = args[0];
		
		pMap = ResourceUtil.getResourceBundleAsHashMap ( propertiesFile );
    	VisitorTripTables tripTables = new VisitorTripTables(pMap);
    	
		float sampleRate = 1.0f;
		for (int i = 1; i< args.length;++i){
			if (args[i].equalsIgnoreCase("-sampleRate"))
			{
				sampleRate = Float.parseFloat(args[i + 1]);
			}
			if (args[i].equalsIgnoreCase("-is64bit"))
			{
				is64bit = Boolean.parseBoolean(args[i + 1]);
			}
		}
        logger.info(String.format("-sampleRate %.4f.", sampleRate));

    	tripTables.setSampleRate(sampleRate);
        
		String matrixServerAddress = "";
        int serverPort = 0;
        try
        {
            // get matrix server address. if "none" is specified, no server will be
            // started, and matrix io will ocurr within the current process.
            matrixServerAddress = Util.getStringValueFromPropertyMap( pMap, "RunModel.MatrixServerAddress" );
            try {
                // get matrix server port.
                serverPort = Util.getIntegerValueFromPropertyMap( pMap, "RunModel.MatrixServerPort" );
            }
            catch (MissingResourceException e) {
                // if no matrix server address entry is found, leave undefined --
                // it's eithe not needed or show could create an error.
            }
        }
        catch (MissingResourceException e) {
            // if no matrix server address entry is found, set to localhost, and a
            // separate matrix io process will be started on localhost.
            matrixServerAddress = "localhost";
            serverPort = MATRIX_DATA_SERVER_PORT;
        }
       
        try
        {
        	if (!matrixServerAddress.equalsIgnoreCase("none"))
            {
        		tripTables.ms = new MatrixDataServerRmi( matrixServerAddress, serverPort, MatrixDataServer.MATRIX_DATA_SERVER_NAME );
        		tripTables.ms.testRemote( "VisitorTripTables" );
            	
    
            	//these methods need to be called to set the matrix data manager in the matrix data server
                MatrixDataManager mdm = MatrixDataManager.getInstance();
                mdm.setMatrixDataServerObject(tripTables.ms);
            }

        } catch (Exception e)
        {
            logger.error(String.format("exception caught running ctramp model components -- exiting."), e);
            throw new RuntimeException();
        }

        String matrixTypeName = Util.getStringValueFromPropertyMap( pMap,"Results.MatrixType" );
        MatrixType mt = MatrixType.lookUpMatrixType( matrixTypeName );
        
		tripTables.createTripTables(mt);

	}

	   /**
	 * @return the sampleRate
	 */
	public double getSampleRate() {
		return sampleRate;
	}
	/**
	 * @param sampleRate the sampleRate to set
	 */
	public void setSampleRate(float sampleRate) {
		this.sampleRate = sampleRate;
	}

}
