package com.pb.mtctm2.abm.reports;

import com.pb.common.calculator.MatrixDataManager;
import com.pb.common.calculator.MatrixDataServerIf;
import org.apache.log4j.Logger;
import com.pb.mtctm2.abm.accessibilities.*;
import com.pb.mtctm2.abm.ctramp.MatrixDataServer;
import com.pb.mtctm2.abm.ctramp.MatrixDataServerRmi;
import com.pb.mtctm2.abm.ctramp.ModelStructure;
import com.pb.mtctm2.abm.ctramp.TapDataManager;
import com.pb.mtctm2.abm.ctramp.MgraDataManager;
import com.pb.mtctm2.abm.ctramp.Modes;
import com.pb.mtctm2.abm.ctramp.TazDataManager;
import com.pb.mtctm2.abm.ctramp.Util;

import java.io.File;
import java.util.*;

/**
 * The {@code SkimBuilder} ...
 *
 * @author crf
 *         Started 10/17/12 9:12 AM
 * @revised 5/6/2013 for SERPM (ymm)
 */
public class SkimBuilder {
    private final static Logger logger = Logger.getLogger(SkimBuilder.class);

    private static final int WALK_TIME_INDEX = 0;
    private static final int BIKE_TIME_INDEX = 0;

    private static final int DA_TIME_INDEX = 0;
    private static final int DA_FF_TIME_INDEX = 1;
    private static final int DA_DIST_INDEX = 2;
    private static final int DA_TOLL_TIME_INDEX = 3;
    private static final int DA_TOLL_FF_TIME_INDEX = 4;
    private static final int DA_TOLL_DIST_INDEX = 5;
    private static final int DA_TOLL_COST_INDEX = 6;
    private static final int DA_TOLL_TOLLDIST_INDEX = 7;
    private static final int HOV_TIME_INDEX = 8;
    private static final int HOV_FF_TIME_INDEX = 9;
    private static final int HOV_DIST_INDEX = 10;
    private static final int HOV_HOVDIST_INDEX = 11;
    
    private static final int HOV_TOLL_TIME_INDEX = 12;
    private static final int HOV_TOLL_FF_TIME_INDEX = 13;
    private static final int HOV_TOLL_DIST_INDEX = 14;
    private static final int HOV_TOLL_COST_INDEX = 15;
    private static final int HOV_TOLL_HOV_DIST_INDEX = 16;
    private static final int HOV_TOLL_TOLL_DIST_INDEX = 17;

    private static final int TRANSIT_LOCAL_ACCESS_TIME_INDEX = 0;
    private static final int TRANSIT_LOCAL_EGRESS_TIME_INDEX = 1;
    private static final int TRANSIT_LOCAL_AUX_WALK_TIME_INDEX = 2;
    private static final int TRANSIT_LOCAL_TOTTIME_INDEX = 3;
    private static final int TRANSIT_LOCAL_FIRST_WAIT_TIME_INDEX = 4;    
    private static final int TRANSIT_LOCAL_TRANSFER_WAIT_TIME_INDEX = 5;
    private static final int TRANSIT_LOCAL_FARE_INDEX = 6;
    private static final int TRANSIT_LOCAL_XFERS_INDEX = 7;

    private static final int TRANSIT_PREM_ACCESS_TIME_INDEX = 0;
    private static final int TRANSIT_PREM_EGRESS_TIME_INDEX = 1;
    private static final int TRANSIT_PREM_AUX_WALK_TIME_INDEX = 2;
    private static final int TRANSIT_PREM_LOCAL_BUS_TIME_INDEX = 3;
    private static final int TRANSIT_PREM_EXPRESS_BUS_TIME_INDEX = 4;
    private static final int TRANSIT_PREM_BRT_TIME_INDEX = 5;
    private static final int TRANSIT_PREM_LRT_TIME_INDEX = 6;
    private static final int TRANSIT_PREM_CR_TIME_INDEX = 7;
    private static final int TRANSIT_PREM_FIRST_WAIT_TIME_INDEX = 8;
    private static final int TRANSIT_PREM_TRANSFER_WAIT_TIME_INDEX = 9;
    private static final int TRANSIT_PREM_FARE_INDEX = 10;
    private static final int TRANSIT_MAIN_MODE_INDEX = 11;
    private static final int TRANSIT_PREM_XFERS_INDEX = 12;

    private final TapDataManager tapManager;
    private final TazDataManager tazManager;
    private final MgraDataManager mgraManager;
    private final AutoTazSkimsCalculator tazDistanceCalculator;
    private final AutoAndNonMotorizedSkimsCalculator autoNonMotSkims;
    private final WalkTransitWalkSkimsCalculator wtw;
    private final WalkTransitDriveSkimsCalculator wtd;
    private final DriveTransitWalkSkimsCalculator dtw;
    
    private float DEFAULT_BIKE_SPEED;//get values from autoNonMotSkims
    private float DEFAULT_WALK_SPEED;//get values from autoNonMotSkims

    public SkimBuilder(Properties properties) {

        HashMap<String,String> rbMap = new HashMap<String,String>((Map<String,String>) (Map) properties);
        startMatrixServer(properties);

           
        tapManager = TapDataManager.getInstance(rbMap);
        tazManager = TazDataManager.getInstance(rbMap);
        mgraManager = MgraDataManager.getInstance(rbMap);
        tazDistanceCalculator = new AutoTazSkimsCalculator(rbMap);
        tazDistanceCalculator.computeTazDistanceArrays();
        autoNonMotSkims = new AutoAndNonMotorizedSkimsCalculator(rbMap);
        autoNonMotSkims.setTazDistanceSkimArrays(tazDistanceCalculator.getStoredFromTazToAllTazsDistanceSkims(),tazDistanceCalculator.getStoredToTazFromAllTazsDistanceSkims());

        DEFAULT_BIKE_SPEED = (float) autoNonMotSkims.getBikeSpeed();
        DEFAULT_WALK_SPEED = (float) autoNonMotSkims.getWalkSpeed();
        
        BestTransitPathCalculator bestPathUEC = new BestTransitPathCalculator(rbMap);
        wtw = new WalkTransitWalkSkimsCalculator();
        wtw.setup(rbMap,logger, bestPathUEC);
        wtd = new WalkTransitDriveSkimsCalculator();
        wtd.setup(rbMap,logger, bestPathUEC);
        dtw = new DriveTransitWalkSkimsCalculator();
        dtw.setup(rbMap,logger, bestPathUEC);
        
    }

    private void startMatrixServer(Properties properties) {
        String serverAddress = (String) properties.get("RunModel.MatrixServerAddress");
        int serverPort = new Integer((String) properties.get("RunModel.MatrixServerPort"));
        logger.info("connecting to matrix server " + serverAddress + ":" + serverPort);

        try{

            MatrixDataManager mdm = MatrixDataManager.getInstance();
            MatrixDataServerIf ms = new MatrixDataServerRmi(serverAddress, serverPort, MatrixDataServer.MATRIX_DATA_SERVER_NAME);
            ms.testRemote(Thread.currentThread().getName());
            mdm.setMatrixDataServerObject(ms);

        } catch (Exception e) {
            logger.error("could not connect to matrix server", e);
            throw new RuntimeException(e);

        }

    }

    //todo: hard coding these next two lookups because it is convenient, but probably should move to a lookup file
    private final String[] modeNameLookup = {
            "UNKNOWN", //ids start at one
            "DRIVEALONEFREE", 	//1
            "DRIVEALONEPAY",	//2
            "SHARED2GP",		//3
            "SHARED2HOV",		//4
            "SHARED2PAY",		//5
            "SHARED3GP",		//6
            "SHARED3HOV",		//7
            "SHARED3PAY",		//8
            "WALK",				//9
            "BIKE",				//10
            "WALK_LOC",			//11
            "WALK_EXP",			//12
            "WALK_BRT",			//13
            "WALK_LR",			//14
            "WALK_CR",			//15
            "PNR_LOC",			//16
            "PNR_EXP",			//17
            "PNR_BRT",			//18
            "PNR_LR",			//19
            "PNR_CR",			//20
            "KNR_LOC",			//21
            "KNR_EXP",			//22
            "KNR_BRT",			//23
            "KNR_LR",			//24
            "KNR_CR",			//25
            "SCHBUS",			//26
    };

    private final TripModeChoice[] modeChoiceLookup = {
            TripModeChoice.UNKNOWN,
            TripModeChoice.DRIVE_ALONE_NO_TOLL,
            TripModeChoice.DRIVE_ALONE_TOLL,
            TripModeChoice.DRIVE_ALONE_NO_TOLL,
            TripModeChoice.HOV_NO_TOLL,
            TripModeChoice.HOV_TOLL,
            TripModeChoice.DRIVE_ALONE_NO_TOLL,
            TripModeChoice.HOV_NO_TOLL,
            TripModeChoice.HOV_TOLL,
            TripModeChoice.WALK,
            TripModeChoice.BIKE,
            TripModeChoice.WALK_LB,
            TripModeChoice.WALK_EB,
            TripModeChoice.WALK_BRT,
            TripModeChoice.WALK_LRT,
            TripModeChoice.WALK_CR,
            TripModeChoice.DRIVE_LB,
            TripModeChoice.DRIVE_EB,
            TripModeChoice.DRIVE_BRT,
            TripModeChoice.DRIVE_LRT,
            TripModeChoice.DRIVE_CR,
            TripModeChoice.DRIVE_LB,
            TripModeChoice.DRIVE_EB,
            TripModeChoice.DRIVE_BRT,
            TripModeChoice.DRIVE_LRT,
            TripModeChoice.DRIVE_CR,
            TripModeChoice.HOV_NO_TOLL

    };

    private int getTod(int tripTimePeriod) {
        int tod = ModelStructure.getSkimPeriodIndex(tripTimePeriod);
        return tod;
    }

    private int getStartTime(int tripTimePeriod) {
        return (tripTimePeriod-1)*30 + 270; //starts at 4:30 and goes half hour intervals after that
    }

    public TripAttributes getTripAttributes(int origin, int destination, int tripModeIndex, int boardTap, int alightTap, int tripTimePeriod, boolean inbound) {
        int tod = getTod(tripTimePeriod);
        TripModeChoice tripMode = modeChoiceLookup[tripModeIndex < 0 ? 0 : tripModeIndex];
        TripAttributes attributes = getTripAttributes(tripMode,origin,destination,boardTap,alightTap,tod,inbound);
        attributes.setTripModeName(modeNameLookup[tripModeIndex < 0 ? 0 : tripModeIndex]);
        attributes.setTripStartTime(getStartTime(tripTimePeriod));
        int oTaz = -1;
        int dTaz = -1;
        oTaz = mgraManager.getTaz(origin);
        dTaz = mgraManager.getTaz(destination);
        attributes.setOriginTAZ(oTaz);
        attributes.setDestinationTAZ(dTaz);
        return attributes;
    }

    private TripAttributes getTripAttributesUnknown() {
        return new TripAttributes(-1,-1,-1,-1,-1);
    }

    private double getCost(double baseCost, double driveDist) {
        return baseCost;
    }

    private TripAttributes getTripAttributes(TripModeChoice modeChoice, int origin, int destination, int boardTap, int alightTap, int tod, boolean inbound) {
        int timeIndex = -1;
        int distIndex = -1;
        int costIndex = -1;

        int rideModeIndex = modeChoice.getRideModeIndex();
        

        switch (modeChoice) {
            case UNKNOWN : return getTripAttributesUnknown();
            case DRIVE_ALONE_NO_TOLL : {
                timeIndex = DA_TIME_INDEX;
                distIndex = DA_DIST_INDEX;
                costIndex = -1;
            }
            case DRIVE_ALONE_TOLL : {
                if (timeIndex < 0) {
                    timeIndex = DA_TOLL_TIME_INDEX;
                    distIndex = DA_TOLL_DIST_INDEX;
                    costIndex = DA_TOLL_COST_INDEX;
                }
            }            
            case HOV_TOLL : {
                if (timeIndex < 0) {
                    timeIndex = HOV_TOLL_TIME_INDEX;
                    distIndex = HOV_TOLL_DIST_INDEX;
                    costIndex = HOV_TOLL_COST_INDEX;
                }                                 
                double[] autoSkims = autoNonMotSkims.getAutoSkims(origin,destination,tod,false,logger);
                return new TripAttributes(autoSkims[timeIndex],autoSkims[distIndex],getCost(costIndex < 0 ? 0.0 : autoSkims[costIndex],autoSkims[distIndex]));
            }
            case HOV_NO_TOLL : {
                if (timeIndex < 0) {
                    timeIndex = HOV_TIME_INDEX;
                    distIndex = HOV_DIST_INDEX;
                    costIndex = -1;
                }                                 
                double[] autoSkims = autoNonMotSkims.getAutoSkims(origin,destination,tod,false,logger);
                return new TripAttributes(autoSkims[timeIndex],autoSkims[distIndex],getCost(costIndex < 0 ? 0.0 : autoSkims[costIndex],autoSkims[distIndex]));
            }

            case WALK :
            case BIKE : {
            	
            	double distance = 0.0;
            	double speed = modeChoice == TripModeChoice.BIKE ? DEFAULT_BIKE_SPEED : DEFAULT_WALK_SPEED;
            	
            	//If mgras are within walking distance, use the mgraToMgra file, else use auto skims distance.
            	if(mgraManager.getMgrasAreWithinWalkDistance(origin, destination)){
            		distance = mgraManager.getMgraToMgraWalkDistFrom(origin, destination);
            		return new TripAttributes((distance/5280)*60/speed,distance/5280,0);
            	} else {
            		distance = autoNonMotSkims.getAutoSkims(origin,destination,tod,false,logger)[DA_DIST_INDEX];
            		return new TripAttributes(distance*60/speed,distance,0);
            	}
                

            }
            case WALK_LB : 
            case WALK_EB :
            case WALK_BRT :
            case WALK_LRT :
            case WALK_CR :
            case DRIVE_LB :
            case DRIVE_EB :
            case DRIVE_BRT :
            case DRIVE_LRT :
            case DRIVE_CR : {
                boolean isDrive = modeChoice.isDrive;
                boolean isPremium = modeChoice.isPremium;

                double[] skims;
                int boardTaz = -1;
                int alightTaz = -1;
                double boardAccessTime = 0.0;
                double alightAccessTime = 0.0;
                boardTaz = mgraManager.getTaz(origin);
                alightTaz = mgraManager.getTaz(destination);
                if (isDrive) {
                    if (!inbound) { //outbound: drive to transit stop at origin, then transit to destination
                        int taz = tapManager.getTazForTap(boardTap);
                        boardTaz = taz;
                        int btapPosition = tazManager.getTapPosition(taz,boardTap,Modes.AccessMode.PARK_N_RIDE);
                        int atapPosition = mgraManager.getTapPosition(destination,alightTap);
                        if (atapPosition < 0 || btapPosition < 0) {
                            logger.info("bad tap position for drive access board tap");
                            logger.info("mc: " + modeChoice);
                            logger.info("origin: " + origin);
                            logger.info("dest: " + destination);
                            logger.info("board tap: " + boardTap);
                            logger.info("alight tap: " + alightTap);
                            logger.info("tod: " + tod);
                            logger.info("inbound: " + inbound);
                            logger.info("board tap position: " + btapPosition);
                            logger.info("alight tap position: " + atapPosition);
                        } else {
                            boardAccessTime = tazManager.getTapTime(taz,btapPosition,Modes.AccessMode.PARK_N_RIDE);
                            alightAccessTime = mgraManager.getMgraToTapWalkTime(destination,atapPosition);
                        }
                        skims = dtw.getDriveTransitWalkSkims(rideModeIndex,boardAccessTime,alightAccessTime,boardTap,alightTap,tod,false);
                    } else { //inbound: transit from origin to destination, then drive
                        int taz = -1;
                        try {
                            taz = tapManager.getTazForTap(alightTap);
                            alightTaz = taz;
                        } catch (NullPointerException e) {
                            logger.info("tap manager can't find taz for alight tap");
                            logger.info("mc: " + modeChoice);
                            logger.info("origin: " + origin);
                            logger.info("dest: " + destination);
                            logger.info("board tap: " + boardTap);
                            logger.info("alight tap: " + alightTap);
                            logger.info("tod: " + tod);
                            logger.info("inbound: " + inbound);
                            logger.info("a: " + tapManager.getTapParkingInfo());
                            logger.info("b: " + tapManager.getTapParkingInfo()[alightTap]);
                            logger.info("b: " + tapManager.getTapParkingInfo()[alightTap][1]);
                            logger.info("b: " + tapManager.getTapParkingInfo()[alightTap][1][0]);
                            throw e;
                        }
                        int atapPosition = tazManager.getTapPosition(taz,alightTap,Modes.AccessMode.PARK_N_RIDE);
                        int btapPosition = mgraManager.getTapPosition(origin,boardTap);
                        if (atapPosition < 0 || btapPosition < 0) {

                            logger.info("mc: " + modeChoice);
                            logger.info("origin: " + origin);
                            logger.info("dest: " + destination);
                            logger.info("board tap: " + boardTap);
                            logger.info("alight tap: " + alightTap);
                            logger.info("tod: " + tod);
                            logger.info("inbound: " + inbound);
                            logger.info("board tap position: " + btapPosition);
                            logger.info("alight tap position: " + atapPosition);
                        } else {
                            boardAccessTime = mgraManager.getMgraToTapWalkTime(origin,btapPosition);
                            alightAccessTime = tazManager.getTapTime(taz,atapPosition,Modes.AccessMode.PARK_N_RIDE);
                        }
                        skims = wtd.getWalkTransitDriveSkims(rideModeIndex,boardAccessTime,alightAccessTime,boardTap,alightTap,tod,false);
                    }
                } else {
                    int bt = mgraManager.getTapPosition(origin,boardTap);
                    int at = mgraManager.getTapPosition(destination,alightTap);
                    if (bt < 0 || at < 0) {
                        logger.info("bad tap position: " + bt + "  " + at);
                        logger.info("mc: " + modeChoice);
                        logger.info("origin: " + origin);
                        logger.info("dest: " + destination);
                        logger.info("board tap: " + boardTap);
                        logger.info("alight tap: " + alightTap);
                        logger.info("tod: " + tod);
                        logger.info("inbound: " + inbound);
                        logger.info("board tap position: " + bt);
                        logger.info("alight tap position: " + at);
                    } else {
                        boardAccessTime = mgraManager.getMgraToTapWalkTime(origin,bt);
                        alightAccessTime = mgraManager.getMgraToTapWalkTime(destination,at);
                    }
                    skims = wtw.getWalkTransitWalkSkims(rideModeIndex,boardAccessTime,alightAccessTime,boardTap,alightTap,tod,false);
                }

                double time = 0.0;
                switch (modeChoice) {
                //Fixed so that time is not being accumulated for all modes, just one per case
                    
                	case DRIVE_CR 	:
                    case WALK_CR 	: {time += skims[TRANSIT_PREM_CR_TIME_INDEX]; break;}
                    case DRIVE_LRT 	:
                    case WALK_LRT 	: {time += skims[TRANSIT_PREM_LRT_TIME_INDEX]; break;}   
                    case DRIVE_BRT 	:
                    case WALK_BRT 	: {time += skims[TRANSIT_PREM_BRT_TIME_INDEX]; break;}  
                    case DRIVE_EB 	: 
                    case WALK_EB 	: {time += skims[TRANSIT_PREM_EXPRESS_BUS_TIME_INDEX]; break;}
                    case DRIVE_LB 	:
                    case WALK_LB 	: {time += skims[isPremium ? TRANSIT_PREM_LOCAL_BUS_TIME_INDEX : TRANSIT_LOCAL_TOTTIME_INDEX]; break;}
       
					default			: throw new IllegalStateException("Should not be here: " + modeChoice);
                }
                time += skims[isPremium ? TRANSIT_PREM_ACCESS_TIME_INDEX : TRANSIT_LOCAL_ACCESS_TIME_INDEX];
                time += skims[isPremium ? TRANSIT_PREM_EGRESS_TIME_INDEX : TRANSIT_LOCAL_EGRESS_TIME_INDEX];
                time += skims[isPremium ? TRANSIT_PREM_AUX_WALK_TIME_INDEX : TRANSIT_LOCAL_AUX_WALK_TIME_INDEX];
                time += skims[isPremium ? TRANSIT_PREM_FIRST_WAIT_TIME_INDEX : TRANSIT_LOCAL_FIRST_WAIT_TIME_INDEX];
                time += skims[isPremium ? TRANSIT_PREM_TRANSFER_WAIT_TIME_INDEX : TRANSIT_LOCAL_TRANSFER_WAIT_TIME_INDEX];
                
                double dist = autoNonMotSkims.getAutoSkims(origin,destination,tod,false,logger)[DA_DIST_INDEX];  //todo: is this correct enough?
                return new TripAttributes(time,dist,skims[isPremium ? TRANSIT_PREM_FARE_INDEX : TRANSIT_LOCAL_FARE_INDEX],boardTaz,alightTaz);
            }
            default : throw new IllegalStateException("Should not be here: " + modeChoice);
        }
    }

    public static enum TripModeChoice {
        UNKNOWN(),
        DRIVE_ALONE_NO_TOLL(false),
        DRIVE_ALONE_TOLL(true),
        HOV_NO_TOLL(false),
        HOV_TOLL(true),
        WALK(),
        BIKE(),
        WALK_LB(Modes.getTransitModeIndex("LB"),false,false),
        WALK_EB(Modes.getTransitModeIndex("EB"),true,false),
        WALK_BRT(Modes.getTransitModeIndex("BRT"),true,false),
        WALK_LRT(Modes.getTransitModeIndex("LR"),true,false),
        WALK_CR(Modes.getTransitModeIndex("CR"),true,false),
        DRIVE_LB(Modes.getTransitModeIndex("LB"),false,true),
        DRIVE_EB(Modes.getTransitModeIndex("EB"),true,true),
        DRIVE_BRT(Modes.getTransitModeIndex("BRT"),true,true),
        DRIVE_LRT(Modes.getTransitModeIndex("LR"),true,true),
        DRIVE_CR(Modes.getTransitModeIndex("CR"),true,true);

        private final int rideModeIndex;
        private final boolean isPremium;
        private final boolean isDrive;
        private final boolean isToll;

        private TripModeChoice(int rideModeIndex, boolean premium, boolean drive, boolean toll) {
            this.rideModeIndex = rideModeIndex;
            isPremium = premium;
            isDrive = drive;
            isToll = toll;
        }
        
        private TripModeChoice(int rideModeIndex, boolean premium, boolean drive) {
            this(rideModeIndex,premium,drive,false);
        }                      
        
        private TripModeChoice(boolean isToll) {
            this(-1,false,false,isToll);
        }
        
        private TripModeChoice() {
            this(-1,false,false);
        }

        private int getRideModeIndex() {
            return rideModeIndex;
        }
    }

    public static class TripAttributes {
        private final float tripTime;
        private final float tripDistance;
        private final float tripCost;
        private final int tripBoardTaz;
        private final int tripAlightTaz;
        private int originTAZ;
        private int destinationTAZ;

        private String tripModeName;

        public int getTripStartTime() {
            return tripStartTime;
        }

        public void setTripStartTime(int tripStartTime) {
            this.tripStartTime = tripStartTime;
        }

        private int tripStartTime;


        public TripAttributes(double tripTime, double tripDistance, double tripCost, int tripBoardTaz, int tripAlightTaz) {
            this.tripTime = (float) tripTime;
            this.tripDistance = (float) tripDistance;
            this.tripCost = (float) tripCost;
            this.tripBoardTaz = tripBoardTaz;
            this.tripAlightTaz = tripAlightTaz;
        }

        public TripAttributes(double tripTime, double tripDistance, double tripCost) {
            this(tripTime,tripDistance,tripCost,-1,-1);
        }

        public void setTripModeName(String tripModeName) {
            this.tripModeName = tripModeName;
        }
        
        public void setOriginTAZ(int oTaz) {
            this.originTAZ = oTaz;
        }
        
        public void setDestinationTAZ(int dTaz) {
            this.destinationTAZ = dTaz;
        }

        public float getTripTime() {
            return tripTime;
        }

        public float getTripDistance() {
            return tripDistance;
        }

        public float getTripCost() {
            return tripCost;
        }

        public String getTripModeName() {
            return tripModeName;
        }

        public int getTripBoardTaz() {
            return tripBoardTaz;
        }

        public int getTripAlightTaz() {
            return tripAlightTaz;
        }
        
        public int getTripOriginTaz() {
        	return originTAZ;
        }
        
        public int getTripDestinationTaz() {
        	return destinationTAZ;
        }
    }

}
