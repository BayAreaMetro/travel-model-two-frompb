package com.pb.mtctm2.abm.visitor;

import org.apache.log4j.Logger;
import com.pb.mtctm2.abm.application.SandagModelStructure;

public class VisitorModelStructure extends SandagModelStructure{
	
	public final static byte NUMBER_VISITOR_PURPOSES = 6;
	public final static byte WORK    = 0;
	public final static byte RECREATION  = 1;
	public final static byte OTHER   = 2;
	
	public final static String[] VISITOR_PURPOSES = {"WORK","RECREATE","OTHER"};
	
	//override on max tour mode, since we have taxi in this model.
	public static final int      MAXIMUM_TOUR_MODE_ALT_INDEX                                        = 27;
	
	public final static byte NUMBER_VISITOR_SEGMENTS = 2;
	public final static byte BUSINESS = 0;
	public final static byte PERSONAL = 1;
	
	public final static String[] VISITOR_SEGMENTS = {"BUSINESS","PERSONAL"};
	public final static byte DEPARTURE = 0;
	public final static byte ARRIVAL = 1;

	public final static byte INCOME_SEGMENTS = 5;
	
	//note that time periods start at 1 and go to 40
	public static final byte TIME_PERIODS = 40;

    public static final int      AM                                                                 = 0;
    public static final int      PM                                                                 = 1;
    public static final int      OP                                                                 = 2;
    public static final int[]    SKIM_PERIODS                                                       = {
            AM, PM, OP                                                                              };
    public static final String[] SKIM_PERIOD_STRINGS                                                = {
            "AM", "PM", "OP"                                                                        };
    public static final int UPPER_EA = 3;
    public static final int UPPER_AM = 9;
    public static final int UPPER_MD = 23;
    public static final int UPPER_PM = 29;
    public static final int UPPER_EV = 35; //19:00-22:00
    
    public static final String[] MODEL_PERIOD_LABELS = { "EA", "AM", "MD", "PM", "EV" };

    public final static byte TAXI = 27;
	
    /**
     * Taxi tour mode
     * 
     * @param tourMode
     * @return
     */
    public boolean getTourModeIsTaxi(int tourMode){
    	
    	if(tourMode == TAXI)
    		return true;
    	else 
    		return false;
    		
    }

    /**
     * return the Skim period index 0=am, 1=pm, 2=off-peak
     */
    public static int getSkimPeriodIndex(int departPeriod)
    {

        int skimPeriodIndex = 0;

        if (departPeriod <= UPPER_EA) skimPeriodIndex = OP;
        else if (departPeriod <= UPPER_AM) skimPeriodIndex = AM;
        else if (departPeriod <= UPPER_MD) skimPeriodIndex = OP;
        else if (departPeriod <= UPPER_PM) skimPeriodIndex = PM;
        else skimPeriodIndex = OP;

        return skimPeriodIndex;

    }

    /**
     * return the Model period index 0=EA, 1=AM, 2=MD, 3=PM, 4=EV
     */
    public static int getModelPeriodIndex(int departPeriod)
    {

        int modelPeriodIndex = 0;

        if (departPeriod <= UPPER_EA) modelPeriodIndex = 0;
        else if (departPeriod <= UPPER_AM) modelPeriodIndex = 1;
        else if (departPeriod <= UPPER_MD) modelPeriodIndex = 2;
        else if (departPeriod <= UPPER_PM) modelPeriodIndex = 3;
        else modelPeriodIndex = 4;

        return modelPeriodIndex;

    }

 

}

