package com.pb.mtctm2.abm.accessibilities;

import java.util.concurrent.ConcurrentHashMap;
import java.util.HashMap;


public class StoredUtilityData
{

    private static StoredUtilityData objInstance = null;

    // these arrays are shared by multiple BestTransitPathCalculator objects in a distributed computing environment
    private float[][][] storedWalkAccessUtils;
    private float[][][] storedDriveAccessUtils;
    private float[][][] storedWalkEgressUtils;
    private float[][][] storedDriveEgressUtils;
    
    private HashMap<Integer,HashMap<Integer,ConcurrentHashMap<Long,Float[]>>> storedDepartPeriodTapTapUtils;
    
    
    
    private StoredUtilityData(){
    }
    
    public static synchronized StoredUtilityData getInstance( int maxMgra, int maxTap, int maxTaz, int[] accEgrSegments, int[] periods)
    {
        if (objInstance == null) {
            objInstance = new StoredUtilityData();
            objInstance.setupStoredDataArrays( maxMgra, maxTap, maxTaz, accEgrSegments, periods);
            return objInstance;
        }
        else {
            return objInstance;
        }
    }    
    
    private void setupStoredDataArrays( int maxMgra, int maxTap, int maxTaz, int[] accEgrSegments, int[] periods){        
        storedWalkAccessUtils = new float[maxMgra + 1][maxTap + 1][];
        storedDriveAccessUtils = new float[maxTaz + 1][maxTap + 1][];
        storedWalkEgressUtils = new float[maxTap + 1][maxMgra + 1][];
        storedDriveEgressUtils = new float[maxTap + 1][maxTaz + 1][];
        
        //put into concurrent hashmap
        storedDepartPeriodTapTapUtils = new HashMap<Integer,HashMap<Integer,ConcurrentHashMap<Long,Float[]>>>();
        for(int i=0; i<accEgrSegments.length; i++) {
        	storedDepartPeriodTapTapUtils.put(accEgrSegments[i], new HashMap<Integer,ConcurrentHashMap<Long,Float[]>>());
        	for(int j=0; j<periods.length; j++) {
        		HashMap<Integer,ConcurrentHashMap<Long,Float[]>> hm = storedDepartPeriodTapTapUtils.get(accEgrSegments[i]);
        		hm.put(periods[j], new ConcurrentHashMap<Long,Float[]>()); //key method paTapKey below
        	}
    	}        
    }
    
    public float[][][] getStoredWalkAccessUtils() {
        return storedWalkAccessUtils;
    }
    
    public float[][][] getStoredDriveAccessUtils() {
        return storedDriveAccessUtils;
    }
    
    public float[][][] getStoredWalkEgressUtils() {
        return storedWalkEgressUtils;
    }
    
    public float[][][] getStoredDriveEgressUtils() {
        return storedDriveEgressUtils;
    }
    
    public HashMap<Integer,HashMap<Integer,ConcurrentHashMap<Long,Float[]>>> getStoredDepartPeriodTapTapUtils() {
        return storedDepartPeriodTapTapUtils;
    }
    
    //create p to a hash key - up to 99,999 
    public long paTapKey(int p, int a) {
    	return(p * 100000 + a);
    }
    
    //convert double array to float array
    public float[] d2f(double[] d) {
    	float[] f = new float[d.length];
    	for(int i=0; i<d.length; i++) {
    		f[i] = (float)d[i];
    	}
    	return(f);
    }
    
    //convert double array to Float array
    public Float[] d2F(double[] d) {
    	Float[] F = new Float[d.length];
    	for(int i=0; i<d.length; i++) {
    		F[i] = new Float(d[i]);
    	}
    	return(F);
    }
    
}
