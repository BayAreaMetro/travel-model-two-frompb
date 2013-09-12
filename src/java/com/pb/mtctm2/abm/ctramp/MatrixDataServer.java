package com.pb.mtctm2.abm.ctramp;

import gnu.cajo.invoke.Remote;
import gnu.cajo.utils.ItemServer;
import org.apache.log4j.Logger;
import java.io.File;
import java.io.Serializable;
import java.util.HashMap;
import com.pb.common.calculator.DataEntry;
import com.pb.common.calculator.MatrixDataServerIf;
import com.pb.common.matrix.Matrix;
import com.pb.common.matrix.MatrixIO32BitJvm;
import com.pb.common.matrix.MatrixType;
import com.pb.common.matrix.MatrixReader;
import com.pb.common.matrix.MatrixWriter;

/**
 * @author Jim Hicks
 * 
 *         Class for managing matrix data in a remote process and accessed by UECs
 *         using RMI.
 */
public class MatrixDataServer
        implements MatrixDataServerIf, Serializable
{

    protected transient Logger            logger                     = Logger.getLogger(MatrixDataServer.class);

    private Object objectLock;
    
    private static final String        VERSION                    = "2.2";

    // These are used if the server is started manually by running this class's
    // main(). If so, these must be defined consistently with
    // any class that acts as a client to the server, i.e. the client must know the
    // address and port as well.
    private static final String        MATRIX_DATA_SERVER_ADDRESS = "127.0.0.1";
    private static final int           MATRIX_DATA_SERVER_PORT    = 1171;
    public static final String         MATRIX_DATA_SERVER_NAME    = MatrixDataServer.class.getCanonicalName();
    private static final String        MATRIX_DATA_SERVER_LABEL   = "matrix server";

    private MatrixIO32BitJvm           ioVm32Bit                  = null;

    private HashMap<String, DataEntry> matrixEntryMap;
    private HashMap<String, Matrix>    matrixMap;
    
    private int ramFor32BitProcess;

    public MatrixDataServer()
    {

        // start the 32 bit JVM used specifically for running matrix io classes
        ioVm32Bit = MatrixIO32BitJvm.getInstance();

        // create the HashMap objects to keep track of matrix data read by the server
        matrixEntryMap = new HashMap<String, DataEntry>();
        matrixMap = new HashMap<String, Matrix>();
        
        objectLock = new Object();
    }

    public String testRemote( String remoteObjectName )
    {
        logger.info("testRemote() called by remote process: " + remoteObjectName + "." );
        return String.format("testRemote() method in %s called by %s.", this.getClass().getCanonicalName(), remoteObjectName);
    }

    public String testRemote()
    {
        logger.info("testRemote() called by remote process." );
        return String.format("testRemote() method in %s called.", this.getClass().getCanonicalName() );
    }

    /*
     * Read a matrix.
     * @param matrixEntry a DataEntry describing the matrix to read
     * @return a Matrix
     */
    public Matrix getMatrix(DataEntry matrixEntry)
    {

        Matrix matrix;
        
        synchronized( objectLock ) {
            
            String name = matrixEntry.name;

            if ( matrixEntryMap.containsKey( name ) ) {
                matrix = matrixMap.get( name );
            }
            else {
                String fileName = matrixEntry.fileName;
                if (matrixEntry.format.equalsIgnoreCase("emme2")) {
                    MatrixReader mr = MatrixReader.createReader(MatrixType.EMME2, new File(fileName));
                    matrix = mr.readMatrix(matrixEntry.matrixName);
                }
                else if (matrixEntry.format.equalsIgnoreCase("binary")) {
                    MatrixReader mr = MatrixReader.createReader(MatrixType.BINARY, new File(fileName));
                    matrix = mr.readMatrix();
                }
                else if (matrixEntry.format.equalsIgnoreCase("zip") || matrixEntry.format.equalsIgnoreCase("zmx")) {
                    MatrixReader mr = MatrixReader.createReader(MatrixType.ZIP, new File(fileName));
                    matrix = mr.readMatrix();
                }
                else if (matrixEntry.format.equalsIgnoreCase("tpplus")) {
                    MatrixReader mr = MatrixReader.createReader(MatrixType.TPPLUS, new File(fileName));
                    matrix = mr.readMatrix(matrixEntry.matrixName);
                }
                else if (matrixEntry.format.equalsIgnoreCase("transcad")) {
                    MatrixReader mr = MatrixReader.createReader(MatrixType.TRANSCAD, new File(fileName));
                    matrix = mr.readMatrix(matrixEntry.matrixName);
                }
                else {
                    throw new RuntimeException("unsupported matrix type: " + matrixEntry.format);
                }

                // Use token name from control file for matrix name (not name from underlying matrix)
                matrix.setName(matrixEntry.name);

                matrixMap.put ( name, matrix );
                matrixEntryMap.put( name, matrixEntry );
            }
                        
        }
        
        return matrix;
    }

    /**
     * Utility method to write a set of matrices to disk.
     * 
     * @param fileName The file name to write to.
     * @param m  An array of matrices
     */
    public void writeMatrixFile(String fileName, Matrix[] m){
             
        File outFile = new File(fileName); 
        MatrixWriter writer = MatrixWriter.createWriter(MatrixType.TPPLUS, outFile ); 
        String[] names = new String[m.length];
        
        for (int i=0; i<m.length; i++) {
            names[i] = m[i].getName(); 
        }

        writer.writeMatrices(names, m); 
        
    }
        
    public void clear()
    {
        if (matrixMap != null) matrixMap.clear();

        if (matrixEntryMap != null) matrixEntryMap.clear();
    }

    public void start32BitMatrixIoServer(MatrixType mType)
    {

        // start the matrix I/O server process
    	ioVm32Bit.setSizeInMegaBytes( ramFor32BitProcess );
        ioVm32Bit.startJVM32();

        // establish that matrix reader and writer classes will use the RMI versions
        // for TPPLUS format matrices
        ioVm32Bit.startMatrixDataServer(mType);
        logger.info("matrix data server 32 bit process started.");

    }

    public void stop32BitMatrixIoServer()
    {

        // stop the matrix I/O server process
        ioVm32Bit.stopMatrixDataServer();

        // close the JVM in which the RMI reader/writer classes were running
        ioVm32Bit.stopJVM32();
        logger.info("matrix data server 32 bit process stopped.");

    }
    
    public void setRam( int ram ){
        ramFor32BitProcess = ram;
    }

    

    public static void main(String args[]) throws Exception
    {

        String serverAddress = MATRIX_DATA_SERVER_ADDRESS;
        int serverPort = MATRIX_DATA_SERVER_PORT;
        String className = MATRIX_DATA_SERVER_NAME;
        String serverLabel = MATRIX_DATA_SERVER_LABEL;
        int ram = -1;
        
        for (int i = 0; i < args.length; i++)
        {
            if (args[i].equalsIgnoreCase("-hostname")) serverAddress = args[i + 1];
            else if (args[i].equalsIgnoreCase("-port")) serverPort = Integer.parseInt(args[i + 1]);
            else if (args[i].equalsIgnoreCase("-label")) serverLabel = args[i + 1];
            else if (args[i].equalsIgnoreCase("-ram")) ram = Integer.parseInt(args[i + 1]);
        }

        MatrixDataServer matrixServer = new MatrixDataServer();
        matrixServer.setRam( ram );

        try
        {

            // create the concrete data server object
            matrixServer.start32BitMatrixIoServer(MatrixType.TPPLUS);

        } catch (RuntimeException e)
        {
            matrixServer.stop32BitMatrixIoServer();
            System.out.println(  "RuntimeException caught in com.pb.models.ctramp.MatrixDataServer.main() -- exiting." );
            e.printStackTrace();
        }

        // bind this concrete object with the cajo library objects for managing RMI
        Remote.config(serverAddress, serverPort, null, 0);
        ItemServer.bind(matrixServer, className);

        // log that the server started
        System.out.println( "server starting on " + (System.getProperty("os.arch")) + " operating system." );
        System.out.println(String.format("%s version %s started on: %s:%d", serverLabel, VERSION,
                serverAddress, serverPort));

    }

}