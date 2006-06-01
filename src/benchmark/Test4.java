package benchmark;

import java.io.IOException;

import mcast.object.ObjectMulticaster;

import ibis.ipl.*;

/**
 * 
 * In this test a single sender sends an object in a chain using the 
 * ObjectMulticaster. The test waits until the specified number of machines is 
 * reached. It can handle machines joining/leaving and crashing (except for a 
 * crash of the master). It does not send to itself.      
 *  
 * @author Jason Maassen
 * @version 1.0 May 9, 2006
 * @since 1.0
 */
public class Test4 extends TestBase {
       
    private static boolean verbose = false;
            
    private ObjectMulticaster omc;
    
    private DoubleData data;
    
    private Test4() throws IbisException, IOException, ClassNotFoundException { 
        
        super();
        omc = new ObjectMulticaster(ibis);
    }
    
    private void start() throws IOException, ClassNotFoundException { 

        waitForMaster();        
        
        if (masterID.equals(ibis.identifier())) {         
            
            // Create the data object
            data = new DoubleData(size);                        
            
            System.err.println("Starting test");
            
            // Run for 'repeat' iterations 
            for (int i=0;i<repeat;i++) {                
                data.iteration = i;                
                runSender();
            } 
        } else {            
            // This one may start halfway, so just run until we see the last 
            // iteration number come by. 
            while (runReceiver());
        }
        
        omc.done();
        done();
    }
              
    private void runSender() throws IOException, ClassNotFoundException { 

        long size = 0;
        
        IbisIdentifier [] ids = getParticipants();
                
        System.err.println("Multicasting to " + ids.length + " machines.");
        
        if (verbose) { 
            for (int i=0;i<ids.length;i++) { 
                System.err.println("   " + ids[i]);                            
            }
        } 
        
        long start = System.currentTimeMillis();
        
        for (int i=0;i<count;i++) { 
            size += omc.send(ids, data);
        } 
        
        long end = System.currentTimeMillis();

        long time = end-start;
        double tp = (size/(1024.0*1024.0))/(time/1000.0);
        
        System.err.println(" sending took " + time + " ms. TP = " + tp + " MB/s.");
    }
    
    private boolean runReceiver() throws IOException, ClassNotFoundException { 

        DoubleData dd = null;
        long size = 0;
        
        long start = System.currentTimeMillis();
        
        for (int c=0;c<count;c++) {
            try { 
                dd = (DoubleData) omc.receive();
                size += dd.getSize();
            } catch (Exception e) { 
                System.err.println("Receive failed: " + e);
            }
        }
        
        long end = System.currentTimeMillis();
        
        if (verbose) { 
            long time = end-start;
            double tp = (size/(1024.0*1024.0))/(time/1000.0);
        
            System.err.println(" receiving took " + time + " ms. TP = " 
                    + tp + " MB/s.");
        } 
        
        return (dd.iteration < repeat);     
    }
               
    public static void main(String [] args) {
               
        parseOptions(args);
        
        if (ring) { 
            System.err.println("Ring not supported in this test");
            System.exit(1);
        }
        
        try { 
            new Test4().start();
        } catch (Exception e) {
            System.err.println("Oops: " + e);
            e.printStackTrace(System.err);
        }
    }
}
