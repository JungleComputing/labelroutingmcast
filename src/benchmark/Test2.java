package benchmark;

import ibis.ipl.IbisException;
import ibis.ipl.IbisIdentifier;

import java.io.IOException;

import mcast.object.ObjectMulticaster;

/**
 * 
 * This test sends a single sender sends a byte [] using the ObjectMulticaster. 
 * The test waits until the specified number of machines is reached. It can only 
 * handle machines joining at the start, but not leaving/crashing. It optionally 
 * inserts itself as the last destination, and waits for the data to return.     
 *  
 * @author Jason Maassen
 * @version 1.0 May 9, 2006
 * @since 1.0
 */
public class Test2 extends TestBase {
       
    private ObjectMulticaster omc;
    
    private byte [] data;
        
    private Test2() throws IbisException, IOException, ClassNotFoundException {         
        super();
    } 
    
    public void init() throws IOException, IbisException {         
        omc = new ObjectMulticaster(ibis, autoSort, signal, "test");
    }
    
    public void addIbis(IbisIdentifier id) {
        omc.addIbis(id);
    }
    
    public void removeIbis(IbisIdentifier id) {
        omc.removeIbis(id);
    }
    
    private void start() throws IOException, ClassNotFoundException { 

        waitForMaster();        
        
        if (masterID.equals(ibis.identifier())) {         
            
            waitForEnoughMachines();
            
            System.err.println("Starting test");
            
            data = new byte[size];            
            
            for (int i=0;i<repeat;i++) {                 
                runTest();
            } 
        } else { 
            for (int i=0;i<repeat;i++) {
                for (int c=0;c<count;c++) {
                    omc.receive();
                } 
            }
        }
        
        omc.done();
        done();
    }
           
    private void runTest() throws IOException, ClassNotFoundException { 

        IbisIdentifier [] ids = getParticipants(true);
        
        if (ids != null) {         
            System.err.println("Running test with " + ids.length + " machines.");

            for (int i=0;i<ids.length;i++) { 
                System.err.println("   " + ids[i]);                            
            }
                
            omc.setDestination(ids);
        }
            
                
        long start = System.currentTimeMillis();
        
        for (int i=0;i<count;i++) { 
            omc.send(data);

            if (ring) { 
                omc.receive();
            } 
        } 
        
        long end = System.currentTimeMillis();

        long time = end-start;
        double tp = ((count*size)/(1024.0*1024.0))/(time/1000.0);
        
        System.err.println("Test took " + time + " ms. TP = " + tp + " MB/s.");
    }
        
    public static void main(String [] args) {

        parseOptions(args);
              
        try { 
            new Test2().start();
        } catch (Exception e) {
            System.err.println("Oops: " + e);
            e.printStackTrace(System.err);
        }
    }
}
