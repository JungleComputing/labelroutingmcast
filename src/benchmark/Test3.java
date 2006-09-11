package benchmark;

import java.io.IOException;

import mcast.object.ObjectMulticaster;

import ibis.ipl.*;

/**
 * 
 * In this test a single sender sends an object in a chain using the 
 * ObjectMulticaster. The test waits until the specified number of machines is 
 * reached. It can only handle machines joining at the start, but not 
 * leaving/crashing. It does not send to itself.      
 *  
 * @author Jason Maassen
 * @version 1.0 May 9, 2006
 * @since 1.0
 */
public class Test3 extends TestBase {
       
    private ObjectMulticaster omc;
    
    private DoubleData data;
    
    private Test3() throws IbisException, IOException, ClassNotFoundException {         
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
            
            // Create the data object
            data = new DoubleData(size);                        

            waitForEnoughMachines();
            
            System.err.println("Starting test");
            
            for (int i=0;i<repeat;i++) {                 
                runSender();
            } 
        } else {
            for (int i=0;i<repeat;i++) {                 
                runReceiver();
            } 
        }
        
        omc.done();
        done();
    }
               
    private void runSender() throws IOException, ClassNotFoundException { 

        IbisIdentifier [] ids = getParticipants(false);
        
        System.err.println("Running test with " + ids.length + " machines.");
        
        for (int i=0;i<ids.length;i++) { 
            System.err.println("   " + ids[i]);                            
        }
                           
        long start = System.currentTimeMillis();
        
        for (int i=0;i<count;i++) { 
            omc.send(ids, data);
        } 
        
        long end = System.currentTimeMillis();

        long time = end-start;
        double tp = ((count*data.getSize())/(1024.0*1024.0))/(time/1000.0);
        
        System.err.println("Sender took " + time + " ms. TP = " + tp + " MB/s.");
    }
    
    private void runReceiver() throws IOException, ClassNotFoundException { 

        long size = 0;
        
        long start = System.currentTimeMillis();
        
        for (int c=0;c<count;c++) {
            DoubleData dd = (DoubleData) omc.receive();
            size += dd.getSize();
        }
        
        long end = System.currentTimeMillis();
        
        long time = end-start;
        double tp = (size/(1024.0*1024.0))/(time/1000.0);
        
        System.err.println("Receiver took " + time + " ms. TP = " + tp + " MB/s.");
    }
    
    public static void main(String [] args) {
               
        parseOptions(args);        
        
        try { 
            new Test3().start();
        } catch (Exception e) {
            System.err.println("Oops: " + e);
            e.printStackTrace(System.err);
        }
    }
}
