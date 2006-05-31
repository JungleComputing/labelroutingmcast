package benchmark;

import ibis.ipl.IbisException;
import ibis.ipl.IbisIdentifier;

import java.io.IOException;

import lrmcast.ByteArrayReceiver;
import lrmcast.LableRoutingMulticast;

/**
 * In this test a single sender sends a byte[] using the LableRoutingMulticast.
 * The test waits until a specified number of machines is participating 
 * 
 * It can handle machines joining/leaving, but not crashing. It is capable of 
 * performing a chained or ring multicast. 
 *
 * The destinations can also be sorted to make the whole thing SMP/cluster aware. 
 *  
 * @author Jason Maassen
 * @version 1.0 May 9, 2006
 * @since 1.0
 */
public class Test1 extends TestBase implements ByteArrayReceiver {
       
    private int receivedMessages = 0;
    
    private LableRoutingMulticast lrmcast;
        
    private Test1() throws IbisException, IOException, ClassNotFoundException {      
        super();        
        lrmcast = new LableRoutingMulticast(ibis, this, autoSort);        
    }
    
    private void start() throws IOException { 

        waitForMaster();        
        
        if (masterID.equals(ibis.identifier())) {         
     
            waitForEnoughMachines();
            
            System.err.println("Starting test");
            
            for (int i=0;i<repeat;i++) {                 
                runTest();
            } 
        } else { 
            // Wait for the master to leave (means the application is done)
            waitForMasterToLeave();            
        }
        
        lrmcast.done();

        done();
    }
            
    private void runTest() { 

        IbisIdentifier [] ids = getParticipants();
               
        receivedMessages = 0;
        
        byte [] data = new byte[size];
        
        System.out.println("Running test with " + ids.length + " machines.");        
        
        long start = System.currentTimeMillis();
        
        for (int i=0;i<count;i++) { 
            lrmcast.send(ids, 0, data);        
        }         
        
        if (ring) {
            // Wait for my own messages to appear
            synchronized (this) {
                while (receivedMessages != count) { 
                    try { 
                        wait();                    
                    } catch (InterruptedException e) {
                        // ignore
                    }
                }
            }
        } 
        
        long end = System.currentTimeMillis();

        long time = end-start;
        double tp = ((count*size)/(1024.0*1024.0))/(time/1000.0);
        
        System.out.println("Test took " + time + " ms. TP = " + tp + " MB/s.");
    }
        
    public synchronized boolean gotMessage(String sender, int id, 
            byte[] message) {
        
        receivedMessages++;
        
        if (ring && receivedMessages == count) { 
            notifyAll();
        }
        
        return true;
    }
    
    public static void main(String [] args) {
            
        parseOptions(args);
       
        try { 
            new Test1().start();
        } catch (Exception e) {
            System.err.println("Oops: " + e);
            e.printStackTrace(System.err);
        }
    }
}
