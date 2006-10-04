package benchmark;

import ibis.ipl.IbisException;
import ibis.ipl.IbisIdentifier;

import java.io.IOException;

import mcast.lrm.Message;
import mcast.lrm.MessageCache;
import mcast.lrm.MessageReceiver;
import mcast.lrm.LableRoutingMulticast;

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
public class Test1 extends TestBase implements MessageReceiver {
       
    private int receivedMessages = 0;    
        
    private LableRoutingMulticast lrmcast;
        
    private MessageCache cache;
    
    private byte [] data;
        
    private Test1() throws IbisException, IOException, ClassNotFoundException {      
        super();
    } 
    
    public void init() throws IOException, IbisException {
        cache = new MessageCache(cacheSize, 8*1024);        
        lrmcast = new LableRoutingMulticast(ibis, this, cache, autoSort, "test");        
    }
    
    private void start() throws IOException { 

        waitForMaster();        
        
        if (masterID.equals(ibis.identifier())) {         
     
            waitForEnoughMachines();
            
            System.err.println("Starting test");
    
            data = new byte[size];
                       
            for (int i=0;i<repeat;i++) {                 
                runTest();
            } 
        } else { 
            // Wait for the master to leave (means the application is done)
            waitForMasterToLeave();            
        }
        
        System.out.println("Machine " + ibis.identifier() + " received " 
                + receivedMessages + " messages");
        
        lrmcast.done();

        done();
    }

    public void gotDone(int id) {
    }
            
    private void runTest() { 

        IbisIdentifier [] ids = getParticipants(true);
               
        receivedMessages = 0;
    
        if (ids != null) {         
            System.out.println("Running test with " + ids.length + " machines.");        
            lrmcast.setDestination(ids);
        } 
            
        long start = System.currentTimeMillis();

        for (int i=0;i<count;i++) { 
            lrmcast.send(0, 0, data, 0, data.length);
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

        long bytes = lrmcast.getBytes(true);
        
        long time = end-start;
        double tp = ((count*size)/(1024.0*1024.0))/(time/1000.0);
        double rtp = ((bytes)/(1024.0*1024.0))/(time/1000.0);
        long overhead = (bytes-(count*size))/count;        
        
        System.out.println("Test took " + time + " ms. TP = " + tp + " MB/s (" 
                + rtp + " MB/s, overhead = " + overhead + " per message)");
    }
        
    public synchronized boolean gotMessage(Message b) { 

        receivedMessages++;
                
        if (ring && receivedMessages == count) { 
            notifyAll();
        }
        
        cache.put(b);
        
        return true;
    }
    
    public void addIbis(IbisIdentifier id) {
        lrmcast.addIbis(id);
    }
    
    public void removeIbis(IbisIdentifier id) {
        lrmcast.removeIbis(id);
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
