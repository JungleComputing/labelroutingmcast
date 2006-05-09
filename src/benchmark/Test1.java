package benchmark;

import java.io.IOException;
import java.util.ArrayList;

import lrmcast.IbisSorter;
import lrmcast.LableRoutingMulticast;
import lrmcast.ByteArrayReceiver;

import ibis.ipl.*;

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
public class Test1 implements ResizeHandler, ByteArrayReceiver {
       
    private static int size = 32*1024;
    private static int count = 1024;
    private static int repeat = 10;
    private static int minMachines = 1;   
    
    private static boolean autoSort = false;
    private static boolean sortOnce = false;    
    
    private static boolean ring = false;
        
    private int receivedMessages = 0;
    
    private Ibis ibis;
    private LableRoutingMulticast lrmcast;
    private IbisIdentifier masterID;         
    
    private ArrayList participants = new ArrayList();    
    private boolean participantsChanged = false;
    
    private IbisIdentifier [] destinations;
    
    private Test1() throws IbisException, IOException, ClassNotFoundException { 
        StaticProperties s = new StaticProperties();
        s.add("Serialization", "object");
        s.add("Communication", "ManyToOne, Reliable, ExplicitReceipt");
        s.add("Worldmodel", "open");

        ibis = Ibis.createIbis(s, this);
        
        System.err.println("Ibis created!");
        
        lrmcast = new LableRoutingMulticast(ibis, this, autoSort);
        
        ibis.enableResizeUpcalls();
    }
    
    private void start() throws IOException { 

        waitForMaster();        
        
        if (masterID.equals(ibis.identifier())) {         
            
            boolean enoughMachines = false;
            
            while (!enoughMachines) {
                System.out.println("Waiting for " + minMachines + " to arrive");
                
                try {                 
                    Thread.sleep(1000);                
                } catch (Exception e) {
                    // ignore        
                }
                
                synchronized (this) {
                    enoughMachines = participants.size() >= minMachines; 
                }
            } 
            
            System.err.println("Starting test");
            
            for (int i=0;i<repeat;i++) {                 
                runTest();
            } 
        } else { 
            // Wait for the master to leave (means the application is done)
            waitForMasterToLeave();            
        }
        
        lrmcast.done();
        ibis.end();
    }
    
    private synchronized IbisIdentifier [] getParticipants() {     
        
        if (destinations == null || participantsChanged) {
            
            int size = participants.size()-1;
            
            if (ring) { 
                size++;
            }
            
            destinations = new IbisIdentifier[size];
        
            // Skip my own ID
            int index = 0;
            
            for (int i=0;i<participants.size();i++) {
                
                IbisIdentifier tmp = (IbisIdentifier) participants.get(i);
                
                if (!tmp.equals(ibis.identifier())) { 
                    destinations[index++] = tmp;
                } 
            }
            
            if (sortOnce) { 
                IbisSorter.sort(ibis.identifier(), destinations);
            }
            
            if (ring) { 
                destinations[index] = ibis.identifier();
            }            
        } 
        
        participantsChanged = false;          
        return destinations;
    } 
        
    private void runTest() { 

        IbisIdentifier [] ids = getParticipants();
               
        receivedMessages = 0;
        
        byte [] data = new byte[size];
        
        System.out.println("Running test with " + ids.length + " machines.");        
        
        long start = System.currentTimeMillis();
        
        for (int i=0;i<count;i++) { 
            lrmcast.send(ids, data);        
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
    
    private synchronized void waitForMaster() { 
        
        System.err.println("Waiting for master to arrive!");
        
        while (masterID == null) { 
            try { 
                wait();
            } catch (Exception e) {
                // ignore
            }
        }        
        
        System.err.println("Master to arrived " + masterID);
    }
        
    private synchronized void waitForMasterToLeave() { 
        
        while (masterID != null) { 
            try { 
                wait();
            } catch (Exception e) {
                // ignore
            }
        }        
        
        System.err.println("Got master " + masterID);
    }
    
    public synchronized void joined(IbisIdentifier id) {
        
        System.err.println("Join " + id);
        
        participants.add(id);
        
        if (participants.size() == 1) { 
            // the first one will be the master
            masterID = id;
            notifyAll();
            
            System.err.println("Master is " + id);
        }
        
        participantsChanged = true;        
    }

    public synchronized void left(IbisIdentifier id) {
        participants.remove(id);
        
        if (id.equals(masterID)) { 
            masterID = null;
            notifyAll();
        }
        
        participantsChanged = true;
    }

    public synchronized void died(IbisIdentifier id) {        
        // ignored
    }

    public void mustLeave(IbisIdentifier[] arg0) {
        // ignored        
    }

    public synchronized void gotMessage(IbisIdentifier sender, byte[] message) {
        receivedMessages++;
        
        if (ring && receivedMessages == count) { 
            notifyAll();
        }
    }
    
    public static void main(String [] args) {
               
        for (int i=0;i<args.length;i++) {
            
            if (args[i].equals("-count")) { 
                count = Integer.parseInt(args[++i]);                
            } else if (args[i].equals("-repeat")) {
                repeat = Integer.parseInt(args[++i]);                
            } else if (args[i].equals("-size")) {
                size = Integer.parseInt(args[++i]);
            } else if (args[i].equals("-machines")) {
                minMachines = Integer.parseInt(args[++i]);
            } else if (args[i].equals("-sortOnce")) {
                sortOnce = true;    
            } else if (args[i].equals("-sortAuto")) {
                autoSort = true;
            } else if (args[i].equals("-ring")) {
                ring = true;    
            } else { 
                System.err.println("Unknown option " + args[i]);
                System.exit(1);
            }
        }
        
        if (ring && autoSort) { 
            System.err.println("Cannot send in a ring AND auto sort the " +
                    "destinations!");
            System.exit(1);
        }
        
        if (sortOnce && autoSort) { 
            System.err.println("Cannot sort in two different ways!!");
            System.exit(1);
        }

        try { 
            new Test1().start();
        } catch (Exception e) {
            System.err.println("Oops: " + e);
            e.printStackTrace(System.err);
        }
    }
}
