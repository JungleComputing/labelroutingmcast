package benchmark;

import java.io.IOException;
import java.util.ArrayList;

import mcast.lrm.IbisSorter;

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
public class TestBase implements ResizeHandler {
       
    protected static int minMachines = 1;
    
    protected static int size = 32*1024;
    protected static int count = 1024;
    protected static int repeat = 10;
    
    protected static boolean autoSort = false;
    protected static boolean sortOnce = false;    
    protected static boolean ring = false;
    protected static boolean verbose = false;
        
    protected Ibis ibis;
    protected IbisIdentifier masterID;         
    
    protected ArrayList participants = new ArrayList();    
    protected boolean participantsChanged = false;
    
    protected IbisIdentifier [] destinations;
    
    protected TestBase() throws IbisException, IOException, ClassNotFoundException { 
        StaticProperties s = new StaticProperties();
        s.add("Serialization", "object");
        s.add("Communication", "ManyToOne, Reliable, ExplicitReceipt");
        s.add("Worldmodel", "open");

        ibis = Ibis.createIbis(s, this);
        
        System.err.println("Ibis created!");
                
        ibis.enableResizeUpcalls();
    }
    
    protected void waitForEnoughMachines() { 
        
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
    }
    
    protected void done() throws IOException { 
        ibis.end();
    }
    
    protected synchronized IbisIdentifier [] getParticipants() {     
        
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
                if (ring) {                 
                    IbisSorter.sort(ibis.identifier(), destinations, 0, 
                            destinations.length-1);
                } else { 
                    IbisSorter.sort(ibis.identifier(), destinations);
                }                
            }
            
            if (ring) { 
                destinations[index] = ibis.identifier();
            }    

            if (verbose) {
                System.err.println("Sending from " + ibis.identifier().name() + " / "
                        + ibis.identifier().cluster() + " to ");
                
                for (int i=0;i<destinations.length;i++) { 
                    System.err.println( destinations[i].name() + " / "
                            + destinations[i].cluster());                         
                }
            }
        } 
        
        participantsChanged = false;          
        return destinations;
    } 
        
    protected synchronized void waitForMaster() { 
        
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
        
    protected synchronized void waitForMasterToLeave() { 
        
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
        left(id);
    }

    public void mustLeave(IbisIdentifier[] id) {
        // ignored
    }
    
    public static void parseOptions(String [] args) {
               
        for (int i=0;i<args.length;i++) {
            
            if (args[i].equals("-machines")) {                
                minMachines = Integer.parseInt(args[++i]);
                args[i] = args[i-1] = null;
            } else if (args[i].equals("-count")) { 
                count = Integer.parseInt(args[++i]);
                args[i] = args[i-1] = null;
            } else if (args[i].equals("-repeat")) {
                repeat = Integer.parseInt(args[++i]);
                args[i] = args[i-1] = null;
            } else if (args[i].equals("-size")) {
                size = Integer.parseInt(args[++i]);
                args[i] = args[i-1] = null;
            } else if (args[i].equals("-sortAuto")) {
                autoSort = true;
                args[i] = null;
            } else if (args[i].equals("-sortOnce")) {
                sortOnce = true;    
                args[i] = null;
            } else if (args[i].equals("-ring")) {
                ring = true;   
                args[i] = null;
            } else if (args[i].equals("-verbose")) {
                verbose = true;   
                args[i] = null;
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
    }
}
