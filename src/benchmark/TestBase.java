package benchmark;

import java.io.IOException;
import java.util.ArrayList;

import mcast.util.IbisSorter;

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
public abstract class TestBase implements RegistryEventHandler {

    protected static int minMachines = 1;
    
    protected static int size = 32*1024;
    protected static int count = 1024;
    protected static int repeat = 10;
    protected static int cacheSize = 250;
        
    protected static boolean autoSort = false;
    protected static boolean sortOnce = false;    
    protected static boolean ring = false;
    protected static boolean verbose = false;        
    protected static boolean signal = false;
        
    protected Ibis ibis;
    protected IbisIdentifier masterID;         
    
    protected ArrayList<IbisIdentifier> participants
            = new ArrayList<IbisIdentifier>();    
    protected boolean participantsChanged = false;
    
    protected IbisIdentifier [] destinations;
    
    protected TestBase() throws IOException, ClassNotFoundException { 
        PortType tp = new PortType(
            PortType.SERIALIZATION_DATA, PortType.COMMUNICATION_RELIABLE,
            PortType.CONNECTION_MANY_TO_ONE, PortType.RECEIVE_AUTO_UPCALLS);

        IbisCapabilities s = new IbisCapabilities(
                IbisCapabilities.MEMBERSHIP_TOTALLY_ORDERED);
        
        try {
            ibis = IbisFactory.createIbis(s, null, true, this, tp);
        } catch(Throwable e) {
            System.out.println("Could not create Ibis!");
            e.printStackTrace();
            System.exit(1);
        }
        
//        System.err.println("Ibis created!");
                
        init();
        
        ibis.registry().enableEvents();
    }
    
    public abstract void init() throws IOException; 
    
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
    
    protected synchronized IbisIdentifier [] getParticipants(boolean onlyIfChanged) {     
        
        if (destinations == null || participantsChanged) {
            
            if (participants.size() == 0) { 
                System.err.print("EEK: don't have any participants left ?!!");
                System.err.print("EEK: where am I then ?!!!!");
                System.exit(1);
            }
            
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
                System.err.println("Sending from " + ibis.identifier() + " / "
                        + ibis.identifier().location().getParent() + " to ");
                
                for (int i=0;i<destinations.length;i++) { 
                    System.err.println( destinations[i] + " / "
                            + destinations[i].location().getParent());                         
                }
            }
        } 
        
        if (onlyIfChanged && !participantsChanged) {
            // nothing has changed.
            return null;
        }
        
        participantsChanged = false;          
        return destinations;
    } 
        
    protected synchronized void waitForMaster() { 
        
        // System.err.println("Waiting for master to arrive!");
        
        while (masterID == null) { 
            try { 
                wait();
            } catch (Exception e) {
                // ignore
            }
        }        
        
        // System.err.println("Master to arrived " + masterID);
    }
        
    protected synchronized void waitForMasterToLeave() { 
        
        while (masterID != null) { 
            try { 
                wait();
            } catch (Exception e) {
                // ignore
            }
        }        
        
        // System.err.println("Got master " + masterID);
    }
    
    public void addIbis(IbisIdentifier id) {
        // empty        
    }
    
    public void removeIbis(IbisIdentifier id) {
        // empty        
    }
        
    public synchronized void joined(IbisIdentifier id) {
        
//        System.err.println("Join " + id);
        
        participants.add(id);
        
        if (participants.size() == 1) { 
            // the first one will be the master
            masterID = id;
            notifyAll();
            
//            System.err.println("Master is " + id);
        }
        
        participantsChanged = true;
        
        // Inform the subclass if necessary
        addIbis(id);
    }

    public synchronized void left(IbisIdentifier id) {
        participants.remove(id);
        
        if (id.equals(masterID)) { 
            masterID = null;
            notifyAll();
        }
        
        participantsChanged = true;
        
        // Inform the subclass if necessary
        removeIbis(id);
    }

    public synchronized void died(IbisIdentifier id) {        
        left(id);
    }

    public void electionResult(String election, IbisIdentifier result) {
        // ignored
    }
    
    public void gotSignal(String signal, IbisIdentifier source) {
        // ignored
    }

    public void poolClosed() {
        // ignored
    }

    public void poolTerminated(IbisIdentifier source) {
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
            } else if (args[i].equals("-cache")) {
                cacheSize = Integer.parseInt(args[++i]);
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
            } else if (args[i].equals("-signal")) {
                signal = true;   
                args[i] = null;
            } else { 
                System.err.println("Unknown option " + args[i]);
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
