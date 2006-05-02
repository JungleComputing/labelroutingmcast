package benchmark;

import java.io.IOException;
import java.util.ArrayList;

import lrmcast.LableRoutingMulticast;
import lrmcast.ByteArrayReceiver;

import ibis.ipl.*;

public class Test1 implements ResizeHandler, ByteArrayReceiver {
       
    private static int size = 1024;
    private static int count = 1024;
    private static int repeat = 10;
    private static int minMachines = 1;
        
    private int receivedMessages = 0;
    
    private Ibis ibis;
    private LableRoutingMulticast lrmcast;
    private IbisIdentifier masterID;         
    
    private ArrayList participants = new ArrayList();
        
    private Test1() throws IbisException, IOException, ClassNotFoundException { 
        StaticProperties s = new StaticProperties();
        s.add("Serialization", "object");
        s.add("Communication", "ManyToOne, Reliable, ExplicitReceipt");
        s.add("Worldmodel", "open");

        ibis = Ibis.createIbis(s, this);
        
        System.err.println("Ibis created!");
        
        lrmcast = new LableRoutingMulticast(ibis, this);
        
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
        // First get the set of Ibis' which will participate in this run         
        IbisIdentifier [] ids = 
            (IbisIdentifier []) participants.toArray(new IbisIdentifier[0]);
        
        // Flip first and last, so I will get my own message back
        IbisIdentifier tmp = ids[0];
        ids[0] = ids[ids.length-1];
        ids[ids.length-1] = tmp;
        
        return ids;
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
        
        synchronized (this) {
            while (receivedMessages != count) { 
                try { 
                    wait();                    
                } catch (InterruptedException e) {
                    // ignore
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
    }

    public synchronized void left(IbisIdentifier id) {
        participants.remove(id);
        
        if (id.equals(masterID)) { 
            masterID = null;
            notifyAll();
        }
    }

    public synchronized void died(IbisIdentifier id) {
        // ignored
    }

    public void mustLeave(IbisIdentifier[] arg0) {
        // ignored        
    }

    public synchronized void gotMessage(IbisIdentifier sender, byte[] message) {
        receivedMessages++;
        
        if (receivedMessages == count) { 
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
            } else { 
                System.err.println("Unknown option " + args[i]);
                System.exit(1);
            }
        }
        
        try { 
            new Test1().start();
        } catch (Exception e) {
            System.err.println("Oops: " + e);
            e.printStackTrace(System.err);
        }
    }
}
