package benchmark;

import java.io.IOException;
import java.util.ArrayList;

import lrmcast.ObjectMulticaster;

import ibis.ipl.*;

public class Test2 implements ResizeHandler {
       
    private static int size = 1024;
    private static int count = 1024;
    private static int repeat = 10;
    private static int minMachines = 1;
        
    private Ibis ibis;
    private IbisIdentifier masterID;         
    
    private ArrayList participants = new ArrayList();
        
    private ObjectMulticaster omc;
    
    private Test2() throws IbisException, IOException, ClassNotFoundException { 
        
        StaticProperties s = new StaticProperties();
        s.add("Serialization", "object");
        s.add("Communication", "ManyToOne, Reliable, ExplicitReceipt");
        s.add("Worldmodel", "open");

        ibis = Ibis.createIbis(s, this);
        System.err.println("Ibis created on " + ibis.identifier());
        
        ibis.enableResizeUpcalls();        
        
        omc = new ObjectMulticaster(ibis);
    }
    
    private void start() throws IOException, ClassNotFoundException { 

        waitForMaster();        
        
        if (masterID.equals(ibis.identifier())) {         
            
            boolean enoughMachines = false;
            
            while (!enoughMachines) {
                System.err.println("Waiting for " + minMachines + " to arrive");
                
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
            for (int i=0;i<repeat;i++) {
                for (int c=0;c<count;c++) {
                    omc.receive();
          //          System.err.println("Result = " + tmp + " " + tmp.length);
                } 
            }
        }
        
        omc.done();
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
        
    private void runTest() throws IOException, ClassNotFoundException { 

        IbisIdentifier [] ids = getParticipants();
        
        byte [] data = new byte[size];
        
        System.err.println("Running test with " + ids.length + " machines.");
        
        for (int i=0;i<ids.length;i++) { 
            System.err.println("   " + ids[i]);                            
        }
                
        long start = System.currentTimeMillis();
        
        for (int i=0;i<count;i++) { 
            omc.send(ids, data);
            omc.receive();
          //  System.err.println("Result = " + tmp + " " + tmp.length);
        } 
        
        long end = System.currentTimeMillis();

        long time = end-start;
        double tp = ((count*size)/(1024.0*1024.0))/(time/1000.0);
        
        System.err.println("Test took " + time + " ms. TP = " + tp + " MB/s.");
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
    }

    public synchronized void died(IbisIdentifier id) {
        // ignored
    }

    public void mustLeave(IbisIdentifier[] arg0) {
        // ignored        
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
            new Test2().start();
        } catch (Exception e) {
            System.err.println("Oops: " + e);
            e.printStackTrace(System.err);
        }
    }
}
