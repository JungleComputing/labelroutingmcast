package benchmark;

import java.io.IOException;
import java.util.ArrayList;

import lrmcast.ObjectMulticaster;

import ibis.ipl.*;

public class Test3 implements ResizeHandler {
       
    private static int size = 1024;
    private static int count = 1024;
    private static int repeat = 10;
    private static int minMachines = 1;
        
    private Ibis ibis;
    private IbisIdentifier masterID;         
    
    private ArrayList participants = new ArrayList();
        
    private ObjectMulticaster omc;
    
    private DoubleData data;
    
    private Test3() throws IbisException, IOException, ClassNotFoundException { 
        
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
            
            // Create the data object
            data = new DoubleData(size);                        
            
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
                runSender();
            } 
        } else {
            for (int i=0;i<repeat;i++) {                 
                runReceiver();
            } 
        }
        
        omc.done();
        ibis.end();
    }
    
    private synchronized IbisIdentifier [] getParticipants() { 
        // Get the set of Ibis' which will participate in this run
        IbisIdentifier [] ids = new IbisIdentifier [participants.size()-1];
        
        // Skip my own ID
        for (int i=1;i<participants.size();i++) { 
            ids[i-1] = (IbisIdentifier) participants.get(i);
        }
        
        return ids;
    } 
        
    private void runSender() throws IOException, ClassNotFoundException { 

        IbisIdentifier [] ids = getParticipants();
                
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
            new Test3().start();
        } catch (Exception e) {
            System.err.println("Oops: " + e);
            e.printStackTrace(System.err);
        }
    }
}
