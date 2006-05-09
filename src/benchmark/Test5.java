package benchmark;

import java.io.IOException;
import java.util.ArrayList;

import lrmcast.ObjectMulticaster;

import ibis.ipl.*;

/**
 * 
 * In this test all machines send an object in a chain using the 
 * ObjectMulticaster. The test waits until the specified number of machines is 
 * reached after which everybody starts sending. A 'null' message is send to 
 * indicate that a machine is done. When the number of null messages is equal to 
 * the number of machines, the application terminates.   
 *  
 * The application can handle machines joining/leaving/crashing, although there 
 * may be a race condition when machines join when the others are already done. 
 *  
 * @author Jason Maassen
 * @version 1.0 May 9, 2006
 * @since 1.0
 */
public class Test5 implements ResizeHandler {
       
    private static int size = 100*1024;
    private static int count = 10;
    private static int repeat = 10;
    private static int machinesNeeded = 1;
        
    private static boolean verbose = false;
    
    private Ibis ibis;

    private ArrayList participants = new ArrayList();
        
    private ObjectMulticaster omc;
    
    private Object data;
    
    private int machinesDone = 0;
        
    private class Receiver extends Thread { 
        
        public void run() {
            
            int messages = 0;
            long bytes = 0;
            
            try { 
                while (true) { 

                    Object tmp = omc.receive();                      
                    bytes += omc.bytesRead();
                    
                    if (tmp == null) {
                        if (machineDone()) { 
                            System.out.println("Done!");
                            System.out.println("Got " + messages + " messages");
                            System.out.println("  (" + (bytes/(1024*1024)) + " MB)");
                            return;
                        }
                    }  
                    
                    messages++;
                }                        
            } catch (Exception e) {
                System.out.println("Oops, receiver died!" + e);
            }
        }         
    }
            
    private Test5() throws IbisException, IOException, ClassNotFoundException { 
        
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

        System.err.println("Starting test");
    
        data = new byte[size];
                
        // Start receive thread
        new Receiver().start();
                        
        waitForOthersToArrive();
      
        long start = System.currentTimeMillis();
        
        // Run for 'repeat' iterations 
        for (int i=0;i<repeat;i++) {
            runSender();
        } 
                
        // Tell eveyone that I'm done
        omc.send(getParticipants(), null);
        
        waitForOthersToQuit();
      
        long end = System.currentTimeMillis();
        
        long total = omc.totalBytes();        
        long time = end-start;
        double tp = (total/(1024.0*1024.0))/(time/1000.0);
        
        System.err.println("Total TP = " + tp + " MB/s. (includes warmup)");
              
        omc.done();
        ibis.end();
    }
    
    private synchronized void waitForOthersToQuit() {

        while (machinesDone < participants.size()-1) { 
            try { 
                wait();
            } catch (Exception e) { 
                // ignore 
            }
        }
    }
       
    private synchronized boolean machineDone() {        
        machinesDone++;        
        notifyAll();
        return (machinesDone == participants.size()-1);
    }

    private synchronized void waitForOthersToArrive() {
        
        while (participants.size() < machinesNeeded) { 
            try { 
                wait();
            } catch (Exception e) { 
                // ignore 
            }
        }
    }
        
    private synchronized IbisIdentifier [] getParticipants() { 
        // Get the set of Ibis' which will participate in this run
        IbisIdentifier [] ids = new IbisIdentifier[participants.size()-1];
        
        // Skip my own ID
        int index = 0;
        
        IbisIdentifier me = ibis.identifier();
        
        for (int i=0;i<participants.size();i++) {
            IbisIdentifier p = (IbisIdentifier) participants.get(i);

            if (!p.equals(me)) { 
                ids[index++] = p;
            } 
        }
        
        return ids;
    } 
        
    private void runSender() throws IOException, ClassNotFoundException { 

        long size = 0;
                
        IbisIdentifier [] ids = getParticipants();
                
        System.err.println("Multicasting to " + ids.length + " machines.");
        
        if (verbose) { 
            for (int i=0;i<ids.length;i++) { 
                System.err.println("   " + ids[i]);                            
            }
        } 
        
        long start = System.currentTimeMillis();
                     
        for (int i=0;i<count;i++) { 
            size += omc.send(ids, data);
        } 
        
        long end = System.currentTimeMillis();

        long time = end-start;
        double tp = (size/(1024.0*1024.0))/(time/1000.0);
        
        System.err.println(" sending took " + time + " ms. TP = " + tp + " MB/s.");
    }
    
  
                     
    public synchronized void joined(IbisIdentifier id) {       
        participants.add(id);
        notifyAll();
        
        if (verbose) { 
            System.err.println("Joined " + id);
        } 
    }

    public synchronized void left(IbisIdentifier id) {
        participants.remove(id);        
        
        if (verbose) { 
            System.err.println("Left " + id);
        } 
    }

    public synchronized void died(IbisIdentifier id) {
        participants.remove(id);
        
        if (verbose) { 
            System.err.println("Died " + id);
        } 
    }

    public synchronized void mustLeave(IbisIdentifier[] id) {             
        participants.remove(id);
        
        if (verbose) { 
            System.err.println("Must leave " + id);
        } 
        
    }
    
    public static void main(String [] args) {
               
        for (int i=0;i<args.length;i++) {
            
            if (args[i].equals("-count")) { 
                count = Integer.parseInt(args[++i]);                
            } else if (args[i].equals("-repeat")) {
                repeat = Integer.parseInt(args[++i]);
            } else if (args[i].equals("-machines")) {
                machinesNeeded = Integer.parseInt(args[++i]);                            
            } else if (args[i].equals("-size")) {                
                size = Integer.parseInt(args[++i]);
            } else if (args[i].equals("-verbose")) {
                verbose = true;            
            } else { 
                System.err.println("Unknown option " + args[i]);
                System.exit(1);
            }
        }
        
        try {            
            new Test5().start();
        } catch (Exception e) {
            System.err.println("Oops: " + e);
            e.printStackTrace(System.err);
        }
    }
}
