package benchmark;

import ibis.ipl.IbisException;
import ibis.ipl.IbisIdentifier;
import ibis.util.Timer;

import java.io.IOException;
import java.util.HashMap;

import mcast.object.ObjectMulticaster;
import mcast.object.SendDoneUpcaller;

class OmcInfo implements SendDoneUpcaller {

    HashMap<Integer, Timer> map = new HashMap<Integer, Timer>();
    Timer total = Timer.createTimer();
    
    synchronized void registerSend(int id) {
        Timer t = Timer.createTimer();
        map.put(id, t);
        t.start();
    }
    
    public synchronized void sendDone(int id) {
        Timer t = map.remove(id);
        if(t == null) {
            System.err.println("got upcall for unknow id: " + id);
//            System.exit(1);
        }
        t.stop();
        total.add(t);
        
        System.err.println("broadcast " + id + " took " + t.totalTime());
    }
    
    void end() {
        System.err.println("total broadcast time was: " + total.totalTime());
    }
}

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
public class Test6 extends TestBase {
        
    private static boolean verbose = false;

    private ObjectMulticaster omc;
    
    private Object data;
    
    private int machinesDone = 0;

    private OmcInfo info;
    
    private class Receiver extends Thread { 
        
        public void run() {
            int messages = 0;
            long bytes = 0;
            
            try { 
                while (true) { 

                    try { 
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
                    } catch (Exception e) { 
                        System.out.println("A receive failed");   
                    }
                }                        
            } catch (Exception e) {
                System.out.println("Oops, receiver died!" + e);
                e.printStackTrace();
                System.exit(1);
            }
        }         
    }
            
    private Test6() throws IbisException, IOException, ClassNotFoundException {         
        super();
    } 
    
    public void init() throws IOException, IbisException {
        info = new OmcInfo();
        omc = new ObjectMulticaster(ibis, true /* efficient multi-cluster */, false, "test", info);
    }
    
    public void addIbis(IbisIdentifier id) {
        omc.addIbis(id);
    }
    
    public void removeIbis(IbisIdentifier id) {
        omc.removeIbis(id);
    }
    
    private void start() throws IOException, ClassNotFoundException { 

        System.err.println("Starting test");
    
        data = new Integer(42);
                
        // Start receive thread
        new Receiver().start();
                        
        waitForEnoughMachines();
      
        long start = System.currentTimeMillis();
        
        // Run for 'repeat' iterations 
        for (int i=0;i<repeat;i++) {
            runSender();
        } 

        // Tell eveyone that I'm done
        int id = omc.send(getParticipants(false), null);
        info.registerSend(id);

        waitForOthersToQuit();

        long end = System.currentTimeMillis();
        
        long total = omc.totalBytes();        
        long time = end-start;
        double tp = (total/(1024.0*1024.0))/(time/1000.0);
        
        System.err.println("Total TP = " + tp + " MB/s. (includes warmup)");
              
        omc.done();
        done();
    }
    
    private synchronized void waitForOthersToQuit() {

        // while (machinesDone < participants.size() - 1) { 
        // Fix: participants.size() is a moving target, because ibis
        // instances may leave, and in fact the leave upcall may arrive
        // sooner than the multicast it sent before leaving. (Ceriel)
        while (machinesDone < Math.max(participants.size(), minMachines)-1) { 
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
        return machinesDone >= Math.max(participants.size(), minMachines)-1;
    }
           
    private void runSender() throws IOException, ClassNotFoundException { 
        long size = 0;
                
        IbisIdentifier [] ids = getParticipants(true);
                
        if (ids != null) {         
            System.err.println("Multicasting to " + ids.length + " machines.");
        
            if (verbose) { 
                for (int i=0;i<ids.length;i++) { 
                    System.err.println("   " + ids[i]);                            
                }
            }
            
            omc.setDestination(ids);
        } 
                        
        long start = System.currentTimeMillis();
                     
        for (int i=0;i<count;i++) { 
            int id = omc.send(data);
            info.registerSend(id);
            size += omc.lastSize();
        } 
        
        long end = System.currentTimeMillis();

        long time = end-start;
        double tp = (size/(1024.0*1024.0))/(time/1000.0);
        
        System.err.println(" sending took " + time + " ms. TP = " + tp + " MB/s.");
    }
    
    public static void main(String [] args) {
               
        parseOptions(args);
        
        try {            
            new Test6().start();
        } catch (Exception e) {
            System.err.println("Oops: " + e);
            e.printStackTrace(System.err);
        }
    }
}
