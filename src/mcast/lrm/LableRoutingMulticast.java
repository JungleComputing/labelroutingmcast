package mcast.lrm;

import ibis.ipl.Ibis;
import ibis.ipl.IbisException;
import ibis.ipl.IbisIdentifier;
import ibis.ipl.PortType;
import ibis.ipl.ReadMessage;
import ibis.ipl.ReceivePort;
import ibis.ipl.ReceivePortIdentifier;
import ibis.ipl.SendPort;
import ibis.ipl.StaticProperties;
import ibis.ipl.WriteMessage;
import ibis.ipl.Upcall;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

public class LableRoutingMulticast extends Thread implements Upcall {

    private final static int ZOMBIE_THRESHOLD = 10000;

    private final Ibis ibis;
    private final PortType portType;
    private ReceivePort receive; 
    
    private MessageReceiver receiver;
    
    private final MessageCache cache;
    
    private final HashMap sendports = new HashMap();    
    private final HashMap diedmachines = new HashMap();
    
    private boolean mustStop = false;    
    private boolean changeOrder = false;    
    
    private String [] destinations = null;
    
    private MessageQueue sendQueue = new MessageQueue(10);
      
    public LableRoutingMulticast(Ibis ibis, MessageReceiver m, MessageCache c) 
        throws IOException, IbisException {
        this(ibis, m, c, false);
    }
            
    public LableRoutingMulticast(Ibis ibis, MessageReceiver m, MessageCache c, 
            boolean changeOrder) throws IOException, IbisException {
        this.ibis = ibis;    
        this.receiver = m;
        this.cache = c;
        this.changeOrder = changeOrder;
                
        StaticProperties s = new StaticProperties();
        s.add("Serialization", "data");
        s.add("Communication", "ManyToOne, Reliable, AutoUpcalls");
        
        portType = ibis.createPortType("Ring", s);
        
        receive = portType.createReceivePort("Ring-" + ibis.identifier().name(), this);
        receive.enableConnections();
        receive.enableUpcalls();
                              
        this.start();
    }
      
    private void receive(ReadMessage rm) { 

           
    }
              
    private SendPort getSendPort(String id) { 
        SendPort sp = (SendPort) sendports.get(id);
        
        if (sp == null) {
            // We're not connect to this ibis yet, so connect and store for 
            // later use.
            
            // Test if the machine died recently to prevent us from trying to 
            // connect over and over again (this may be a problem since a single
            // large mcast may be fragmented into many small packets, each with 
            // the same route containing the dead machine)            
            Long ripTime = (Long) diedmachines.get(id);

            if (ripTime != null) { 
                
                long now = System.currentTimeMillis();
                
                if (now - ripTime.longValue() > ZOMBIE_THRESHOLD) { 
                    // the machine has been dead for a long time, but the sender
                    // insists it is still alive. Lets try again and see what 
                    // happens.
                    diedmachines.remove(id);
                    
                    System.err.println("Sender insists that " + id  
                            + " is still allive, so I'll try again!");
                } else { 
                    //System.err.println("Ignoring " + id + " since it's dead!");
                    return null;
                }                
            }
            
            
            boolean failed = false;
            
            ReceivePortIdentifier tmp = null; 
                            
            try { 
                sp = portType.createSendPort();
                
                tmp = ibis.registry().lookupReceivePort("Ring-" + id, 1000);
                
                if (tmp != null) {                 
                    sp.connect(tmp, 1000);                
                    sendports.put(id, sp);
                } else { 
                    failed = true;
                }
            } catch (IOException e) {
                failed = true;
            } 
            
            if (failed) {                 
                System.err.println("Failed to connect to " + id 
                        + " - informing nameserver!");
                
                // notify the nameserver that this machine may be dead...
                try { 
                    if (tmp != null) { 
                        ibis.registry().maybeDead(tmp.ibis());
                    } 
                    diedmachines.put(id, new Long(System.currentTimeMillis()));                    
                } catch (Exception e2) {
                    
                    System.err.println("Failed to inform nameserver! " + e2);
                    
                    // ignore
                }
                
                System.err.println("Done informing nameserver");
                                
                return null;
            }
        }
   
        return sp;
    }
            
    private void internalSend(Message m) {
      
        if (m.destinations == null || m.numDestinations == 0) { 
            return; 
        }
        
        // Get the next target from the destination array. If this fails, get 
        // the next one, etc. If no working destination is found we give up.  
        int index = 0;        
        SendPort sp = null;
        String id = null;
        
        do { 
            id = m.destinations[index++];
            sp = getSendPort(id);
        } while (sp == null && index < m.numDestinations);
        
        if (sp == null) { 
            // No working destinations where found, so give up!
            return;
        }
        
        // send the message to the target 
        try { 
            WriteMessage wm = sp.newMessage();
            m.write(wm, index);        
            wm.finish();
        } catch (IOException e) {
            System.err.println("Write to " + id + " failed! " + e);
            e.printStackTrace(System.err);
            sendports.remove(id);            
        }
    } 

    public void setDestination(IbisIdentifier [] destinations) { 

        if (changeOrder) { 
            // We are allowed to change the order of machines in the destination
            // array. This can be used to make the mcast 'cluster aware'.            
            IbisSorter.sort(ibis.identifier(), destinations);                     
        }
        
        this.destinations = new String[destinations.length];
        
        for (int i=0;i<destinations.length;i++) { 
            this.destinations[i] = destinations[i].name();
        }
    }
    
    public void send(int id, int num, byte [] message, int off, int len) {
                
        Message m = cache.get(ibis.identifier().name(), destinations, id, num,
                message, off, len);
              
        internalSend(m);
        
        cache.put(m);
    } 

    public void send(Message m) {
        
        if (m.destinations == null) { 
            m.destinations = destinations;
        }
        
        if (m.sender == null) { 
            m.sender = ibis.identifier().name();
        }
        
        internalSend(m);
    } 
    
    public void run() { 

        boolean done = false;
        
        while (!done) {   

            Message m = sendQueue.dequeue();
            
            try { 
                internalSend(m);
            } catch (Exception e) {
                System.err.println("Sender thread got exception! " + e);
                e.printStackTrace(System.err);
            }

            try { 
              //  System.out.println("Delivery of " + m.len);
                receiver.gotMessage(m);                 
            } catch (Throwable e) {
                System.err.println("Delivery failed! " + e);
                e.printStackTrace(System.err);
            }
/*            
            
            try { 
                receive(receive.receive());               
            } catch (Exception e) {
                
                synchronized (this) {
                    if (!mustStop) {                
                        System.err.println("Receive failed! " + e);
                    } 
                } 
            }  
            
            synchronized (this) {
                done = mustStop;
            }
            */
        }    
    }

    public void done() {

        synchronized (this) {
            mustStop = true;
        }
                
        try {             
            receive.disableConnections();
        
            Iterator i = sendports.values().iterator();
        
            while (i.hasNext()) { 
                SendPort tmp = (SendPort) i.next();
                tmp.close();
            }
            
            receive.close(1000);
        } catch (Exception e) {
            // ignore, we tried...
        }
    }

    public void upcall(ReadMessage rm) throws IOException {
        
        Message message = null; 
            
        try {
            int len = rm.readInt();
            int dst = rm.readInt();
            
            message = cache.get(len, dst);                        
            message.read(rm, len, dst);
            
            //message.read(rm);            
            sendQueue.enqueue(message);
            
        } catch (IOException e) {
            System.err.println("Failed to receive message: " + e);
            e.printStackTrace(System.err);
            rm.finish(e);
            
            if (message != null) { 
                cache.put(message);
            }
        }               
    }
}
