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
import ibis.ipl.Upcall;
import ibis.ipl.WriteMessage;

import java.io.IOException;
import java.util.HashMap;

import mcast.util.BoundedObjectQueue;
import mcast.util.DynamicObjectArray;
import mcast.util.IbisSorter;

public class LableRoutingMulticast extends Thread implements Upcall {

    private final static int ZOMBIE_THRESHOLD = 10000;

    private final Ibis ibis;
    private final PortType portType;    
    private final String name;
    
    private ReceivePort receive; 
    
    private MessageReceiver receiver;
    
    private final MessageCache cache;
    
    private final DynamicObjectArray sendports = new DynamicObjectArray();    
    private final DynamicObjectArray diedmachines = new DynamicObjectArray();
    
//    private boolean mustStop = false;    
    private boolean changeOrder = false;    
    
    private short [] destinations = null;
    
    private HashMap knownIbis = new HashMap();
    private DynamicObjectArray ibisList = new DynamicObjectArray();
        
    private short nextIbisID = 0;
    private short myID = -1;
   
    private long bytes = 0;
    
    
    private BoundedObjectQueue sendQueue = new BoundedObjectQueue(64);
      
    public LableRoutingMulticast(Ibis ibis, MessageReceiver m, MessageCache c, String name) 
        throws IOException, IbisException {
        this(ibis, m, c, false, name);
    }
            
    public LableRoutingMulticast(Ibis ibis, MessageReceiver m, MessageCache c, 
            boolean changeOrder, String name) throws IOException, IbisException {
        this.ibis = ibis;    
        this.receiver = m;
        this.name = name;
        this.cache = c;
        this.changeOrder = changeOrder;

        StaticProperties s = new StaticProperties();
        s.add("Serialization", "data");
        s.add("Communication", "ManyToOne, Reliable, AutoUpcalls");
        
        portType = ibis.createPortType("Ring", s);
        
        receive = portType.createReceivePort("Ring-" + name + "-" + ibis.identifier().name(), this);
        receive.enableConnections();
        receive.enableUpcalls();
                              
        this.start();
    }
                       
    
    private synchronized SendPort getSendPort(int id) {
        
        if (id == -1) { 
            return null;
        }
                
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
                
                IbisIdentifier ibisID = (IbisIdentifier) ibisList.get(id); 
                
                if (ibisID != null) { 
                    tmp = ibis.registry().lookupReceivePort("Ring-" + name + "-" 
                            + ibisID.name(), 10000);
                } 
                
                if (tmp != null) {                 
                    sp.connect(tmp, 10000);                
                    sendports.put(id, sp);
                } else { 
                    System.err.println("Lookup of port " + 
                            "Ring-" + name + "-"+ ibisID.name() + "failed!");
                    failed = true;
                }
            } catch (IOException e) {
                failed = true;                
                e.printStackTrace(System.err);                
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
        short id = -1;
        
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
            bytes += wm.finish();
        } catch (IOException e) {
            System.err.println("Write to " + id + " failed! " + e);
            e.printStackTrace(System.err);
            sendports.remove(id);            
        }
    } 

    public synchronized void addIbis(IbisIdentifier ibis) {
        
        if (!knownIbis.containsKey(ibis)) { 
            knownIbis.put(ibis, new Short(nextIbisID));            
            ibisList.put(nextIbisID, ibis);
                        
            if (ibis.equals(this.ibis.identifier())) { 
                myID = nextIbisID;
            }
        
            nextIbisID++;
        }        
    }

    public synchronized void removeIbis(IbisIdentifier ibis) {
        Short tmp = (Short) knownIbis.remove(ibis);
        
        if (tmp != null) { 
            ibisList.remove(tmp.shortValue());
        }
    }

    private synchronized short getIbisID(IbisIdentifier ibis) {
        
        Short s = (Short) knownIbis.get(ibis);
        
        if (s != null) { 
            return s.shortValue();
        } else { 
            //System.err.println("Ibis " + ibis + " not known!");
            return -1;
        }
    }
    
    public void setDestination(IbisIdentifier [] destinations) { 

        if (changeOrder) { 
            // We are allowed to change the order of machines in the destination
            // array. This can be used to make the mcast 'cluster aware'.            
            IbisSorter.sort(ibis.identifier(), destinations);                     
        }
        
        this.destinations = new short[destinations.length];
        
        for (int i=0;i<destinations.length;i++) { 
            this.destinations[i] = getIbisID(destinations[i]);
        }
    }
    
    public long getBytes(boolean reset) { 
        
        long tmp = bytes;
        
        if (reset) { 
            bytes = 0;
        }
        
        return tmp;
    }
    
    public void send(int id, int num, byte [] message, int off, int len) {
                
        Message m = cache.get(myID, destinations, id, num,
                message, off, len, false);              
        
        internalSend(m);
    } 

    public void send(Message m) {
        
        if (m.destinations == null) { 
            m.destinations = destinations;
        }
        
        m.sender = myID;
        
        //sendQueue.enqueue(m);
        internalSend(m);
    } 
    
    public void run() { 

        boolean done = false;
        
        while (!done) {   

            Message m = (Message) sendQueue.dequeue();
            
            try { 
                internalSend(m);
            } catch (Exception e) {
                System.err.println("Sender thread got exception! " + e);
                e.printStackTrace(System.err);
            }

            if (!m.local) {        
                try { 
                    receiver.gotMessage(m);                 
                } catch (Throwable e) {
                    System.err.println("Delivery failed! " + e);
                    e.printStackTrace(System.err);
                }            
            } else {             
                cache.put(m);
            } 
        }    
    }

    public void done() {
/*
        synchronized (this) {
            mustStop = true;
        }
*/                
        try {             
            receive.disableConnections();

            int last = sendports.last();
            
            for (int i=0;i<last;i++) { 
                SendPort tmp = (SendPort) sendports.get(i);
                
                if (tmp != null) {                 
                    tmp.close();
                } 
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
            message.local = false;

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
