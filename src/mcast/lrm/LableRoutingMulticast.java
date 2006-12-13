package mcast.lrm;

import ibis.ipl.Ibis;
import ibis.ipl.IbisIdentifier;
import ibis.ipl.PortType;
import ibis.ipl.ReadMessage;
import ibis.ipl.ReceivePort;
import ibis.ipl.ReceivePortIdentifier;
import ibis.ipl.SendPort;
import ibis.ipl.StaticProperties;
import ibis.ipl.Upcall;
import ibis.ipl.WriteMessage;

import ibis.util.GetLogger;
import ibis.util.TypedProperties;

import java.io.IOException;
import java.util.HashMap;

import mcast.util.DynamicObjectArray;
import mcast.util.IbisSorter;

import org.apache.log4j.Logger;

public class LableRoutingMulticast extends Thread implements Upcall {

    private final static int ZOMBIE_THRESHOLD = 10000;

    private static final Logger logger
            = GetLogger.getLogger(LableRoutingMulticast.class.getName());
    private static final int QUEUE_SIZE
            = TypedProperties.intProperty("lrmc.queueSize", 32);

    private final Ibis ibis;
    private final PortType portType;    
    private final String name;
    
    private ReceivePort receive; 
    
    private MessageReceiver receiver;
    
    private final MessageCache cache;
    
    private final DynamicObjectArray sendports = new DynamicObjectArray();    
    private final DynamicObjectArray diedmachines = new DynamicObjectArray();
    
    private boolean finish = false;
    private boolean changeOrder = false;    
    
    private short [] destinations = null;
    
    private HashMap<IbisIdentifier, Short> knownIbis
            = new HashMap<IbisIdentifier, Short>();
    private DynamicObjectArray ibisList = new DynamicObjectArray();
        
    private short nextIbisID = 0;
    private short myID = -1;
   
    private long bytes = 0;

    private MessageQueue sendQueue = new MessageQueue(QUEUE_SIZE);
      
    public LableRoutingMulticast(Ibis ibis, MessageReceiver m, MessageCache c, 
            String name) throws IOException {
        this(ibis, m, c, false, name);
    }
            
    public LableRoutingMulticast(Ibis ibis, MessageReceiver m, MessageCache c, 
            boolean changeOrder, String name) throws IOException {
        this.ibis = ibis;    
        this.receiver = m;
        this.name = name;
        this.cache = c;
        this.changeOrder = changeOrder;

        StaticProperties s = new StaticProperties();
        s.add("Serialization", "data");
        s.add("Communication", "ManyToOne, Reliable, AutoUpcalls");
        
        try {
            portType = ibis.createPortType("Ring", s);
        } catch(Throwable e) {
            throw new IOException("Could not create port type" + e);
        }

        s = ibis.properties();
        
        receive = portType.createReceivePort("Ring-" + name, this);
        receive.enableConnections();
        receive.enableUpcalls();
                              
        super.setName("LableRoutingMulticast:" + name);
        this.start();
    }


    private synchronized SendPort getSendPort(int id) {
        
        if (id == -1) { 
            logger.info("Ignoring " + id);
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
                    
                    logger.info("Sender insists that " + id  
                            + " is still allive, so I'll try again!");
                } else { 
                    logger.info("Ignoring " + id + " since it's dead!");
                    return null;
                }                
            }
            
            
            boolean failed = false;
            
            ReceivePortIdentifier tmp = null; 

            IbisIdentifier ibisID = (IbisIdentifier) ibisList.get(id); 

            try { 
                sp = portType.createSendPort();         
                
                if (ibisID == null) {
                    synchronized(this) {
                        try {
                            wait(10000);
                        } catch(Exception e) {
                            // ignored
                        }
                    }
                }

                ibisID = (IbisIdentifier) ibisList.get(id); 

                if (ibisID != null) {
                    if (ibisID != null) {
                        sp.connect(ibisID, "Ring-" + name, 10000);
                        sendports.put(id, sp);
                    } else {
                        logger.info("No Ibis yet at position " + id);
                        failed = true;
                    }
                } else {
                    logger.info("No Ibis yet at position " + id);
                    failed = true;
                }
            } catch (IOException e) {
                failed = true;                
                logger.info("Got exception ", e);
            } 
            
            if (failed) {
                logger.info("Failed to connect to " + id 
                            + " - informing nameserver!");
                                
                // notify the nameserver that this machine may be dead...
                try { 
                    if (ibisID != null) {
                        ibis.registry().maybeDead(ibisID);
                    } 
                    diedmachines.put(id, new Long(System.currentTimeMillis()));                    
                } catch (Exception e2) {
                    logger.info("Failed to inform nameserver! " + e2);
                    // ignore
                }
                
                logger.info("Done informing nameserver");
                return null;
            }
        }
   
        return sp;
    }

    private void internalSend(Message m) {
        SendPort sp = null;
        if (m.destinationsUsed == 0) {  
            if (m.last) {
                sp = getSendPort(m.sender); 
                if (sp != null) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Writing DONE message " + m.id
                             + " to sender "  + m.sender);
                    }
                    try {
                        WriteMessage wm = sp.newMessage();
                        wm.writeInt(-1);
                        wm.writeInt(m.id);
                        wm.finish();
                    } catch(IOException e) {
                        logger.debug("Writing DONE message to "
                                + m.sender + " failed");
                    }
                } else if (logger.isDebugEnabled()) {
                    logger.debug("No sendport for sender " + m.sender);
                }
            }
            return;
        }

        // Get the next target from the destination array. If this fails, get 
        // the next one, etc. If no working destination is found we give up.  
        int index = 0;        
        short id = -1;
        
        do { 
            id = m.destinations[index++]; 
            sp = getSendPort(id); 
            if (sp == null) {
                synchronized(this) {
                    if (finish) {
                        return;
                    }
                }
            }
        } while (sp == null && index < m.destinationsUsed);
        
        try {
            if (sp == null) { 
                // No working destinations where found, so give up!
                logger.info("No working destinations found, giving up!");
                return;
            }
            
            if (logger.isDebugEnabled()) {
                logger.debug("Writing message " + m.id + "/" + m.num 
                     + " to " + id
                     + ", sender " + m.sender
                     + ", destinations left = " + (m.destinationsUsed - index));
            }
            
            // send the message to the target        
            WriteMessage wm = sp.newMessage();
            m.write(wm, index);        
            bytes += wm.finish();
        } catch (IOException e) {
            logger.info("Write to " + id + " failed! ", e);
            sendports.remove(id);            
        }
    } 

    public synchronized void addIbis(IbisIdentifier ibis) {
        
        if (!knownIbis.containsKey(ibis)) { 
            knownIbis.put(ibis, new Short(nextIbisID));            
            ibisList.put(nextIbisID, ibis);
            
            logger.info("Adding Ibis " + nextIbisID + " " + ibis);
                                    
            if (ibis.equals(this.ibis.identifier())) {                
                logger.info("I am " + nextIbisID + " " + ibis);
                myID = nextIbisID;
            }
        
            nextIbisID++;
            notifyAll();
        }        
    }

    public synchronized void removeIbis(IbisIdentifier ibis) {
                
        Short tmp = knownIbis.remove(ibis);
        
        if (tmp != null) {
            logger.info("Removing ibis " + tmp.shortValue() + " " + ibis);
            ibisList.remove(tmp.shortValue());
        }
    }

    private synchronized short getIbisID(IbisIdentifier ibis) {
        
        Short s = knownIbis.get(ibis);
        
        if (s != null) { 
            return s.shortValue();
        } else { 
            logger.debug("Ibis " + ibis + " not known!");
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
            logger.debug("  " + i + " (" + destinations[i] + " at " 
                  + destinations[i].cluster() + ") -> " + this.destinations[i]);
        }
    }
    
    public long getBytes(boolean reset) { 
        
        long tmp = bytes;
        
        if (reset) { 
            bytes = 0;
        }
        
        return tmp;
    }
        
    public boolean send(Message m) {
        
        short [] destOld = m.destinations;
        
        m.destinations = destinations;
        m.destinationsUsed = destinations.length;        
        m.sender = myID;
        m.local = true;
        
        internalSend(m);
        
        m.destinations = destOld;        
        return true;        
    }
           
    // Remove ? 
    public boolean send(int id, int num, byte [] message, int off, int len) {        
        
        if (myID == -1) {
            logger.debug("Cannot send packet: local ID isn't known!");
            return true;            
        }
        
        Message m = cache.get(); 
        
        byte [] tmp = m.buffer;
        
        m.sender = myID;
        m.destinations = destinations; 
        m.destinationsUsed = destinations.length;
        m.id = id;
        m.num = num;
        m.buffer = message;
        m.off = off;
        m.len = len;
        m.local = true;
        m.last = false;
                        
        //sendQueue.enqueue(m);        
        internalSend(m);
        
        // Restore the original buffer before putting the 
        // message back in the cache... 
        m.buffer = tmp;        
        cache.put(m);
        
        return true;
    } 
    
    public void run() { 

        while (true) {
            Message m = (Message) sendQueue.dequeue();
            if (m == null) {
                // Someone wants us to stop
                return;
            }

            try { 
                internalSend(m);
            } catch (Exception e) {
                logger.info("Sender thread got exception! ", e);
            } finally {
                cache.put(m);
            }
        }    
    }

    public void done() {
        // sendQueue.printTime();
        synchronized(this) {
            finish = true;
        }
        sendQueue.terminate();
        try {
            join(10000);
        } catch(Exception e) {
            // ignored
        }
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
        } catch (Throwable e) {
            // ignore, we tried...
        }
    }

    public void upcall(ReadMessage rm) throws IOException {
        
        Message message = null; 
            
        try {
            int len = rm.readInt();
            if (len == -1) {
                // DONE message
                int id = rm.readInt();
                if (logger.isDebugEnabled()) {
                    logger.debug("Got DONE for message " + id);
                }
                receiver.gotDone(id);
                return;
            }
            
            int dst = rm.readInt();
            
            message = cache.get(len);                        
            message.read(rm, len, dst);            

            if (logger.isDebugEnabled()) {
                logger.debug("Reading message " + message.id + "/"
                        + message.num + " from " + message.sender);
            }

            if (!message.local) {        
                message.refcount++;
                try { 
                    receiver.gotMessage(message);
                } catch (Throwable e) {
                    logger.info("Delivery failed! ", e);
                }
            }

            // Is this OK? sendQueue may block (is not allowed in upcall)!
            // However, calling finish() here may change the message order,
            // so we cannot do that. (Ceriel).
            sendQueue.enqueue(message);
            
        } catch (IOException e) {
            logger.info("Failed to receive message: ", e);
            rm.finish(e);
            
            if (message != null) { 
                cache.put(message);
            }
        }               
    }

    public int getPrefferedMessageSize() {
        return cache.getPrefferedMessageSize();
    }
}
