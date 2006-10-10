package mcast.object;

import ibis.util.GetLogger;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;

import mcast.lrm.Message;
import mcast.lrm.MessageCache;

import org.apache.log4j.Logger;

//import mcast.lrm.ByteArrayCache;

public class LRMCInputStream extends InputStream {

    private static final Logger logger
            = GetLogger.getLogger(LRMCInputStream.class.getName());
    
    private final short source;       

    private final ObjectReceiver receiver;
    
    private final ArrayList queue = new ArrayList();
    
    private Message current = null; 
    
    private int index = 0;
    
    private int currentID = 0;    
    private int currentNum = 0;    
          
    // private long memoryUsage = 0;     
//    private int lowBound = 0;
//    private int highBound = 1024*1024;
       
    private MessageCache cache; 
           
    public LRMCInputStream(short source, MessageCache cache) { 
        this(source, cache, null);
    }
    
    public LRMCInputStream(short source, MessageCache cache, 
            ObjectReceiver receiver) {  
        
        this.source = source;
        this.cache = cache;
        this.receiver = receiver;        
    }
        
    public short getSource() { 
        return source;
    }
    
    public boolean haveData() {        
        if (current != null && index < current.len) {
            return true;
        }
        synchronized(queue) {
            return queue.size() != 0;
        }
    }
    
    public boolean addMessage(Message m) { 
        
        synchronized(queue) {
            queue.add(m);
            queue.notify();
        }
        
        if (logger.isDebugEnabled()) {
            logger.debug("Queued message " + m.id + "/" + m.num
                    + "(" + m.len + ")");
        }
        
      //  synchronized (this) {
            // Note: use real length here!
      //      memoryUsage += m.buffer.length;
      //  } 
        
        if (receiver != null && m.last) { 
            receiver.haveObject(this);
        }           
        return true;
    }

    private void getMessage() { 
        synchronized(queue) {
            while (queue.size() == 0) {
                try {
                    queue.wait();
                } catch(Exception e) {
                    // ignored
                }
            }
            current = (Message) queue.remove(0);        
        }

        index = 0;
        
        if (logger.isDebugEnabled()) {
            logger.debug("Dequeued message " + current.id + "/" + current.num
                    + "(" + current.len + ")");
        }
    }

    private void checkMessage() throws IOException { 
        // Check if the ID/number of the packet corresponds to what we expect.
        
        if (currentID == 0) {
            // We must start a new series of packets here. Each series 
            // corresponds to a 'multi fragment' message.
                    
            while (current.num != 0) {
                // Oh dear, we seem to have missed the start of a series of
                // packets. We may have lost a message somewhere (which is 
                // unlikely) or the last receive failed half way due to some
                // deserialization problem and the stream is now confused a bit
                // (this is more likely). To solve this, we simply drop the 
                // current packet and get a new one. We keep trying until we see
                // a 'first packet'.                    
                logger.info("___ Dropping packet " + current.id + "/" + 
                        current.num + " [" + current.len + "] " +
                                "since it's not the first one!");                
                freeMessage();
                getMessage();
                if (current == null) {
                    return;
                }
            } 
        
            if (logger.isDebugEnabled()) {
                logger.debug("Starting new series " + current.id);            
            }
                        
            currentID  = current.id;
            currentNum = 0;
    /*
            if (memoryUsage > highBound) { 
                System.err.println("++++ Current memory usage " + 
                        (memoryUsage/1024) + " KB, series " + currentID);
                
                lowBound = highBound;
                highBound += 1024*1024;

            } else if (memoryUsage < lowBound) { 

                System.err.println("---- Current memory usage " + 
                        (memoryUsage/1024) + " KB, series " + currentID);
                
                highBound = lowBound;
                lowBound -= 1024*1024;                
            }
      */                      
        } else if (currentID != current.id || currentNum != current.num) {                      
            
            // Oh dear, we seem to have missed a part of a series of packets. 
            // This is likely to happen when one of our predecessors in the
            // multicast chain has crashed. As a result, it does not forward the 
            // one or more packets to me. When it's predecesor notices this, it 
            // may change the chain and start sending to me directly. This way, 
            // we see a sudden change in the ID number, without having seen a 
            // 'last packet' for the previous series, or we see the currentNum 
            // skip a few values.
            
            // We solve this setting the currentID to 0 (to indicate that we 
            // want to start a new series) and throwing an exception to notify 
            // the user that we cannot finish the current multicast. We will 
            // process the current message when the user has handled the 
            // exception and tries to receive again. 
            String tmp = "Inconsistency discovered in multicast packet series," 
                    + " current series " + currentID + "/" + currentNum + 
                    " next packet " + current.id + "/" + current.num;
            
            currentID  = 0;                        
            throw new IOException(tmp);            
        } 
    }
    
    private void freeMessage() { 
        // Free the current message and updates the currentID if necessary

        if (logger.isDebugEnabled()) {
            logger.debug("Freed message " + current.id + "/" + current.num 
                    + " last = " + current.last);
        }
        
        if (current.last) { 
            currentID  = 0;
            currentNum = 0;
        } else { 
            currentNum++;
        }
        
        // Note use real length here!
       // memoryUsage -= current.buffer.length;

        cache.put(current);        
        current = null;
    }

    public int read(byte b[], int off, int len) throws IOException {
        
        if (current == null) {
            getMessage();
        }
        if (current == null) {
            throw new IOException("Someone wants us to stop ...");
        }
        
        checkMessage();
        if (current == null) {
            throw new IOException("Someone wants us to stop ...");
        }

        int leftover = current.len-index;
                
        if (leftover <= len) { 
            if (logger.isDebugEnabled()) {
                logger.debug("Copying " + index + " " + current.buffer.length  
                        + " " + off + " " + leftover);
            }
            
            System.arraycopy(current.buffer, index, b, off, leftover);            
            freeMessage();
            return leftover;
        } else {          
            System.arraycopy(current.buffer, index, b, off, len);
            index += len;
            return len;
        } 
    }
    
    public int read() throws IOException {
        // Ouch ... fortunately it's never used! 
        byte [] tmp = new byte[1];        
        read(tmp, 0, 1);               
        return tmp[0];
    }

}
