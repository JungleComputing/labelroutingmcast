package mcast.object;

import java.io.IOException;
import java.io.InputStream;

import mcast.lrm.Message;
import mcast.lrm.MessageCache;

//import mcast.lrm.ByteArrayCache;

public class LRMCInputStream extends InputStream implements LRMCStreamConstants {
    
    private String source;       
    private ObjectReceiver receiver;
        
    private Message current = null; 
    
    private int index = 0;
    
    private int currentID = 0;    
    private int currentNum = 0;    
    
    private long memoryUsage = 0; 
    
    private Message head;
    private Message tail;
   
    private int lowBound = 0;
    private int highBound = 1024*1024;
       
    private MessageCache cache; 
           
    public LRMCInputStream(String source, MessageCache cache) { 
        this(source, cache, null);
    }
    
    public LRMCInputStream(String source, MessageCache cache, 
            ObjectReceiver receiver) {  
        
        this.source = source;
        this.cache = cache;
        this.receiver = receiver;
    }
        
    public String getSource() { 
        return source;
    }
    
    public synchronized boolean haveData() {  
        return (head != null) || 
            (current != null && index < current.len) ;
    }
    
    private synchronized void queueMessage(Message m) { 
        
        if (head == null) { 
            head = tail = m;
            notifyAll();
        } else {             
            tail.next = m;
            tail = tail.next;
        }
        
        // Note use real length here!
        memoryUsage += m.buffer.length;
    }
    
    public void addMessage(Message m) {
        
        if ((m.num & LAST_PACKET) != 0) { 
            m.last = true;
            m.num &= ~LAST_PACKET;
        } 
        
      /*  System.err.println("___ Got packet (" + source + ") " + id + 
                (firstPacket ? "F" : " ") + (lastPacket ? "L" : " ") + 
                " byte[" + buffer.length + "]");
        */        
        queueMessage(m); 
        
        if (receiver != null && m.last) { 
            receiver.haveObject(this);
        }     
    }
        
    private synchronized void getMessage() { 
        
        while (head == null) {
            try { 
                wait();
            } catch (InterruptedException e) {
                // ignore
            }
        }        
        
        current = head;
        head = head.next;
        
        if (head == null) { 
            tail = null;
        }
        
        index = 0;
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
                System.err.println("___ Dropping packet " + current.id + "/" + 
                        current.num + " [" + current.len + "] " +
                                "since it's not the first one!");                
                freeMessage();
                getMessage();
            } 
                
            currentID  = current.id;
            currentNum = 0;
    
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
    
    private synchronized void freeMessage() { 
        // Free the current message and updates the currentID if necessary
        
        if (current.last) { 
            currentID  = 0;
            currentNum = 0;
        } else { 
            currentNum++;
        }
        
        // Note use real length here!
        memoryUsage -= current.buffer.length;

        cache.put(current);        
        current = null;
    }
    
    public int read(byte b[], int off, int len) throws IOException {
        
        if (current == null || index == current.len) { 
            getMessage();
        } 
        
        checkMessage();
                
        int leftover = current.len-index;
                
        if (leftover <= len) { 
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
