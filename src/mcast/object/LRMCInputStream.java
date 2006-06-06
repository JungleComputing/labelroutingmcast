package mcast.object;

import java.io.IOException;
import java.io.InputStream;

public class LRMCInputStream extends InputStream implements LRMCStreamConstants {
    
    private String source;       
    private ObjectReceiver receiver;
        
    private Buffer current = null; 
    private int index = 0;
    private int currentID = 0;    
    
    private long memoryUsage = 0; 
    
    private Buffer head;
    private Buffer tail;
   
    private int lowBound = 0;
    private int highBound = 1024*1024;
       
    private static BufferCache cache = new BufferCache(1000);
       
    public LRMCInputStream(String source) { 
        this(source, null);
    }
    
    public LRMCInputStream(String source, ObjectReceiver receiver) { 
        this.source = source;
        this.receiver = receiver;
    }
        
    public String getSource() { 
        return source;
    }
    
    public synchronized boolean haveData() {  
        return (head != null) || 
            (current != null && index < current.len) ;
    }
    
    private synchronized void addBuffer(int id, int num, boolean last, 
            byte [] buffer, int len) { 
        
        // TODO: cache these ?
        Buffer tmp = cache.get();
        tmp.set(id, num, last, buffer, len);
        
        if (head == null) { 
            head = tail = tmp;
            notifyAll();
        } else {             
            tail.next = tmp;
            tail = tail.next;
        }
        
        // Note use real length here!
        memoryUsage += buffer.length;
    }
    
    public void addBuffer(int id, int num, byte [] buffer, int len) {
        
        boolean lastPacket = false;
        
        if ((num & LAST_PACKET) != 0) { 
            lastPacket = true;
            num = num & ~LAST_PACKET;
        } 
        
      /*  System.err.println("___ Got packet (" + source + ") " + id + 
                (firstPacket ? "F" : " ") + (lastPacket ? "L" : " ") + 
                " byte[" + buffer.length + "]");
        */        
        addBuffer(id, num, lastPacket, buffer, len);
        
        if (receiver != null && lastPacket) { 
            receiver.haveObject(this);
        }     
    }
        
    private synchronized void getBuffer() { 
        
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

    private void checkBuffer() throws IOException { 
        // Checks if the ID number of the packet corresponds to what we expect.
        
        if (currentID == 0) {
            // We must start a new series of buffers here. Each series 
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
                freeBuffer();
                getBuffer();
            } 
                
            currentID = current.id;
    
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
                            
        } else if (currentID != current.id) {                      
            // Oh dear, we seem to have missed the end of a series of packets. 
            // This is likely to happen when one of our predecessors in the
            // multicast chain has crashed. As a result, it does not forward the 
            // last part of a series of packets (which together form one 
            // multicast message). When the next multicast start, the problem 
            // may be fixed (i.e., by changing the chain). This way, we see a 
            // sudden change in the ID number, without having seen a 'last 
            // packet' for the previous series. 
            
            // We solve this by setting the currentID to 0, and throwing an 
            // exception to notify the user that we cannot finish the current 
            // multicast. We will process the current message when the user has
            // handled the exception and tries to receive again
            String tmp = "Inconsistency discovered in multicast packet series," 
                    + " current series " + currentID + " next packet " 
                    + current.id + "/" + current.num;
            
            currentID = 0;                        
            throw new IOException(tmp);
        }        
    }
    
    private synchronized void freeBuffer() { 
        // Free's the current buffer and updates the currentID if necessary
        
        if (current.last) { 
            currentID = 0;
        }
        
        // Note use real length here!
        memoryUsage -= current.buffer.length;

        cache.put(current);        
        current = null;
    }
    
    public int read(byte b[], int off, int len) throws IOException {
        
        if (current == null || index == current.len) { 
            getBuffer();
        } 
        
        checkBuffer();
                
        int leftover = current.len-index;
                
        if (leftover <= len) { 
            System.arraycopy(current.buffer, index, b, off, leftover);            
            freeBuffer();
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
