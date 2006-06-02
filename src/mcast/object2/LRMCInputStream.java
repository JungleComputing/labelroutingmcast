package mcast.object2;

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
            
    private class Buffer { 
        final int id;      
        final int num;              
        final boolean first; 
        final boolean last;
        final byte [] buffer;
        
        Buffer next;
        
        Buffer(int id, int num, boolean first, boolean last, byte [] buffer) { 
            this.id = id;
            this.num = num;
            this.first = first;
            this.last = last;
            this.buffer = buffer;
        }
    }
        
    public LRMCInputStream(String source, ObjectReceiver receiver) { 
        this.source = source;
        this.receiver = receiver;
    }
    
    public String getSource() { 
        return source;
    }
    
    public boolean haveData() {  
        return (head != null);
    }
    
    private synchronized void addBuffer(int id, int num, boolean first, 
            boolean last, byte [] buffer) { 
        
        // TODO: cache these ?
        Buffer tmp = new Buffer(id, num, first, last, buffer);
        
        if (head == null) { 
            head = tail = tmp;
            notifyAll();
        } else {             
            tail.next = tmp;
            tail = tail.next;
        }
    }
    
    public void addBuffer(int id, int num, byte [] buffer) {
        
        boolean firstPacket = false;
        boolean lastPacket = false;
        
        if ((id & FIRST_PACKET) != 0) { 
            firstPacket = true;
            id = id & ~FIRST_PACKET;
        } 
            
        if ((id & LAST_PACKET) != 0) { 
            lastPacket = true;
            id = id & ~LAST_PACKET;
        } 
        
      /*  System.err.println("___ Got packet (" + source + ") " + id + 
                (firstPacket ? "F" : " ") + (lastPacket ? "L" : " ") + 
                " byte[" + buffer.length + "]");
        */        
        addBuffer(id, num, firstPacket, lastPacket, buffer);        

        memoryUsage += buffer.length;
        
        if (lastPacket) { 
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
               
            while (!current.first) {
                // Oh dear, we seem to have missed the start of a series of
                // packets. We may have lost a message somewhere (which is 
                // unlikely) or the last receive failed half way due to some
                // deserialization problem and the stream is now confused a bit
                // (this is more likely). To solve this, we simply drop the 
                // current packet and get a new one. We keep trying until we see
                // a 'first packet'.                    
                System.err.println("___ Dropping packet " + current.id + "/" + 
                        current.num + " [" + current.buffer.length + "] " +
                                "since it's not the first one!");         
                freeBuffer();
                getBuffer();
            } 
                
            currentID = current.id;
    
       //     System.err.println("____ Current memory usage " + 
       //             (memoryUsage/(1024*1024)) + " MB");
            
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
    
    private void freeBuffer() { 
        // Free's the current buffer and updates the currentID if necessary
        
        if (current.last) { 
            currentID = 0;
        }
        
        memoryUsage -= current.buffer.length;

        // TODO: recycle buffer ? 
        current = null;
    }
    
    public int read(byte b[], int off, int len) throws IOException {
        
        if (current == null || index == current.buffer.length) { 
            getBuffer();
        } 
        
        checkBuffer();
                
        int leftover = current.buffer.length-index;
                
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
