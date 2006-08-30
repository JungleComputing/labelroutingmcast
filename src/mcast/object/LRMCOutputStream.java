package mcast.object;

import java.io.IOException;
import java.io.OutputStream;

import mcast.lrm.LableRoutingMulticast;
import mcast.lrm.Message;
import mcast.lrm.MessageCache;

public class LRMCOutputStream extends OutputStream {

    private final LableRoutingMulticast mcast;
    private final MessageCache cache; 
    
    private int currentID = 1;  
    private int currentNUM = 0;  
        
    private boolean closed = false;    
    private boolean firstPacket = false;
    
    private Message message;
    
    LRMCOutputStream(LableRoutingMulticast mcast, MessageCache cache) { 
        this.mcast = mcast;
        this.cache = cache;
        message = cache.get();        
    }

    public void reset() { 
        firstPacket = true;
    }

    public void close() { 
        closed = true;
    }

    public int getPrefferedBufferSize() { 
        return mcast.getPrefferedMessageSize();
    }
    
    public byte [] getBuffer() { 
        return message.buffer;
    }
    
    public byte [] write(int off, int len, boolean lastPacket) {
        
        if (closed) { 
            System.err.println("____ got write(" + len + ") while closed!");
            return null;
        }
                
        if (firstPacket) {
            firstPacket = false;
            currentNUM = 0;
        } else { 
            currentNUM++;
        }
        
        message.off = 0;
        message.len = len;        
                
        if (lastPacket) {
            message.num = currentNUM | Message.LAST_PACKET;
            message.id = currentID++;
        } else { 
            message.num = currentNUM;
            message.id = currentID;
        }
                        
        if (mcast.send(message)) { 
            return message.buffer;
        } 
        
        message = cache.get();
        return message.buffer;
    }
        
    /*
    public byte [] write(byte [] b, int off, int len, boolean lastPacket) {
        
       if (closed) { 
           System.err.println("____ got write(" + off + ", " + len + 
                   ") while closed!");
           return b;
       }
               
       if (firstPacket) {
           firstPacket = false;
           currentNUM = 0;
       } else { 
           currentNUM++;
       }
       
       if (lastPacket) { 
           currentNUM |= Message.LAST_PACKET;
       }
       
       
      // System.err.println("____ got write(" + currentID + " " + currentNUM 
       //       + ", byte[" + len + "] " + firstPacket + " " + lastPacket + ")");
              
       boolean reuse = mcast.send(currentID++, currentNUM, b, off, len);           

       if (reuse) {        
           return b;
       } else { 
           return new byte[b.length];
       }
    }
    */
    
    public void write(int b) throws IOException {
        // Ouch ... fortunately, it is never used...
        write(new byte[] { (byte) (b & 0xff) }, 0, 1);
    }
}
