/**
 * 
 */
package mcast.lrm;

import java.io.IOException;


import ibis.ipl.ReadMessage;
import ibis.ipl.WriteMessage;

public class Message { 
    
    public static final int LAST_PACKET = 1 << 31;
    
    public short sender;
    
    public short [] destinations;
        
    public int id;
    public int num;

    public byte [] buffer;
    public int off;    
    public int len;
    
    public boolean last = false;
    public boolean local;
    
    public Message next;
        
    //private int useCount = 0;
    
    Message() {       
    }
       
    Message(int size, int destSize) { 
        buffer = new byte[size];
        destinations = new short[destSize];
            }
   
    Message(short sender, short [] destinations, int id, int num, 
            byte [] buffer, int off, int len, boolean local) { 
        this.sender = sender;
        this.destinations = destinations;
        this.id = id;
        this.num = num;
        this.buffer = buffer;
        this.off = off;
        this.len = len;
        this.local = local;
    }   
           
    void read(ReadMessage rm, int len, int dst) throws IOException { 

        this.off = 0;
        this.len = len;
        
        sender = rm.readShort();        
        id = rm.readInt();
        num = rm.readInt();            
        
        last = ((num & LAST_PACKET) != 0);

        if (last) { 
            num &= ~LAST_PACKET; 
        }
        
        if (len > 0) { 
            rm.readArray(buffer, 0, len);
        } 
        
        if (dst > 0) {
            // TODO optimize!            
            if (destinations == null || destinations.length < dst) {
                destinations = new short[dst];
            } 
        
            rm.readArray(destinations, 0, dst);
        } else { 
            destinations = null;
        }
    } 
    
    void write(WriteMessage wm, int fromDest) throws IOException { 
        
        int destinationLength = destinations.length-fromDest; 
        
        // First write the two variable lengths present in the message.   
        wm.writeInt(len);                
        wm.writeInt(destinationLength);
        
        // Then write the content that guaranteed to be there        
        wm.writeShort(sender);
        wm.writeInt(id);
        
        if (last) { 
            wm.writeInt(num | Message.LAST_PACKET);
        } else { 
            wm.writeInt(num);            
        }        
        
        // Finally write the actual data that has a variable size        
        if (len > 0) { 
            wm.writeArray(buffer, off, len);
        }
        
        if (destinationLength > 0) { 
            wm.writeArray(destinations, fromDest, destinationLength);
        }                                           
    }    
}
