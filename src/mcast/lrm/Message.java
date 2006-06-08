/**
 * 
 */
package mcast.lrm;

import java.io.IOException;


import ibis.ipl.ReadMessage;
import ibis.ipl.WriteMessage;

public class Message { 
    
    public static final int LAST_PACKET = 1 << 31;
    
    public String sender;
    public String [] destinations;
    
    public int id;
    public int num;
    public boolean last;
    
    public final byte [] buffer;
    public int off;    
    public int len;
    
    public Message next;
        
    //private int useCount = 0;
    
    Message(int size, int destSize) { 
        buffer = new byte[size];
        destinations = new String[destSize];
    }
   
    Message(String sender, String [] destinations, int id, int num, byte [] buffer, int off, int len) { 
        this.destinations = destinations;
        this.id = id;
        this.num = num;
        this.buffer = buffer;
        this.off = off;
        this.len = len;
    }   
    
    /*
    void set(String sender, String [] destinations, int id, int num, int off, int len) {
        
        this.sender = sender;
        this.destinations = destinations;
        
        this.off = off;        
        this.len = len;
        
        this.id = id;
        this.num = (num & ~LAST_PACKET);        
        last = ((num & LAST_PACKET) != 0);
    }
    */
    
    void read(ReadMessage rm, int len, int dst) throws IOException { 

        this.off = 0;
        this.len = len;
        
        sender = rm.readString();        
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
            if (destinations == null || destinations.length != dst) {
                destinations = new String[dst];
            } 
        
            for (int i=0;i<dst;i++) { 
                destinations[i] = rm.readString();
            }
        } else { 
            destinations = null;
        }
    } 
            
    void write(WriteMessage wm, int fromDest) throws IOException { 
        
        // First write the two variable lengths present in the message.        
        wm.writeInt(len);                
        wm.writeInt(destinations.length-fromDest);
        
        // Then write the content that guaranteed to be there        
        wm.writeString(sender);

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
        
        for (int i=fromDest;i<destinations.length;i++) { 
            wm.writeString(destinations[i]);
        }                                             
    }
    
    /*
    synchronized void up() { 
        useCount++;
    }
    
    synchronized int down() { 
        return --useCount;
    }
    */
}
