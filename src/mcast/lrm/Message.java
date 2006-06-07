/**
 * 
 */
package mcast.lrm;

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
    
    Message(int size) { 
        buffer = new byte[size];
    }
   
    Message(String sender, String [] destinations, int id, int num, byte [] buffer, int off, int len) { 
        this.destinations = destinations;
        this.id = id;
        this.num = num;
        this.buffer = buffer;
        this.off = off;
        this.len = len;
    }   
    
    void set(String sender, String [] destinations, int id, int num, int off, int len) {
        
        this.sender = sender;
        this.destinations = destinations;
        
        this.off = off;        
        this.len = len;
        
        this.id = id;
        this.num = (num & ~LAST_PACKET);        
        last = ((num & LAST_PACKET) != 0);
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
