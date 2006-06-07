/**
 * 
 */
package mcast.lrm;

public class Message { 
    
    public static final int LAST_PACKET = 1 << 31;
    
    public int id;
    public int num;
    public boolean last;
    
    public final byte [] buffer;
    public int len;
    
    public Message next;
        
    Message(int size) { 
        buffer = new byte[size];
    }
    
    void set(int id, int num, int len) { 
        this.id = id;
        this.len = len;
        this.num = (num & ~LAST_PACKET);        
        last = ((num & LAST_PACKET) != 0);
    }
}