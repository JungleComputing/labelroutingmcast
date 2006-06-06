/**
 * 
 */
package mcast.lrm;

public class Message { 
    
    public int id;
    public int num;
    public boolean last;
    
    public final byte [] buffer;
    public int len;
    
    public Message next;
        
    Message(int size) { 
        buffer = new byte[size];
    }
    
    void set(int id, int num, boolean last, int len) { 
        this.id = id;
        this.num = num;
        this.last = last;
        this.len = len;
    }
}