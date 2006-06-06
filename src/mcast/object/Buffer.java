/**
 * 
 */
package mcast.object;

class Buffer { 
    
    int id;
    int num;
    boolean last;
    byte [] buffer;
    int len;
    
    Buffer next;
    
    void set(int id, int num, boolean last, byte [] buffer, int len) { 
        this.id = id;
        this.num = num;
        this.last = last;
        this.buffer = buffer;
        this.len = len;
    }
}