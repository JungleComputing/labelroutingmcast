/**
 * 
 */
package mcast.object;

class Buffer { 
    
    int id;
    int num;
    boolean last;
    byte [] buffer;
    
    Buffer next;
    
    void set(int id, int num, boolean last, byte [] buffer) { 
        this.id = id;
        this.num = num;
        this.last = last;
        this.buffer = buffer;
    }
}