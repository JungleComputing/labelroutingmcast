package mcast.object;

public class BufferCache {

    private final int MAX_SIZE;
    
    private Buffer cache;
    private int size;
    
    BufferCache(int max) { 
        this.MAX_SIZE = max;
    }
    
    public synchronized void put(Buffer b) { 
            
        if (size < MAX_SIZE) {
            b.buffer = null;
            b.next = cache;
            cache = b;
            size++;
        } else {             
            b.buffer = null;
            b.next = null;
        }
    }
    
    public synchronized Buffer get() { 
        
        if (size == 0) { 
            return new Buffer();
        } 
        
        Buffer tmp = cache;
        cache = cache.next;
        tmp.next = null;
        size--;
        
        return tmp;        
    }    
}
