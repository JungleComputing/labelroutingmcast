package mcast.lrm;

public class MessageCache {

    private final int MESSAGE_SIZE;
    private final int MAX_SIZE;
        
    private Message cache;
    private int size;
    
   // private long memoryUsage = 0;     
   // private int lowBound = 0;
   // private int highBound = 1024*1024;
      
    private long hits = 0;
    private long miss = 0;
    
    private long store = 0;    
    private long discard = 0;
    
    public MessageCache(int cacheSize, int messageSize) { 
        this.MAX_SIZE = cacheSize;
        this.MESSAGE_SIZE = messageSize;
        
        /*
        // fill the cache
        for (int i=0; i<MAX_SIZE; i++) {
            Message m = get();
            put(m);
        }
        */
    }
    
    public synchronized void setDestinationSize(int count) {
        Message tmp = cache;
        while(tmp != null) {
            if(tmp.destinations == null || tmp.destinations.length < count) {
                tmp.destinations = new short[count];
            }
            
            tmp = tmp.next;
        }
    }
    
    public synchronized void put(Message m) {         
        m.refcount--;
        if (m.refcount == 0) {
            if (size < MAX_SIZE && 
                    m.buffer != null && m.buffer.length == MESSAGE_SIZE) {
                
                m.next = cache;
                cache = m;
                size++;
                store++;
            } else {      
                m.next = null;
                discard++;
            }        
        }
    }
    
    /*
    public synchronized Message get(short sender, short [] destinations, 
            int id, int num, byte [] message, int off, int len, boolean local) { 
        
        if (size == 0) { 
            return new Message(sender, destinations, id, num, message, off, 
                    len, local);
        } 
        
        Message tmp = cache;
        cache = cache.next;
        tmp.next = null;
        size--;
        
        tmp.sender = sender;
        tmp.destinations = destinations; 
        tmp.id = id;
        tmp.num = num;
        tmp.buffer = message;
        tmp.off = off;
        tmp.len = len;
        tmp.local = local;
        tmp.last = false;
        
        return tmp;        
    }
     
    public Message get(int len, int dst) { 

        Message m = get(len);
        
        if (m.destinations == null || m.destinations.length != dst) {
            // TODO optimize!
            m.destinations = new short[dst];
        } 
        
        return m;
    }
    */
   
    
    public Message get(int len) {        
        if (len > MESSAGE_SIZE) { 
            // System.err.println("Creating new message of size " + len);
            miss++;
            return new Message(len);
        }
                
        return get();               
    }

    public synchronized Message get() {
        Message tmp = null;

        if (size == 0) {
            miss++;
            tmp =  new Message(MESSAGE_SIZE);
        } else {
            hits++;
            
            tmp = cache;
            cache = cache.next;
            size--;
        }
        tmp.next = null;
        tmp.local = false;
        tmp.refcount = 1;

        return tmp;               
    }
        
    
    public int getPrefferedMessageSize() {
        return MESSAGE_SIZE;
    }
}
