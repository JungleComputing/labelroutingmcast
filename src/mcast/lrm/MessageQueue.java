package mcast.lrm;

public class MessageQueue {

    private final int limit;
    
    private Message head; 
    private Message tail; 
    
    private int size = 0;

    public MessageQueue() { 
        // no limit...
        this(Integer.MAX_VALUE);
    }
    
    public MessageQueue(int limit) { 
        this.limit = limit;
    }
    
    public synchronized int size() { 
        return size;
    }
    
    public synchronized void enqueue(Message m) { 
        
        if (m.len == 0) {
            System.out.println("Dangerous enqueue! len = 0");
            (new Throwable()).printStackTrace();
        }
        while (size >= limit) {            
            try { 
                wait();
            } catch (Exception e) {
                // TODO: handle exception
            }
        }
        
        if (head == null) { 
            head = tail = m;
            m.next = null;
            notifyAll();
        } else { 
            tail.next = m;
            tail = m;            
        }
        
        size++;
    }
    
    public synchronized Message dequeue() { 

        while (size == 0) { 
            try { 
                wait();
            } catch (Exception e) {
                // TODO: handle exception
            }
        }
                
        Message tmp = head;         
        head = head.next;
        tmp.next = null;
        
        size--;

        if (size == limit-1) { 
            notifyAll();
        }
        
        return tmp;
    }

    public synchronized Message dequeue(long millis) { 

        if (size == 0) { 
            try { 
                wait(millis);
            } catch (Exception e) {
                // TODO: handle exception
            }
        }
        if (size == 0) {
            return null;
        }

        return dequeue();
    }
}
