package mcast.lrm;

import java.util.LinkedList;

public class MessageQueue {

    private LinkedList queue = new LinkedList();    
    private final int limit;
    
    public MessageQueue(int limit) { 
        this.limit = limit;
    }
    
    public synchronized void enqueue(Message m) { 
        
        while (queue.size() >= limit) { 
            try { 
                wait();
            } catch (Exception e) {
                // TODO: handle exception
            }
        }
        
        queue.addLast(m);
        notifyAll();
    }
    
    public synchronized Message dequeue() { 

        while (queue.size() == 0) { 
            try { 
                wait();
            } catch (Exception e) {
                // TODO: handle exception
            }
        }
        
        notifyAll();
        return (Message) queue.removeFirst();
    }
}
