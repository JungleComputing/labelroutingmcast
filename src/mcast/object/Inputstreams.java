package mcast.object;

import mcast.lrm.Message;

public class Inputstreams {

    private static final int DEFAULT_SIZE = 64;

    private LRMCInputStream[] inputStreams = new LRMCInputStream[DEFAULT_SIZE];

    private boolean[] hasData = new boolean[DEFAULT_SIZE];

    private boolean[] busy = new boolean[DEFAULT_SIZE];

    private int streamsWithData = 0;

    private int index = 0;

    private int last = -1;
    
    public synchronized void add(LRMCInputStream is, short sender) {
        if (sender >= inputStreams.length) {
            resize(sender);
        }

        inputStreams[sender] = is;
        if (sender > last) {
            last = sender;
        }
    }

    private void resize(int minimumSize) {
        int newSize = hasData.length;

        while (newSize <= minimumSize) {
            newSize *= 2;
        }

        LRMCInputStream[] tmp1 = new LRMCInputStream[newSize];
        System.arraycopy(inputStreams, 0, tmp1, 0, inputStreams.length);
        inputStreams = tmp1;

        boolean[] tmp2 = new boolean[newSize];
        System.arraycopy(hasData, 0, tmp2, 0, hasData.length);
        hasData = tmp2;

        boolean[] tmp3 = new boolean[newSize];
        System.arraycopy(busy, 0, tmp2, 0, busy.length);
        busy = tmp2;
    }

    public synchronized LRMCInputStream find(short sender) {
        if(sender < 0 || sender > last) {
            return null;
        }
        
        return inputStreams[sender];
    }

    public synchronized void returnStream(LRMCInputStream is) {
        busy[is.getSource()] = false;
        if (is.haveData()) {
            hasData(is);
        }
    }

    public synchronized void hasData(LRMCInputStream is, Message m) {
        is.addMessage(m);
        hasData(is);
    }

    public synchronized void hasData(LRMCInputStream is) {
        int src = is.getSource();
        if (! hasData[src] && ! busy[src]) {
            // Fix: Test before setting and incrementing counter (Ceriel)
            // Fix: Don't set hasData while it is busy. This may be incorrect
            // when we are still reading from the stream. We will see if
            // there is new data when we return the stream. (Ceriel)
            hasData[src] = true;
            // System.out.println("hasData " + is.getSource());
            streamsWithData++;
        
            if (streamsWithData == 1) { 
                notifyAll();
            }
        }
    }

    public synchronized LRMCInputStream getNextFilledStream() {
        
        while (streamsWithData == 0) {
            try {
                wait();
            } catch (Exception e) {
                // ignore
            }
        }

        final int size = inputStreams.length;
        
        for (int i=1;i<=size;i++) {
            if (hasData[(index + i) % size]) {
                index = (index + i) % size;
                hasData[index] = false;
                break;
            }
        }

        streamsWithData--;

        // System.out.println("nextFilled " + inputStreams[index].getSource());

        busy[index] = true;

        return inputStreams[index];
    }
}
