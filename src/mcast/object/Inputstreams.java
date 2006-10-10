package mcast.object;

import ibis.util.GetLogger;

import mcast.lrm.Message;
import mcast.lrm.MessageCache;

import org.apache.log4j.Logger;

public class Inputstreams {

    private static final int DEFAULT_SIZE = 64;

    private static final Logger logger
            = GetLogger.getLogger(Inputstreams.class.getName());

    private LRMCInputStream[] inputStreams = new LRMCInputStream[DEFAULT_SIZE];

    private boolean[] hasData = new boolean[DEFAULT_SIZE];

    private boolean[] busy = new boolean[DEFAULT_SIZE];

    private int streamsWithData = 0;

    private int index = 0;

    private int last = -1;
    
    private void add(LRMCInputStream is, short sender) {
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
        System.arraycopy(busy, 0, tmp3, 0, busy.length);
        busy = tmp3;
    }

    public synchronized LRMCInputStream get(short sender, MessageCache cache,
            ObjectMulticaster om) {
        LRMCInputStream tmp = find(sender);
        if (tmp == null) {
            tmp = new LRMCInputStream(sender, cache, om);
            add(tmp, sender);
        }
        return tmp;
    }

    private LRMCInputStream find(short sender) {
        if(sender < 0 || sender > last) {
            return null;
        }
        
        return inputStreams[sender];
    }

    public synchronized void returnStream(LRMCInputStream is) {
        busy[is.getSource()] = false;
        if (is.haveData()) {
            if (logger.isDebugEnabled()) {
                logger.debug("return stream " + is.getSource()
                        + ", still has data");
            }
            hasData(is);
        } else {
            if (logger.isDebugEnabled()) {
                logger.debug("return stream " + is.getSource()
                        + ", no data left");
            }
        }
    }

    public synchronized boolean hasData(LRMCInputStream is, Message m) {
        if (is.addMessage(m)) {
            hasData(is);
            return true;
        }
        return false;
    }

    public synchronized void hasData(LRMCInputStream is) {
        int src = is.getSource();
        if (! hasData[src] && ! busy[src]) {
            // Fix: Test before setting and incrementing counter (Ceriel)
            // Fix: Don't set hasData while it is busy. This may be incorrect
            // when we are still reading from the stream. We will see if
            // there is new data when we return the stream. (Ceriel)
            hasData[src] = true;
            if (logger.isDebugEnabled()) {
                logger.debug("Setting hasData for stream " + src);
                if (! is.haveData()) {
                    logger.debug("Set hasData but no data?", new Throwable());
                }
            }
            streamsWithData++;
        
            if (streamsWithData == 1) { 
                notifyAll();
            }
        }
    }

    public synchronized LRMCInputStream getNextFilledStream() {
        
        while (streamsWithData == 0) {
            try {
                wait(2000);
            } catch (InterruptedException e) {
                return null;
            }
            if (Thread.interrupted()) {
                return null;
            }
        }

        final int size = inputStreams.length;
        
        for (int i=1;i<=size;i++) {
            if (hasData[(index + i) % size]) {
                index = (index + i) % size;
                break;
            }
        }

        if (logger.isDebugEnabled() && ! hasData[index]) {
            logger.debug("GetNextFilledStream returns !hasData stream"
                    + ", streamsWithData = " + streamsWithData
                    + ", index = " + index, new Throwable());
        }

        hasData[index] = false;
        if (logger.isDebugEnabled()) {
            logger.debug("start read from stream " + index);
        }

        busy[index] = true;

        if (logger.isDebugEnabled() && ! inputStreams[index].haveData()) {
            logger.debug("GetNextFilledStream returns empty stream"
                    + ", streamsWithData = " + streamsWithData
                    + ", index = " + index, new Throwable());
        }

        streamsWithData--;

        return inputStreams[index];
    }
}
