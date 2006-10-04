package mcast.object;

/**
 * Upcaller for message acknowledgements.
 */
public interface SendDoneUpcaller {
    /**
     * Notifies that the object multicaster has received and acknowledgement
     * from the last receiver for the specified send.
     * Note that the object multicaster is unreliable, messages can get lost,
     * so that it is not guaranteed that this upcall will occur for every
     * send.
     * @param sendId the identification of the send, as returned by
     * ObjectMulticaster.send().
     */
    public void sendDone(int sendId);
}
