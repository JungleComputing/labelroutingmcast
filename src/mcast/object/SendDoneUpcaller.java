package mcast.object;

/**
 * Upcaller for message acknowledgements.
 */
public interface SendDoneUpcaller {
    /**
     * Notifies that the object multicaster has received and acknowledgement
     * from the last receiver for the send that was passed this
     * <code>SendDoneUpcaller</code>.
     * Note that the object multicaster is unreliable, messages can get lost,
     * so that it is not guaranteed that this upcall will occur.
     */
    public void sendDone();
}
