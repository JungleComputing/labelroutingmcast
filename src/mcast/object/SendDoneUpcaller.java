package mcast.object;

/**
 * Upcaller for message acknowledgements.
 */
public interface SendDoneUpcaller {
    /**
     * Notifies that the object multicaster has received and acknowledgement
     * from the last receiver for the specified send. The object multicaster
     * numbers its sends from 1 upwards.
     * Note that the object multicaster is unreliable, messages can get lost,
     * so that it is not guaranteed that this upcall will occur.
     * @param id the send number that is acknowledged.
     */
    public void sendDone(int id);
}
