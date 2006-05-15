package lrmcast;

import ibis.ipl.IbisIdentifier;

public interface ByteArrayReceiver {
    public boolean gotMessage(IbisIdentifier sender, byte [] message);    
}
