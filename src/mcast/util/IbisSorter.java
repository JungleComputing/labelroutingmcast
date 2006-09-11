package mcast.util;

import java.util.Comparator;
import java.util.Arrays;


import ibis.ipl.IbisIdentifier;

public class IbisSorter implements Comparator {

    // General sorter to use when no cluster order is preferred. 
    private static final IbisSorter sorter = new IbisSorter("unknown", null); 
    
    private final String preferredCluster;
    private final String preferredName;
           
    private IbisSorter(String preferredCluster, String preferredName) { 
        this.preferredCluster = preferredCluster;
        this.preferredName = preferredName;
    }

    public static void sort(IbisIdentifier [] ids) { 
        sort(ids, 0, ids.length);
    }    

    public static void sort(IbisIdentifier local, IbisIdentifier [] ids) { 
        sort(local, ids, 0, ids.length);
    }

    
    public static void sort(IbisIdentifier [] ids, int from, int to) { 
        Arrays.sort(ids, from, to, sorter);
    }    

    public static void sort(IbisIdentifier local, IbisIdentifier [] ids, 
            int from, int to) { 
    
        IbisSorter tmp = sorter;
        
        if (!local.name().equals(sorter.preferredName) ||
                !local.cluster().equals(sorter.preferredCluster)) {            
            tmp = new IbisSorter(local.cluster(), local.name());
        }
        
        Arrays.sort(ids, from, to, tmp);
    }
    
    // Returns the index of the first character that is different in the two 
    // Strings. Thus, the higher the number returned, the longer the prefix that 
    // the two Strings share.  
    private static int firstDifference(String s1, String s2) { 
        
        // first, make sure that we s1 is the shortest string. 
        if (s1.length() > s2.length()) { 
            String tmp = s1; 
            s1 = s2; 
            s2 = tmp;
        }
        
        for (int i=0;i<s1.length();i++) { 
            if (s1.charAt(i) != s2.charAt(i)) { 
                return i;
            }
        }
        
        return s1.length();
    }
        
    public int compare(Object o1, Object o2) {

        IbisIdentifier id1 = (IbisIdentifier) o1;
        IbisIdentifier id2 = (IbisIdentifier) o2;
        
        String cluster1 = id1.cluster();
        String cluster2 = id2.cluster();
        
        if (cluster1.equals(cluster2)) { 
            // The clusters are identical, so the order depends completely 
            // on the names. 
            // 
            // For SMP awareness, we assume that the identifiers of two ibises 
            // on an SMP machine are 'closer' that the identifiers of two ibises
            // on different machines. This way, the identifiers will 
            // automatically be sorted 'pair-wise' (or quad/oct/etc, depending
            // on the number of ibises that share the SMP machines).
            // 
            // One aditional problem is that we want the ibises that share the 
            // machine with the sender to be first (which isn't that simple!). 
            
            if (preferredName == null) { 
                return id1.name().compareTo(id2.name());
            } else {
                // Figure out if one of the two strings has a longer prefix 
                // in common with 'preferredName'. Note that this will result 
                // in the lenght of the string only if the IbisIdentifier 
                // actually contains the 'preferredName'. Therefore, this 
                // IbisIdentifier will end up at the first position of the 
                // array, which is exactly what we want.  
                int d1 = firstDifference(preferredName, id1.name());
                int d2 = firstDifference(preferredName, id2.name());
                
                // If both have the same distance, we sort them alphabetically.
                // Otherwise, we prefer the one that is closest to 
                // 'preferredName', since these may actually be located on the 
                // same machine.  
                if (d1 == d2) { 
                    return id1.name().compareTo(id2.name());
                } else if (d1 <= d2) { 
                    return 1;
                } else { 
                    return -1;
                } 
            }
        } 
        
        // The clusters are different. If one of the two is equal to the 
        // preferredCluster, we want that one to win. Otherwise, we just return 
        // the 'natural order'.  
        if (cluster1.equals(preferredCluster)) { 
            return -1;
        } 
        
        if (cluster2.equals(preferredCluster)) {  
            return 1;
        } 

        return cluster1.compareTo(cluster2);
    }    
}
