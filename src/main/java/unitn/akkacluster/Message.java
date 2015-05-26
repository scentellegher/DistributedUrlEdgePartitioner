package unitn.akkacluster;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class Message {
    public static class REGISTER_NODE implements Serializable{
        public final int id;
        public REGISTER_NODE(int i){
            id = i;
        }
    }
     
    public static class START_DOMAIN_COMPUATATION implements Serializable{
    
    }
    
    public static class DOMAIN_COMPUTATION_DONE implements Serializable{
        final Map<Integer, Integer> domainsSize;

        public DOMAIN_COMPUTATION_DONE(Map<Integer, Integer> domains) {
            domainsSize = domains;
        }
    }
    
    public static class ASSIGNMENT implements Serializable{
        final Map<Integer, Integer> dom2part;

        public ASSIGNMENT(Map<Integer, Integer> assignment) {
            dom2part = assignment;
        }
    }
    
    public static class DUMPED implements Serializable{
    
    }
    
    public static class SHUTDOWN implements Serializable{
        
    }
}
