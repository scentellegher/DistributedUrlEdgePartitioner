package unitn.akkacluster;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.japi.Creator;
import gnu.trove.map.hash.THashMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import unitn.akkacluster.Message.*;

public class Master extends UntypedActor {

    private final int K;
    
    private HashMap<Integer,ActorRef> nodes;

    public static Props props(final int id) {
        return Props.create(new Creator<Master>() {
            @Override
            public Master create() throws Exception {
                return new Master(id);
            }
        });
    }

    public Master(int k) {
        K = k;
        nodes = new HashMap();
    }

    int registered_cluster_counter = 0;
    int domain_computation_done_cluster_counter = 0;
    int dumped_cluster_counter = 0;
    //map that stores all the domain_id->size received from the slaves
    Map<Integer,Integer> fullMap = new HashMap<Integer, Integer>();
    
    @Override
    public void onReceive(Object message) throws Exception {
        if (message instanceof REGISTER_NODE) {
            int id = ((REGISTER_NODE)message).id;
            nodes.put(id,getSender());
            System.out.println("MASTER RECEIVED REGISTER " + id);
            registered_cluster_counter++;
            if (registered_cluster_counter == K) {
                for (ActorRef node : nodes.values()) {
                    node.tell(new START_DOMAIN_COMPUATATION(), self());
                }
            }

        } else if (message instanceof DOMAIN_COMPUTATION_DONE) {
            domain_computation_done_cluster_counter++;
            Message.DOMAIN_COMPUTATION_DONE m = (Message.DOMAIN_COMPUTATION_DONE) message;
//            System.out.println("map received with size="+m.domainsSize.size());
            fullMap.putAll(m.domainsSize);
            if (domain_computation_done_cluster_counter == K) {
                System.out.println("MASTER: All domain size received");
                System.out.println("MASTER: Map size= "+fullMap.size());
                
                // sort the map by size in descending order
                System.out.println("MASTER: sorting domains...");
                ValueComparator bvc =  new ValueComparator(fullMap);
                TreeMap<Integer, Integer> sortedDomains = new TreeMap<Integer, Integer>(bvc);
                sortedDomains.putAll(fullMap);
                System.out.println("MASTER: block to partition assignment...");
                
                //greedy assignment
                int min=0;
                int[] load = new int[K];
                Map<Integer, Integer> dom2part = new THashMap<Integer, Integer>();

                for (Map.Entry<Integer, Integer> entry : sortedDomains.entrySet()) {
                    //return the index of the partition with the smallest load
                    min = selectMinLoad(load,K);
                    //add the domain->partition assignment
                    dom2part.put(entry.getKey(), min);
                    //update load
                    load[min] += entry.getValue();           
                }
                for (ActorRef node : nodes.values()) {
                    //send to the slaves the domain -> partition assignment
                    node.tell(new ASSIGNMENT(dom2part), self());
                }                
            }
        } else if (message instanceof DUMPED) {
            dumped_cluster_counter++;
            if (dumped_cluster_counter == K){
               System.out.println("MASTER: dumped!"); 
               for (ActorRef node : nodes.values()) {
                    node.tell(new SHUTDOWN(), self());
               } 
               getContext().system().shutdown();
            }
        } else{
            System.out.println("MY PATH IS [" + this.getSelf().path() + "]");
        }
    }
    
    private static int selectMinLoad(int [] load, int K){
        int minload = Integer.MAX_VALUE;
        int min=-1;
        for(int i=0; i<K; i++){
            if(load[i]< minload){
                minload = load[i];
                min = i;
            }
        }
        return min;
    }
}
