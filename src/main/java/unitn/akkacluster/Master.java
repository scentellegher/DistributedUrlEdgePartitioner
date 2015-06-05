package unitn.akkacluster;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.japi.Creator;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import gnu.trove.map.hash.THashMap;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
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

    //aws credentials for s3
    AWSCredentials credentials = new ProfileCredentialsProvider().getCredentials();
    AmazonS3 s3client = new AmazonS3Client(credentials);
    
    int registered_cluster_counter = 0;
    int domain_computation_done_cluster_counter = 0;
    int dumped_cluster_counter = 0;
    //map that stores all the domain_id->size received from the slaves
    Map<Integer,Integer> fullMap = new HashMap<Integer, Integer>();
    int value=0;
    
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

            for (Map.Entry<Integer, Integer> entry : m.domainsSize.entrySet()) {
                if(fullMap.containsKey(entry.getKey())){ //merge domains
                    value = fullMap.get(entry.getKey()) + entry.getValue();
                    fullMap.put(entry.getKey(),value);
                } else {
                    fullMap.put(entry.getKey(), entry.getValue());
                }
            }
            
            if (domain_computation_done_cluster_counter == K) {
                
                //create "parts" folder
                String folderName = "parts";
                createFolder("cent-dataset/india2004", folderName, s3client);
                
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
                for(int i=0; i< load.length; i++){
                    System.out.println("load["+i+"]= "+load[i]);
                }
                for (ActorRef node : nodes.values()) {
                    //send to the slaves the domain -> partition assignment
                    node.tell(new ASSIGNMENT(dom2part, K), self());
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
    
    //fuction that create a folder into a specific bucket
    public static void createFolder(String bucketName, String folderName, AmazonS3 client) {
        // create meta-data for your folder and set content-length to 0
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(0);
        
        // create empty content
        InputStream emptyContent = new ByteArrayInputStream(new byte[0]);
        
        // create a PutObjectRequest passing the folder name suffixed by /
        PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName,
                folderName + "/", emptyContent, metadata);
        
        // send request to S3 to create folder
        client.putObject(putObjectRequest);
    }
}
