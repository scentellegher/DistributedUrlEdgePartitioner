package unitn.akkacluster;


import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.japi.Creator;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import gnu.trove.map.hash.THashMap;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

//Dummy node
public class Node extends UntypedActor{
    private ActorRef master;
    private final int id; 
    private final String masterPath;
    private final String dataset;
    private final String localDir;
    
    public static Props props(final int id, final String mp, final String d, final String l) {
        return Props.create(new Creator<Node>() {
            @Override
            public Node create() throws Exception {
                return new Node(id,mp,d,l);
            }
        });
    }
    
    public Node(int id, String mp, String d, String l){
        this.id = id;
        masterPath = mp ;
        dataset = d;
        localDir = l;
    }
    
        
    //map that contains domain_id -> size
    Map<Integer, Integer> map = new HashMap<Integer, Integer>();
    THashMap <Integer, Integer> assignmentMap = new THashMap<Integer, Integer>();
    
    //aws credential for s3
    AWSCredentials credentials = new ProfileCredentialsProvider().getCredentials();
    AmazonS3 s3client = new AmazonS3Client(credentials);   
    
    String line;
    String [] tmp;
    
    @Override
    public void onReceive(Object message) throws Exception {
            
        if(message instanceof Message.REGISTER_NODE){
            getContext().actorSelection(masterPath).tell(new Message.REGISTER_NODE(id), self());
        } else if(message instanceof Message.START_DOMAIN_COMPUATATION){
            master = getSender();
            System.out.println("Node "+id+ " started domain computation");
            //domain size computation
            int domain = 0;
            int old_domain=0;
            int dim = 0;
            
            S3Object object = s3client.getObject(new GetObjectRequest(dataset, "part000"+id));
            BufferedReader br = new BufferedReader(new InputStreamReader(object.getObjectContent()));
            
            //compute domain sizes
            while((line = br.readLine())!=null){
                tmp = line.split(" ");
                domain = Integer.parseInt(tmp[0]);
                
                if(old_domain == domain){
                    dim++;
                } else {
                    map.put(old_domain, dim);
                    old_domain = domain;
                    dim = 1;
                }
            }
            map.put(old_domain, dim);
            br.close();
            
            // send map to the master
            master.tell(new Message.DOMAIN_COMPUTATION_DONE(map), self());
        } else if(message instanceof Message.ASSIGNMENT){
            master = getSender();
            System.out.println("Node "+id+ " started assignment");
            
            //dump based on assignment
            Message.ASSIGNMENT m = (Message.ASSIGNMENT) message;
            assignmentMap.putAll(m.dom2part);
            
            S3Object object = s3client.getObject(new GetObjectRequest(dataset, "part000"+id));
            BufferedReader br = new BufferedReader(new InputStreamReader(object.getObjectContent()));
            
            // dump the domains contained in my dataset part
            br = new BufferedReader(new InputStreamReader(object.getObjectContent()));
            //open file writers for the partitions
            FileWriter [] files = new FileWriter[m.K];
            for(int i=0; i<m.K; i++){
                files[i]= new FileWriter(new File("/home/ubuntu/datasets/"+localDir+"/partitions/part_" + i + "_node_"+id));
            }
            
            int domain = 0;
            String edge;
            while((line = br.readLine())!=null){
                tmp = line.split(" ");
                domain = Integer.parseInt(tmp[0]);
                if(map.containsKey(domain)){
                    edge = tmp[1]+" "+tmp[2];
                    files[assignmentMap.get(domain)].write(edge+"\n");
                }
            }
           
            // close file writers
            for(int j=0; j<m.K; j++){
                files[j].close();
            }
            br.close();
            
            System.out.println("Node "+id+" is uploading to s3...");
            //upload partitions parts to S3
            String fileName;
            for(int i=0; i<m.K; i++){
                fileName = "part_" + i +"_node_"+id;
                s3client.putObject(new PutObjectRequest(dataset+"/parts", fileName, new File("/home/ubuntu/datasets/"+localDir+"/partitions/part_" + i + "_node_"+id)));
            }            
            
            // dumped!
            master.tell(new Message.DUMPED(), self());
        } else if (message instanceof Message.SHUTDOWN){
            getContext().system().shutdown();
        } else {
            unhandled(message);
        }
    }

    
}