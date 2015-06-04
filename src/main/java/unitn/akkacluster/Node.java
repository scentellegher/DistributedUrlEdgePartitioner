package unitn.akkacluster;


import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.japi.Creator;
import gnu.trove.map.hash.THashMap;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.Map;

//Dummy node
public class Node extends UntypedActor{
    private ActorRef master;
    private final int id; 
    private final String masterPath;
    
    public static Props props(final int id, final String mp) {
        return Props.create(new Creator<Node>() {
            @Override
            public Node create() throws Exception {
                return new Node(id,mp);
            }
        });
    }
    
    public Node(int id, String mp){
        this.id = id;
        masterPath = mp ;
    }
    
    File input;
    BufferedReader br;
    
    //map that contains domain_id -> size
    Map<Integer, Integer> map = new HashMap<Integer, Integer>();
    
    THashMap <Integer, Integer> assignmentMap = new THashMap<Integer, Integer>();
    
    @Override
    public void onReceive(Object message) throws Exception {
        input = new File("/home/cent/Desktop/webgraph/india2004/parts/part000"+id);
        br = new BufferedReader(new FileReader(input));
        String line;
        String [] tmp;
        
        if(message instanceof Message.REGISTER_NODE){
            getContext().actorSelection(masterPath).tell(new Message.REGISTER_NODE(id), self());
        } else if(message instanceof Message.START_DOMAIN_COMPUATATION){
            master = getSender();
            System.out.println("Node "+id+ " started domain computation");
            //domain size computation
            int domain = 0;
            int old_domain=0;
            int dim = 0;
            
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
            
            // dump the domains contained in my dataset part
            br = new BufferedReader(new FileReader(input));
            //open file writers for the partitions
            FileWriter [] files = new FileWriter[m.K];
            for(int i=0; i<m.K; i++){
                files[i]= new FileWriter(new File("/home/cent/Desktop/webgraph/india2004/partitions/node"+id+"_part_" + i));
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
            
            // dumped!
            master.tell(new Message.DUMPED(), self());
        } else if (message instanceof Message.SHUTDOWN){
            getContext().system().shutdown();
        } else {
            unhandled(message);
        }
    }

    
}
