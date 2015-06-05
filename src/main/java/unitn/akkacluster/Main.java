package unitn.akkacluster;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.File;
import java.util.Scanner;
import unitn.akkacluster.Message.REGISTER_NODE;

public class Main {

    public static String ip;
    
    // If I am the master, I create the system and create the Master Actor
    public static void master() {
        Config config = ConfigFactory.parseString("akka.remote.netty.tcp.hostname=" + ip)
                .withFallback(ConfigFactory.parseFile(new File("application.conf")));
        Integer slaves = config.getInt("unitn.slaves");
        ActorSystem system = ActorSystem.create("ClusterSystem", config);
        ActorRef master = system.actorOf(Master.props(slaves), "master");
    }

    //If I am the node, I need to connect with the master.
    public static void node(int id, String dataset, String localDir) {
        
        int port = 0; //random port
        Config config = ConfigFactory.parseString("akka.remote.netty.tcp.port=" + port)
                .withFallback(ConfigFactory.parseString("akka.remote.netty.tcp.hostname=" + ip))
                .withFallback(ConfigFactory.parseFile(new File("application.conf")));
        
        String masterPath = (String) config.getList("akka.cluster.seed-nodes").get(0).unwrapped() + "/user/master"; //read the path of Master Actor

        ActorSystem system = ActorSystem.create("ClusterSystem", config);
        ActorRef node = system.actorOf(Node.props(id, masterPath, dataset, localDir), "node" + id);
        node.tell(new REGISTER_NODE(id), null); //Tell the node actor to talk to the master node
    }
    
    public static void main(String[] argv) throws InterruptedException {
        if(argv.length<4){
            System.err.println("Usage: [ec2/local] [master/all/nodeid] [s3 folder path] [local folder]");
            System.exit(1);
        }
        if(argv[0].equals("ec2")){
            ip = getIP();
        }else if(argv[0].equals("local")){
            ip = "127.0.0.1";
        }

        if (argv[1].equals("master")) {
            master();
        } else if (argv[1].equals("all")) {
            master();
            Thread.sleep(5000);
            node(0, argv[2], argv[3]);
            node(1, argv[2], argv[3]);
            node(2, argv[2], argv[3]);
            node(3, argv[2], argv[3]);
        } else {
            int id = Integer.parseInt(argv[1]);
            node(id, argv[2], argv[3]);
        }
    }

    //Gets local ip via semi-ugly curl call
    public static String getIP() {
        try {
            Process p = Runtime.getRuntime().exec("curl http://169.254.169.254/latest/meta-data/local-ipv4");
            int returnCode = p.waitFor();
            if (returnCode == 0) {
                Scanner s = new Scanner(p.getInputStream());
                String ip = s.nextLine();
                s.close();
                return ip;
            } else {
                return null;
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            System.exit(1);
            return null;
        }
    }
}
