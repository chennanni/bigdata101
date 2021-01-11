package max.learn.zookeeper;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.util.Date;
import java.util.List;

/**
 * 首先，需要在云端服务器上启动 Zookeeper 组件。
 * 启动程序，将 Client 连上 Server，进行 node 的基本 CRUD 操作。
 */
public class ZkClient {

    private static String HOST = "aliyun";
    private static String FOLDER = "/myTestNode";

    public static void main (String args[]) throws Exception
    {
        ZkConnect connector = new ZkConnect();
        ZooKeeper zk = connector.connect(HOST);

        // delete if existing
        String newNode = FOLDER + new Date().getTime();
        Stat stat = zk.exists(newNode, false);
        if (stat != null) {
            System.out.println("0. Delete existing node: " + newNode);
            connector.deleteNode(newNode);
        }
        // create node
        connector.createNode(newNode, "test data 1".getBytes());
        System.out.println("1. Create node: " + newNode);

        // get all nodes
        System.out.println("2. Get all nodes...");
        List<String> zNodes = zk.getChildren("/", true);
        for (String zNode: zNodes)
        {
            System.out.println("### ChildrenNode: " + zNode);
        }

        // get node data
        byte[] data = zk.getData(newNode, true, zk.exists(newNode, true));
        System.out.print("3. Get node data: ");
        for ( byte dataPoint : data)
        {
            System.out.print ((char)dataPoint);
        }
        System.out.print("\n");

        // change node data
        connector.updateNode(newNode, "test data 2".getBytes());
        data = zk.getData(newNode, true, zk.exists(newNode, true));
        System.out.print("4. Modify node data: ");
        for ( byte dataPoint : data)
        {
            System.out.print ((char)dataPoint);
        }
        System.out.print("\n");
    }
}
