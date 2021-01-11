package max.learn.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class HbaseTest {
    Connection connection = null;
    Table table = null;
    Admin admin = null;

    String tableName = "my_hbase_java_api";

    @Before
    public void setUp() {
        Configuration configuration = new Configuration();
        configuration.set("hbase.rootdir", "hdfs://hadoop000:9000/hbase");
        configuration.set("hbase.zookeeper.quorum", "hadoop000:2181");
        configuration.set("hbase.cluster.distributed", "true");
        configuration.set("host.name", "172.19.183.52");

        try {
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();

            Assert.assertNotNull(connection);
            Assert.assertNotNull(admin);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void getConnection() {

    }

    @After
    public void tearDown() {
        try {
            connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void printResult(Result result) {
        for (Cell cell : result.rawCells()) {
            System.out.println(Bytes.toString(result.getRow()) + "\t "
                    + Bytes.toString(CellUtil.cloneFamily(cell)) + "\t"
                    + Bytes.toString(CellUtil.cloneQualifier(cell)) + "\t"
                    + Bytes.toString(CellUtil.cloneValue(cell)) + "\t"
                    + cell.getTimestamp());
        }
    }

    @Test
    public void createTable() throws Exception {
        TableName table = TableName.valueOf(tableName);
        if (admin.tableExists(table)) {
            System.out.println(tableName + " 已经存在...");
        } else {
            HTableDescriptor descriptor = new HTableDescriptor(table);
            descriptor.addFamily(new HColumnDescriptor("info"));
            descriptor.addFamily(new HColumnDescriptor("address"));
            admin.createTable(descriptor);
            System.out.println(tableName + " 创建成功...");
        }
    }

    @Test
    public void queryTableInfos() throws Exception {
        HTableDescriptor[] tables = admin.listTables();
        if (tables.length > 0) {
            for (HTableDescriptor table : tables) {
                System.out.println(table.getNameAsString());
                HColumnDescriptor[] columnDescriptors = table.getColumnFamilies();
                for (HColumnDescriptor hColumnDescriptor : columnDescriptors) {
                    System.out.println("\t" + hColumnDescriptor.getNameAsString());
                }
            }
        }
    }
}
