package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;

/**
 * @Author: gsh
 * @Date: Created in 2018/4/17 9:31
 * @Description:
 */
public class HBase_Example {
    public static Configuration configuration;
    public static Connection connection;
    public static Admin admin;

    private static final String HBASE_ROOTDIR = "hbase.rootdir";
    private static final String ZK_QUORUM = "hbase.zookeeper.quorum";
    private static final String ZK_CLIENT_PORT = "hbase.zookeeper.property.clientPort";

    private static final String HDFS_DIR = "hdfs://localhost:9000/hbase";
    private static final String ZK_POS_IP = "localhost";
    private static final String ZK_PORT_VALUE = "2181";

    //主函数中的语句请逐句执行
    public static void main(String[] args) throws IOException {
        listTables();
        //createTable("Score", new String[]{"sname", "course"});
    }


    //建立链接
    public static void init() {
        configuration = HBaseConfiguration.create();
        configuration.set(HBASE_ROOTDIR, HDFS_DIR);
        configuration.set(ZK_QUORUM, ZK_POS_IP);
        configuration.set(ZK_CLIENT_PORT, ZK_PORT_VALUE);
        // configuration.set("hbase.master", "192.168.123.128:60000");
        try {
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //关闭链接
    public static void close() {
        try {
            if (admin != null) {
                admin.close();
            }
            if (null != connection) {
                connection.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 建表，HBase的表中有一个系统默认的属性作为主键，主键无需自行创建，默认为put命令操作中表名后的第一个数据，因此此处无需创建id列
     *
     * @param myTableName
     * @param colFamily
     * @throws IOException
     */
    public static void createTable(String myTableName, String[] colFamily) throws IOException {
        init();
        TableName tableName = TableName.valueOf(myTableName);
        if (admin.tableExists(tableName)) {
            System.out.println("table is exists!");
        } else {
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
            for (String str : colFamily) {
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(str);
                hTableDescriptor.addFamily(hColumnDescriptor);
            }
            admin.createTable(hTableDescriptor);
            System.out.println("create table success");
        }
        close();
    }

    public static void deleteTable(String tableName) throws IOException {
        init();
        TableName tn = TableName.valueOf(tableName);
        if (admin.tableExists(tn)) {
            admin.disableTable(tn);
            admin.deleteTable(tn);
        }
        close();
    }

    /**
     * 查看已有表
     *
     * @throws IOException
     */
    public static void listTables() throws IOException {
        init();
        HTableDescriptor hTableDescriptors[] = admin.listTables();
        for (HTableDescriptor hTableDescriptor : hTableDescriptors) {
            System.out.println(hTableDescriptor.getNameAsString());
        }
        close();
    }

    /**
     * 向某一行的某一列插入数据
     *
     * @param tableName 表名
     * @param rowkey    行健
     * @param colFamily 列族名
     * @param col       列名
     * @param val       值
     * @throws IOException
     */
    public static void insertRow(String tableName, String rowkey, String colFamily, String col, String val) throws IOException {
        init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(rowkey.getBytes());
        put.addColumn(colFamily.getBytes(), col.getBytes(), val.getBytes());
        table.put(put);
        table.close();
        close();
    }

    /**
     * 删除数据
     *
     * @param tableName 表名
     * @param rowkey    行键
     * @param colFamily 列族名
     * @param col
     * @throws IOException
     */
    public static void deleteRow(String tableName, String rowkey, String colFamily, String col) throws IOException {
        init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(rowkey.getBytes());
        //删除制定列族的所有数据
        //delete.addFamily(colFamily.getBytes());
        //删除指定列的数据
        //delete.addColumn(colFamily.getBytes(), col.getBytes());

        table.delete(delete);
        table.close();
        close();
    }

    /**
     * 根据行健rowkey查找数据
     *
     * @param tableName
     * @param rowkey
     * @param colFamily
     * @param col
     * @throws IOException
     */
    public static void getData(String tableName, String rowkey, String colFamily, String col) throws IOException {
        init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(rowkey.getBytes());
        get.addColumn(colFamily.getBytes(), col.getBytes());
        Result result = table.get(get);
        showCell(result);
        table.close();
        close();
    }

    /**
     * 格式化输出
     *
     * @param result
     */
    public static void showCell(Result result) {
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            System.out.println("RowName: " + new String(CellUtil.cloneRow(cell)) + "");
            System.out.println("Timetamp: " + cell.getTimestamp() + " ");
            System.out.println("column Family: " + new String(CellUtil.cloneFamily(cell)) + "  ");
            System.out.println("row Name:" + new String(CellUtil.cloneQualifier(cell)) + " ");
            System.out.println("value:" + new String(CellUtil.cloneValue(cell)) + " ");

        }
    }
}
