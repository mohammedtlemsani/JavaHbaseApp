package ma.enset;



import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

public class Main {
    public static final String TABLE_NAME = "users";
    public static final String CF_PERSONAL_DATA = "personal data";
    public static final String CF_PROFESSIONAL_DATA = "professional data";

    public static void main(String[] args) {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "zookeeper");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.master", "hbase-master:16000");

        try {
            Connection connection = ConnectionFactory.createConnection(conf);
            Admin admin = connection.getAdmin();
            TableName tableName = TableName.valueOf(TABLE_NAME);
            TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableName);

            // Define column families
            builder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(CF_PERSONAL_DATA));
            builder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(CF_PROFESSIONAL_DATA));

            TableDescriptor tableDescriptor = builder.build();

            // Check if the table already exists, if not, create it
            if (!admin.tableExists(tableName)) {
                admin.createTable(tableDescriptor);
                System.out.println("Table created successfully.");
            } else {
                System.out.println("Table already exists.");
            }

            // Insert data into the table
            Table table = connection.getTable(tableName);
            Put put = new Put(Bytes.toBytes("1111"));
            put.addColumn(Bytes.toBytes(CF_PERSONAL_DATA), Bytes.toBytes("name"), Bytes.toBytes("Mohammed Tlemsani"));
            put.addColumn(Bytes.toBytes(CF_PERSONAL_DATA), Bytes.toBytes("email"), Bytes.toBytes("mohammed@tlemsani.com"));
            put.addColumn(Bytes.toBytes(CF_PROFESSIONAL_DATA), Bytes.toBytes("company"), Bytes.toBytes("enset"));
            table.put(put);
            System.out.println("Data inserted successfully");

            // Close connection
            table.close();
            connection.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}