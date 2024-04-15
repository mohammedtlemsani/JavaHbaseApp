package ma.enset;



import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

public class Main {
    public static final String TABLE_NAME = "students";
    public static final String CF_INFO = "info";
    public static final String CF_GRADES = "grades";

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
            builder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(CF_INFO));
            builder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(CF_GRADES));

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

            // Etudiant 1
            Put putStudent1 = new Put(Bytes.toBytes("student1"));
            putStudent1.addColumn(Bytes.toBytes(CF_INFO), Bytes.toBytes("name"), Bytes.toBytes("John Doe"));
            putStudent1.addColumn(Bytes.toBytes(CF_INFO), Bytes.toBytes("age"), Bytes.toBytes("20"));
            putStudent1.addColumn(Bytes.toBytes(CF_GRADES), Bytes.toBytes("math"), Bytes.toBytes("B"));
            putStudent1.addColumn(Bytes.toBytes(CF_GRADES), Bytes.toBytes("science"), Bytes.toBytes("A"));
            table.put(putStudent1);

            // Etudiant 2
            Put putStudent2 = new Put(Bytes.toBytes("student2"));
            putStudent2.addColumn(Bytes.toBytes(CF_INFO), Bytes.toBytes("name"), Bytes.toBytes("Jane Smith"));
            putStudent2.addColumn(Bytes.toBytes(CF_INFO), Bytes.toBytes("age"), Bytes.toBytes("22"));
            putStudent2.addColumn(Bytes.toBytes(CF_GRADES), Bytes.toBytes("math"), Bytes.toBytes("A"));
            putStudent2.addColumn(Bytes.toBytes(CF_GRADES), Bytes.toBytes("science"), Bytes.toBytes("A"));
            table.put(putStudent2);

            System.out.println("Data inserted successfully");

            // Fetch and display all information for "student1"
            Get getStudent1 = new Get(Bytes.toBytes("student1"));
            Result resultStudent1 = table.get(getStudent1);
            System.out.println("Information for student1:");
            System.out.println("Name: " + Bytes.toString(resultStudent1.getValue(Bytes.toBytes(CF_INFO), Bytes.toBytes("name"))));
            System.out.println("Age: " + Bytes.toString(resultStudent1.getValue(Bytes.toBytes(CF_INFO), Bytes.toBytes("age"))));
            System.out.println("Math Grade: " + Bytes.toString(resultStudent1.getValue(Bytes.toBytes(CF_GRADES), Bytes.toBytes("math"))));
            System.out.println("Science Grade: " + Bytes.toString(resultStudent1.getValue(Bytes.toBytes(CF_GRADES), Bytes.toBytes("science"))));

            // Update Jane Smith's age and math grade
            Put putUpdateJane = new Put(Bytes.toBytes("student2"));
            putUpdateJane.addColumn(Bytes.toBytes(CF_INFO), Bytes.toBytes("age"), Bytes.toBytes("23"));
            putUpdateJane.addColumn(Bytes.toBytes(CF_GRADES), Bytes.toBytes("math"), Bytes.toBytes("A+"));
            table.put(putUpdateJane);
            System.out.println("Jane Smith's age and math grade updated successfully");

            // Delete student1
            Delete deleteStudent1 = new Delete(Bytes.toBytes("student1"));
            table.delete(deleteStudent1);
            System.out.println("Student1 deleted successfully");

            // Fetch and display information for all students
            Scan scan = new Scan();
            ResultScanner scanner = table.getScanner(scan);
            System.out.println("Information for all students:");
            for (Result result : scanner) {
                System.out.println("Row Key: " + Bytes.toString(result.getRow()));
                System.out.println("Name: " + Bytes.toString(result.getValue(Bytes.toBytes(CF_INFO), Bytes.toBytes("name"))));
                System.out.println("Age: " + Bytes.toString(result.getValue(Bytes.toBytes(CF_INFO), Bytes.toBytes("age"))));
                System.out.println("Math Grade: " + Bytes.toString(result.getValue(Bytes.toBytes(CF_GRADES), Bytes.toBytes("math"))));
                System.out.println("Science Grade: " + Bytes.toString(result.getValue(Bytes.toBytes(CF_GRADES), Bytes.toBytes("science"))));
                System.out.println("--------------------");
            }

            // Close connection
            scanner.close();
            table.close();
            connection.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}