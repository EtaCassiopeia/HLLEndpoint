package com.github.etacassiopeia.hbase.endpoint;

import com.github.etacassiopeia.hbase.endpoint.service.ExtendedServices;
import com.google.protobuf.ByteString;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * <h1>Test</h1>
 * The Test class
 *
 * @author Mohsen Zainalpour
 * @version 1.0
 * @since 2/08/16
 */
public class Example {
    public static void main(String[] args) throws IOException {

        Configuration conf = HBaseConfiguration.create();
        TableName tableName = TableName.valueOf("VisitCount");
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(tableName);

        byte[] columnFamily = Bytes.toBytes("CF");
        byte[] qualifier = Bytes.toBytes("hll");

        HLLWriter writer = new HLLWriter((HTable) table);
        for (int i = 0; i <= 10; i++) {
            byte[] rowKey = Bytes.toBytes(i + "kh.google.com/flatfile?f1c-02013313100111-d.5907.652\\x013\\xA1(");
            for (int j = 30; j <= 40; j++)
                writer.send(HLLWriter.createRequest(rowKey, columnFamily, qualifier, "valueX " + j));
        }

        table.close();
        connection.close();
    }
}
