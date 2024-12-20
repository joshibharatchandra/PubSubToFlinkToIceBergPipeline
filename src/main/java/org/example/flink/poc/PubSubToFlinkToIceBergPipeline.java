package org.example.flink.poc;

import com.google.auth.oauth2.GoogleCredentials;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.util.Collector;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.types.Types;
import org.apache.flink.streaming.connectors.gcp.pubsub.PubSubSource;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.LocalDateTime;
import org.apache.hadoop.conf.Configuration;



public class PubSubToFlinkToIceBergPipeline {
    public static void main(String[] args) throws Exception {
        // Step 1: Set the Hadoop home directory (required for Hadoop-related operations)
        System.setProperty("hadoop.home.dir", "C:/Users/bhara/OneDrive/Desktop/SOFTWARES/hadoop/winutils/hadoop-2.6.0");

        // Step 2: Initialize Flink environment and enable checkpointing for fault tolerance
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);  // Checkpoints every 5 seconds here we can increase this

        // Step 3: Load Google Cloud credentials for accessing Pub/Sub
       // GoogleCredentials googleCredentials = GoogleCredentials.fromStream(PubSubToFlinkToIceBergPipeline.class.getClassLoader().getResourceAsStream("wired-record-443707-u8-618011b336b0.json"));
        GoogleCredentials googleCredentials = GoogleCredentials.fromStream(
                new FileInputStream("src/main/resources/test-project-123456-444507-88dcc71b8be9.json"));

        // Step 4: here we are deserialize the Pub/Sub messages into JSON objects
        DeserializationSchema<JsonNode> deserializer = new DeserializationSchema<JsonNode>() {
            @Override
            public JsonNode deserialize(byte[] message) throws IOException {
                return new ObjectMapper().readTree(message);
            }

            @Override
            public boolean isEndOfStream(JsonNode nextElement) {
                return false;  // this line Keep reading messages until the end of the stream we get
            }

            @Override
            public TypeInformation<JsonNode> getProducedType() {
                return TypeInformation.of(JsonNode.class);
            }
        };

        // Step 5: Set up the Pub/Sub source to receive messages from Google Cloud Pub/Sub
        PubSubSource<JsonNode> pubSubSource = PubSubSource.newBuilder()
                .withDeserializationSchema(deserializer)
                .withProjectName("test-project-123456-444507")
                .withSubscriptionName("new-topic-sub")
                .withCredentials(googleCredentials)
                .build();

        // Step 6: Add the Pub/Sub source to the Flink job (this is where the messages will be read)
        DataStream<JsonNode> pubSubMessages = env.addSource(pubSubSource);

        // Step 7: Map the incoming Pub/Sub messages to a StatementAccount object
        SingleOutputStreamOperator<StatementAccount> statementAccounts = pubSubMessages.map(msg -> {
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode statementAccountNode = msg.get("body").get("statementAccount");
            if (statementAccountNode == null) {
                throw new RuntimeException("Field 'statementAccount' not found in JSON message: " + msg.toString());
            }
            return objectMapper.treeToValue(statementAccountNode, StatementAccount.class);  // Convert JSON to StatementAccount object
        });



        // Step 8: Map StatementAccount to RowData, which will be used by Iceberg to store the data
        SingleOutputStreamOperator<RowData> statementAccountRowData = statementAccounts.map(statementAccount -> {
            GenericRowData row = new GenericRowData(14);  // Create a row with 14 fields as per the schema

            // Set the fields in the row according to the StatementAccount fields
            row.setField(0, StringData.fromString(statementAccount.getAccountNo()));
            row.setField(1, StringData.fromString(statementAccount.getIfscCode()));  // this is for bank_code
            row.setField(2, statementAccount.getCurrentBalance());
            row.setField(3, 0.0);  // previous_balance
            row.setField(4, StringData.fromString(statementAccount.getAccountNo()));  // masked_account_number
            row.setField(5, StringData.fromString(statementAccount.getFacility()));  // data_fetched_date
            row.setField(6, null);  // fip_id
            row.setField(7, StringData.fromString(statementAccount.getAccountType()));
            row.setField(8, StringData.fromString("Summary Placeholder"));  // summary
            row.setField(9, StringData.fromString("Active"));  // status
            row.setField(10, null);
            row.setField(11, null);
            row.setField(12, null);
            row.setField(13, 1);
            return row;
        });

        // Define Iceberg schema for the StatementAccount table
        Schema accountSummarySchema = new Schema(
                Types.NestedField.required(1, "account_id", Types.StringType.get()),
                Types.NestedField.required(2, "bank_code", Types.StringType.get()),
                Types.NestedField.optional(3, "current_balance", Types.DoubleType.get()),
                Types.NestedField.optional(4, "previous_balance", Types.DoubleType.get()),
                Types.NestedField.optional(5, "masked_account_number", Types.StringType.get()),
                Types.NestedField.optional(6, "data_fetched_date", Types.TimestampType.withZone()),
                Types.NestedField.optional(7, "fip_id", Types.StringType.get()),
                Types.NestedField.optional(8, "account_type", Types.StringType.get()),
                Types.NestedField.optional(9, "summary", Types.StringType.get()),
                Types.NestedField.optional(10, "status", Types.StringType.get()),
                Types.NestedField.optional(11, "consent_revoked_date", Types.DateType.get()),
                Types.NestedField.optional(12, "created_on", Types.TimestampType.withZone()),
                Types.NestedField.optional(13, "updated_on", Types.TimestampType.withZone()),
                Types.NestedField.optional(14, "fetch_count", Types.IntegerType.get())
        );

        // Process messages and extract transactions
        SingleOutputStreamOperator<RowData> depositTransactionStream = pubSubMessages.flatMap(new FlatMapFunction<JsonNode, RowData>() {
            @Override
            public void flatMap(JsonNode message, Collector<RowData> out) throws Exception {
                ObjectMapper objectMapper = new ObjectMapper();

                // Navigate done for xns
                JsonNode xnsArray = message.at("/body/statementAccount/xns");

                if (xnsArray.isArray()) {
                    for (JsonNode xnNode : xnsArray) {
                        // Convert kra h each JSON object in xns array to an Xn object
                        Xn xn = objectMapper.treeToValue(xnNode, Xn.class);

                        // Create a RowData for each transaction
                        GenericRowData row = new GenericRowData(18);

                        row.setField(0, StringData.fromString(xn.getTxnId()));
                        row.setField(1, StringData.fromString("dummy_user_id_" + xn.getIndex()));
                        row.setField(2, StringData.fromString("dummy_link_ref_number_" + xn.getIndex())); // Dummy Link Reference
                        row.setField(3, StringData.fromString(xn.getTxnId()));
                        row.setField(4, StringData.fromString(xn.getDate()));
                        row.setField(5, StringData.fromString(xn.getMode()));
                        row.setField(6, StringData.fromString(xn.getType()));
                        row.setField(7, xn.getBalance());
                        row.setField(8, LocalDateTime.now());
                        row.setField(9, StringData.fromString("2024-12")); // Dummy Period
                        row.setField(10, StringData.fromString(xn.getCategory()));
                        row.setField(11, StringData.fromString("dummy_digipass_category")); // Dummy Digipass Category
                        row.setField(12, xn.getAmount());
                        row.setField(13, StringData.fromString("dummy_fip_id"));
                        row.setField(14, StringData.fromString(xn.getReference()));
                        row.setField(15, StringData.fromString(xn.getNarration()));
                        row.setField(16, LocalDateTime.now());
                        row.setField(17, LocalDateTime.now());

                        // Emit the RowData
                        out.collect(row);
                    }
                } else {
                    throw new RuntimeException("'xns' field is not an array or is missing in the message: " + message.toString());
                }
            }
        });

        // Define Iceberg schema for the DepositTransaction table
        Schema depositTransactionSchema = new Schema(
                Types.NestedField.required(1, "id", Types.StringType.get()),
                Types.NestedField.required(2, "user_id", Types.StringType.get()),
                Types.NestedField.optional(3, "link_ref_number", Types.StringType.get()),
                Types.NestedField.optional(4, "txn_id", Types.StringType.get()),
                Types.NestedField.optional(5, "value_date", Types.StringType.get()),
                Types.NestedField.optional(6, "mode", Types.StringType.get()),
                Types.NestedField.optional(7, "type", Types.StringType.get()),
                Types.NestedField.optional(8, "current_balance", Types.DoubleType.get()),
                Types.NestedField.optional(9, "transaction_timestamp", Types.TimestampType.withZone()),
                Types.NestedField.optional(10, "month_year", Types.StringType.get()),
                Types.NestedField.optional(11, "category", Types.StringType.get()),
                Types.NestedField.optional(12, "digipass_category", Types.StringType.get()),
                Types.NestedField.optional(13, "amount", Types.DoubleType.get()),
                Types.NestedField.optional(14, "fip_id", Types.StringType.get()),
                Types.NestedField.optional(15, "reference", Types.StringType.get()),
                Types.NestedField.optional(16, "narration", Types.StringType.get()),
                Types.NestedField.optional(17, "created_on", Types.TimestampType.withZone()),
                Types.NestedField.optional(18, "updated_on", Types.TimestampType.withZone())
        );



        // Step 10: Define Iceberg catalog and write data for both tables
        String warehouseLocation = "gs://cloudsql-functions-golang14/warehouse";  // Specify GCS warehouse path
        String db = "db";  // Database name
        String tableNameStatementAccount = "statement_account_table";
        String tableNameDepositTransaction = "deposit_transaction_table";

        Configuration hadoopConf = new Configuration();
        HadoopCatalog catalog = new HadoopCatalog(hadoopConf, warehouseLocation);

        // Create tables if they don't exist
        TableIdentifier statementAccountTableId = TableIdentifier.of(db, tableNameStatementAccount);
        TableIdentifier depositTransactionTableId = TableIdentifier.of(db, tableNameDepositTransaction);

        if (!catalog.tableExists(statementAccountTableId)) {
            catalog.createTable(statementAccountTableId, accountSummarySchema);  // Create StatementAccount table if missing
        }

        if (!catalog.tableExists(depositTransactionTableId)) {
            catalog.createTable(depositTransactionTableId, depositTransactionSchema);  // Create DepositTransaction table if missing
        }

        // Step 11: Write data to Iceberg tables for both StatementAccount and DepositTransaction
        FlinkSink.forRowData(statementAccountRowData)
                .tableLoader(TableLoader.fromHadoopTable(warehouseLocation + "/" + db + "/" + tableNameStatementAccount, hadoopConf))
                .table(catalog.loadTable(statementAccountTableId))
                .overwrite(true)  // this is for overwrite existing data
                .append();

        FlinkSink.forRowData(depositTransactionStream)
                .tableLoader(TableLoader.fromHadoopTable(warehouseLocation + "/" + db + "/" + tableNameDepositTransaction, hadoopConf))
                .table(catalog.loadTable(depositTransactionTableId))
                .overwrite(true)  // same here overwrite existing data
                .append();

        // Step 12: Execute the Flink job (this starts the streaming process)
        env.execute("Flink Pub/Sub Message Reader with Dummy Data for Multiple Tables");
    }
}
