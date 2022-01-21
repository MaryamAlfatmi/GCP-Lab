
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.JsonToRow;
import org.apache.beam.sdk.util.RowJson.RowJsonDeserializer.UnsupportedRowJsonException;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Streaming {

    private static final Logger LOG = LoggerFactory.getLogger(StreamingIngestion.class);
    // declared input paramters
    private static final String JOB_NAME = "usecase1-labid-1";
    private static final String REGION = "europe-west4";
    private static final String GCP_TEMP_LOCATION = "gs://c4e-uc1-dataflow-temp-1/temp";
    private static final String STAGING_LOCATION = "gs://c4e-uc1-dataflow-temp-1/staging";
    private static final String SERVICE_ACCOUNT = "c4e-uc1-sa-1@nttdata-c4e-bde.iam.gserviceaccount.com";
    private static final String WORKER_MACHINE_TYPE = "n1-standard-1";
    private static final String SUBNETWORK = "regions/europe-west4/subnetworks/subnet-uc1-1";
    // declared bq table and pub-sub topic
    private static final String BQ_TABLE = "nttdata-c4e-bde:uc1_1.account";
    private static final String INPUT_SUB = "projects/nttdata-c4e-bde/subscriptions/uc1-input-topic-sub-1";
    private static final String DLQ_TOPIC = "projects/nttdata-c4e-bde/topics/uc1-dlq-topic-1";


    public static final Schema BQ_SCHEMA = Schema.builder().addInt64Field("id").addStringField("name")
            .addStringField("surname").build();

    public static void main(String[] args) throws Exception {
        DataflowPipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
                .as(DataflowPipelineOptions.class);
        // set the command line attributes
        options.setJobName(JOB_NAME);
        options.setRegion(REGION);
        options.setRunner(DataflowRunner.class);
        options.setGcpTempLocation(GCP_TEMP_LOCATION);
        options.setStagingLocation(STAGING_LOCATION);
        options.setServiceAccount(SERVICE_ACCOUNT);
        options.setWorkerMachineType(WORKER_MACHINE_TYPE);
        options.setSubnetwork(SUBNETWORK);
        options.setStreaming(true);

        run(options);

    }

    public static void run(DataflowPipelineOptions options) throws Exception {
        Pipeline pipeline = Pipeline.create(options);
        LOG.info("Creating Pipeline.........................................................");

        PCollection<String> pubsubmessage = pipeline.apply("ReadMessageFromPubSub",
                PubsubIO.readStrings().fromSubscription(INPUT_SUB));
        try {
            pubsubmessage.apply(JsonToRow.withSchema(BQ_SCHEMA)).apply(BigQueryIO.<Row>write().to(BQ_TABLE).useBeamSchema()
            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
            .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

        } catch (Exception e) {
            pubsubmessage.apply("WriteToDlqTopic",PubsubIO.writeStrings().to(DLQ_TOPIC));
        }

        LOG.info("Writing to BigQuery.....................");
        pipeline.run().waitUntilFinish();

    }


}