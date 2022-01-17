import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

public class Streaming {
    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        options.setJobName("StreamingIngestion");

        Pipeline pipeline = Pipeline.create(options);

        PCollection<String> pubsubMessage = pipeline.apply(PubsubIO.readStrings().fromTopic(""));
        PCollection<TableRow> bqRow = pubsubMessage.apply(ParDo.of(new ConvertorStringBq()));
        bqRow.apply(BigQueryIO.writeTableRows().to("")
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

        pipeline.run();


    }

    private static class ConvertorStringBq extends DoFn<String, TableRow> {
        @ProcessElement
        public void processing(ProcessContext pc) {
            TableRow tr = new TableRow().set("id", Integer.parseInt(pc.element()))
                    .set("name", pc.element().toString())
                    .set("surname", pc.element().toString());

            pc.output(tr);
        }

    }
}