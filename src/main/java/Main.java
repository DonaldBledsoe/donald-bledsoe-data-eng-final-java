import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;

import java.util.Arrays;

public class Main {
    public static void main(String[] args) {

        DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
        options.setRunner(DataflowRunner.class);
        options.setProject("york-cdf-start");
        options.setStagingLocation("gs://york_temp_files/staging");
        options.setTempLocation("gs://york_temp_files/tmp");
        options.setRegion("us-central1");
        options.setJobName("db-capstone-java");

        Pipeline pipeline= Pipeline.create(options);

        TableReference views_spec =
                new TableReference()
                        .setProjectId("york-cdf-start")
                        .setDatasetId("final_donald_bledsoe_java")
                        .setTableId("cust_tier_code-sku-total_no_of_product_views");

        TableReference sales_spec =
                new TableReference()
                        .setProjectId("york-cdf-start")
                        .setDatasetId("final_donald_bledsoe_java")
                        .setTableId("cust_tier_code-sku-total_sales_amount");

        PCollection<TableRow> views_rows =
                pipeline.apply(
                        "Read from BigQuery query1",
                        BigQueryIO.readTableRows()
                                .fromQuery("SELECT table2.cust_tier_code, CAST(table1.sku as INT64) as sku, COUNT(table1.sku) as  "+
                                        " total_no_of_product_views FROM `york-cdf-start.final_input_data.product_views` as table1 " +
                                        " JOIN `york-cdf-start.final_input_data.customers` as table2 ON table1.customer_id = table2.customer_id" +
                                        " GROUP BY table2.cust_tier_code, table1.SKU ORDER BY table1.sku, table2.CUST_TIER_CODE asc ")
                                .usingStandardSql());

        PCollection<TableRow> sales_rows =
                pipeline.apply(
                        "Read from BigQuery query2",
                        BigQueryIO.readTableRows()
                                .fromQuery("SELECT table2.cust_tier_code, table1.sku, ROUND (SUM (table1.order_amt), 2) as total_sales_amount" +
                                        "  FROM `york-cdf-start.final_input_data.orders`as table1 JOIN `york-cdf-start.final_input_data.customers` as table2 " +
                                        "  ON table1.customer_id = table2.customer_id GROUP BY table2.cust_tier_code, table1.SKU ORDER BY table1.sku, table2.CUST_TIER_CODE asc ")
                                .usingStandardSql());

        TableSchema views_schema =
                new TableSchema()
                        .setFields(
                                Arrays.asList(
                                        new TableFieldSchema()          //schema HAS to match what you pull in the query
                                                .setName("cust_tier_code").setType("STRING"),
                                        new TableFieldSchema()
                                                .setName("sku").setType("INT64"),
                                        new TableFieldSchema()
                                                .setName("total_no_of_product_views").setType("INT64")
                                )
                        );

        TableSchema sales_schema =
                new TableSchema()
                        .setFields(
                                Arrays.asList(
                                        new TableFieldSchema()          //schema HAS to match what you pull in the query
                                                .setName("cust_tier_code").setType("STRING"),
                                        new TableFieldSchema()
                                                .setName("sku").setType("INT64"),
                                        new TableFieldSchema()
                                                .setName("total_sales_amount").setType("FLOAT64")
                                )
                        );


        views_rows.apply(
                "Write to BigQuery1",
                BigQueryIO.writeTableRows()
                        .to(views_spec)
                        .withSchema(views_schema)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));

        sales_rows.apply(
                "Write to BigQuery2",
                BigQueryIO.writeTableRows()
                        .to(sales_spec)
                        .withSchema(sales_schema)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));


        pipeline.run().waitUntilFinish();
    }

}