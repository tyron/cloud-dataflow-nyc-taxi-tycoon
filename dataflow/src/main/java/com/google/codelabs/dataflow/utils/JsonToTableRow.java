package com.google.codelabs.dataflow.utils;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;

import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.io.ByteArrayInputStream;
import java.io.IOException;

@SuppressWarnings("serial")
/*
 * public class JsonToTableRow extends PTransform<PCollection<String>,
 * PCollection<TableRow>> {
 * 
 * @Override public PCollection<TableRow> expand(PCollection<String>
 * stringPCollection) { return stringPCollection.apply("JsonToTableRow",
 * MapElements.<String, com.google.api.services.bigquery.model.TableRow>via( new
 * SimpleFunction<String, TableRow>() {
 * 
 * @Override public TableRow apply(String json) { try {
 * 
 * InputStream inputStream = new ByteArrayInputStream(
 * json.getBytes(StandardCharsets.UTF_8.name()));
 * 
 * //OUTER is used here to prevent EOF exception return
 * TableRowJsonCoder.of().decode(inputStream, Coder.Context.OUTER); } catch
 * (IOException e) { throw new RuntimeException("Unable to parse input", e); } }
 * })); } }
 */

public class JsonToTableRow extends SimpleFunction<String, TableRow> {

    @Override
    public TableRow apply(String t) {
        InputStream inputStream;
        TableRow out = null;
        try {
            inputStream = new ByteArrayInputStream(t.getBytes(StandardCharsets.UTF_8.name()));
            out = TableRowJsonCoder.of().decode(inputStream, Coder.Context.OUTER);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
      return out;
    }
  }
