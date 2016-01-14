package com.yahoo.ycsb.db;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.marklogic.client.*;
import com.marklogic.client.document.DocumentManager;
import com.marklogic.client.document.DocumentWriteSet;
import com.marklogic.client.io.JacksonHandle;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.io.marker.JSONWriteHandle;
import com.yahoo.ycsb.*;

import java.io.StringWriter;
import java.io.Writer;
import java.util.*;

/**
 * Created by Emmanuel TOURDOT on 13/01/2016.
 */
public class MarkLogicClient extends DB {
  public static final String HOST_PROPERTY = "marklogic.host";
  public static final String PORT_PROPERTY = "marklogic.port";
  public static final String USER_PROPERTY = "marklogic.user";
  public static final String PASSWORD_PROPERTY = "marklogic.password";
  public static final String FORMAT_PROPERTY = "marklogic.format";
  public static final String BATCHSIZE_PROPERTY = "marklogic.batchsize";
  protected static final ObjectMapper JSON_MAPPER = new ObjectMapper();

  DatabaseClient client;
  String format;
  int batchSize;
  DocumentManager documentManager;
  DocumentWriteSet writeSet;

  @Override
  public void init() throws DBException {
    Properties props = getProperties();
    String host = props.getProperty(HOST_PROPERTY, "localhost");
    int port = Integer.parseInt(props.getProperty(PORT_PROPERTY, "8003"));
    String user = props.getProperty(USER_PROPERTY);
    String password = props.getProperty(PASSWORD_PROPERTY);
    this.format = props.getProperty(FORMAT_PROPERTY, "json");
    this.batchSize = Integer.parseInt(props.getProperty(BATCHSIZE_PROPERTY, "1"));
    client = DatabaseClientFactory.newClient(host, port, user, password, DatabaseClientFactory.Authentication.BASIC);
    documentManager = client.newDocumentManager();
    writeSet = documentManager.newWriteSet();
  }

  @Override
  public void cleanup() {
    client.release();
  }

  @Override
  public Status read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
    String uri = "/" + table + "/" + key + "." + format;
    DocumentManager documentManager = client.newDocumentManager();
    try {
      JsonNode document = (JsonNode) documentManager.readAs(uri, JsonNode.class);
      if (document == null) {
        return Status.ERROR;
      }
      decode(document.toString(), fields, result);
      return Status.OK;
    } catch (Exception e) {
      e.printStackTrace();
    }
    return Status.ERROR;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    return Status.NOT_IMPLEMENTED;
  }

  @Override
  public Status update(String table, String key, HashMap<String, ByteIterator> values) {
    String uri = "/" + table + "/" + key + "." + format;
    Object toSave = encode(values);
    try {
      documentManager.writeAs(uri, toSave);
      return Status.OK;
    } catch (Exception e) {
      e.printStackTrace();
    }
    return Status.ERROR;
  }

  @Override
  public Status insert(String table, String key, HashMap<String, ByteIterator> values) {
    String uri = "/" + table + "/" + key + "." + format;
    Object toSave = encode(values);
    try {
      if (batchSize == 1) {
        documentManager.writeAs(uri, toSave);
        return Status.OK;
      } else {
        writeSet.add(uri, new StringHandle().with((String) toSave));
        if (writeSet.size() == batchSize) {
          documentManager.write(writeSet);
          writeSet.clear();
        }
        return Status.OK;
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return Status.ERROR;
  }

  @Override
  public Status delete(String table, String key) {
    String uri = "/" + table + "/" + key + "." + format;
    try {
      documentManager.delete(uri);
      return Status.OK;
    } catch (Exception e) {
      e.printStackTrace();
    }
    return Status.ERROR;
  }

  private Object encode(final HashMap<String, ByteIterator> source) {
    HashMap<String, String> stringMap = StringByteIterator.getStringMap(source);
    if ("text".equals(format)) {
      return stringMap;
    }

    ObjectNode node = JSON_MAPPER.createObjectNode();
    for (Map.Entry<String, String> pair : stringMap.entrySet()) {
      node.put(pair.getKey(), pair.getValue());
    }
    JsonFactory jsonFactory = new JsonFactory();
    Writer writer = new StringWriter();
    try {
      JsonGenerator jsonGenerator = jsonFactory.createGenerator(writer);
      JSON_MAPPER.writeTree(jsonGenerator, node);
    } catch (Exception e) {
      throw new RuntimeException("Could not encode JSON value");
    }
    return writer.toString();
  }

  private void decode(final Object source, final Set<String> fields,
                      final HashMap<String, ByteIterator> dest) {
    if ("json".equals(format)) {
      try {
        JsonNode json = JSON_MAPPER.readTree((String) source);
        boolean checkFields = fields != null && !fields.isEmpty();
        for (Iterator<Map.Entry<String, JsonNode>> jsonFields = json.fields(); jsonFields.hasNext();) {
          Map.Entry<String, JsonNode> jsonField = jsonFields.next();
          String name = jsonField.getKey();
          if (checkFields && fields.contains(name)) {
            continue;
          }
          JsonNode jsonValue = jsonField.getValue();
          if (jsonValue != null && !jsonValue.isNull()) {
            dest.put(name, new StringByteIterator(jsonValue.asText()));
          }
        }
      } catch (Exception e) {
        throw new RuntimeException("Could not decode JSON:"+e.getMessage());
      }
    } else {
      HashMap<String, String> converted = (HashMap<String, String>) source;
      for (Map.Entry<String, String> entry : converted.entrySet()) {
        dest.put(entry.getKey(), new StringByteIterator(entry.getValue()));
      }
    }
  }
}
