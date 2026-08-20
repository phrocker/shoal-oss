import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.admin.servers.ServerId;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;

public final class AccumuloSmoke {
  private static final String CLIENT_PROPERTIES =
      "/opt/accumulo/conf/accumulo-client.properties";
  private static final String TABLE = "shoal_accumulo4_smoke";

  private AccumuloSmoke() {}

  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      throw new IllegalArgumentException("expected ready or smoke");
    }
    try (AccumuloClient client = Accumulo.newClient().from(CLIENT_PROPERTIES).build()) {
      if ("ready".equals(args[0])) {
        ready(client);
      } else if ("smoke".equals(args[0])) {
        smoke(client);
      } else {
        throw new IllegalArgumentException("expected ready or smoke");
      }
    }
  }

  private static void ready(AccumuloClient client) throws Exception {
    if (!client.tableOperations().exists("accumulo.metadata")) {
      throw new IllegalStateException("metadata table is unavailable");
    }
    if (client.instanceOperations().getServers(ServerId.Type.TABLET_SERVER).isEmpty()) {
      throw new IllegalStateException("no tablet server is registered");
    }
  }

  private static void smoke(AccumuloClient client) throws Exception {
    TableOperations tables = client.tableOperations();
    if (tables.exists(TABLE)) {
      tables.delete(TABLE);
    }
    tables.create(TABLE);
    try {
      Mutation mutation = new Mutation("row-1");
      mutation.put("cf", "cq", new Value("value-1".getBytes(StandardCharsets.UTF_8)));
      try (BatchWriter writer = client.createBatchWriter(TABLE)) {
        writer.addMutation(mutation);
        writer.flush();
      }
      tables.flush(TABLE, null, null, true);

      int cells = 0;
      try (Scanner scanner = client.createScanner(TABLE, Authorizations.EMPTY)) {
        for (Map.Entry<Key,Value> entry : scanner) {
          cells++;
          Key key = entry.getKey();
          if (!"row-1".equals(key.getRow().toString())
              || !"cf".equals(key.getColumnFamily().toString())
              || !"cq".equals(key.getColumnQualifier().toString())
              || !"value-1".equals(entry.getValue().toString())) {
            throw new IllegalStateException("unexpected cell: " + entry);
          }
        }
      }
      if (cells != 1) {
        throw new IllegalStateException("expected exactly one cell, got " + cells);
      }
    } finally {
      if (tables.exists(TABLE)) {
        tables.delete(TABLE);
      }
    }
  }
}
