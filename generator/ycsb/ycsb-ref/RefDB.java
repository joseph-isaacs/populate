import site.ycsb.*;
import java.io.*;
import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Writes every operation as TSV. Load (`-load`): `key\tfield0\tfield1...`
 * per insert, fields in name order. Run (`-t`, with `-p refdb.run=true`):
 * `OP\tkey\tscanlength\treadfields\tfield0...` per operation, where
 * `readfields` lists the fields a read or scan asks for (comma-separated,
 * in name order; empty for all) and the field columns hold the values an
 * update or insert writes (empty if not written).
 */
public class RefDB extends DB {
  private BufferedWriter out;
  private int fieldcount;
  private String prefix;
  private boolean run;
  public void init() throws DBException {
    Properties p = getProperties();
    fieldcount = Integer.parseInt(p.getProperty("fieldcount", "10"));
    prefix = p.getProperty("fieldnameprefix", "field");
    run = Boolean.parseBoolean(p.getProperty("refdb.run", "false"));
    try {
      out = new BufferedWriter(new FileWriter(p.getProperty("refdb.out")), 1 << 20);
      String s = p.getProperty("refdb.seed");
      if (s != null) {
        Field f = Thread.class.getDeclaredField("threadLocalRandomSeed"); f.setAccessible(true);
        Field pr = Thread.class.getDeclaredField("threadLocalRandomProbe"); pr.setAccessible(true);
        ThreadLocalRandom.current();
        f.setLong(Thread.currentThread(), Long.parseLong(s));
        pr.setInt(Thread.currentThread(), 1);
        System.err.println("REFDB thread id " + Thread.currentThread().getId());
      }
    } catch (Exception e) { throw new DBException(e); }
  }
  public void cleanup() throws DBException {
    try { out.close(); } catch (IOException e) { throw new DBException(e); }
  }
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    if (run) {
      return op("INSERT", key, "", null, values);
    }
    Map<String, String> got = new HashMap<>();
    for (Map.Entry<String, ByteIterator> e : values.entrySet()) got.put(e.getKey(), e.getValue().toString());
    try {
      out.write(key);
      for (int i = 0; i < fieldcount; i++) { out.write('\t'); out.write(got.get(prefix + i)); }
      out.write('\n');
    } catch (IOException e) { return Status.ERROR; }
    return Status.OK;
  }

  private Status op(String op, String key, String scan, Set<String> fields, Map<String, ByteIterator> values) {
    Map<String, String> got = new HashMap<>();
    if (values != null) {
      for (Map.Entry<String, ByteIterator> e : values.entrySet()) got.put(e.getKey(), e.getValue().toString());
    }
    try {
      out.write(op); out.write('\t'); out.write(key); out.write('\t'); out.write(scan); out.write('\t');
      if (fields != null) {
        boolean first = true;
        for (int i = 0; i < fieldcount; i++) {
          if (fields.contains(prefix + i)) { if (!first) out.write(','); out.write(prefix + i); first = false; }
        }
      }
      for (int i = 0; i < fieldcount; i++) {
        out.write('\t');
        String v = got.get(prefix + i);
        if (v != null) out.write(v);
      }
      out.write('\n');
    } catch (IOException e) { return Status.ERROR; }
    return Status.OK;
  }
  public Status read(String t, String k, Set<String> f, Map<String, ByteIterator> r) { return op("READ", k, "", f, null); }
  public Status scan(String t, String s, int n, Set<String> f, Vector<HashMap<String, ByteIterator>> r) {
    return op("SCAN", s, Integer.toString(n), f, null);
  }
  public Status update(String t, String k, Map<String, ByteIterator> v) { return op("UPDATE", k, "", null, v); }
  public Status delete(String t, String k) { return Status.OK; }
}
