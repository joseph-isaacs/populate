import site.ycsb.*;
import java.io.*;
import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

/** Writes every insert as `key\tfield0\tfield1...\n` (fields in name order). */
public class RefDB extends DB {
  private BufferedWriter out;
  private int fieldcount;
  private String prefix;
  public void init() throws DBException {
    Properties p = getProperties();
    fieldcount = Integer.parseInt(p.getProperty("fieldcount", "10"));
    prefix = p.getProperty("fieldnameprefix", "field");
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
    Map<String, String> got = new HashMap<>();
    for (Map.Entry<String, ByteIterator> e : values.entrySet()) got.put(e.getKey(), e.getValue().toString());
    try {
      out.write(key);
      for (int i = 0; i < fieldcount; i++) { out.write('\t'); out.write(got.get(prefix + i)); }
      out.write('\n');
    } catch (IOException e) { return Status.ERROR; }
    return Status.OK;
  }
  public Status read(String t, String k, Set<String> f, Map<String, ByteIterator> r) { return Status.OK; }
  public Status scan(String t, String s, int n, Set<String> f, Vector<HashMap<String, ByteIterator>> r) { return Status.OK; }
  public Status update(String t, String k, Map<String, ByteIterator> v) { return Status.OK; }
  public Status delete(String t, String k) { return Status.OK; }
}
