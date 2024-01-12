package dev.morling.onebrc;

import java.io.File;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class CalculateAverage_abmodi {

  private static final String filePathStr = "./measurements.txt";
  private static final Path filePath = Paths.get(filePathStr);
  private static final long FILE_SIZE = new File(filePathStr).length();

  private static final List<Map<String, Result>> results = new CopyOnWriteArrayList<>();
  private static final long CHUNK_SIZE = 4 * 1024 * 1024;
  private static class Result {
    int count;
    long sum;
    int min;
    int max;

    public Result() {
      count = 0;
      sum = 0;
      min = Integer.MAX_VALUE;
      max = Integer.MIN_VALUE;
    }

    public String toString() {
      double mean = (double)(sum)/count;
      return min/10.0 + "/" + round(mean) + "/" + max/10.0;
    }

    private double round(double value) {
      return Math.round(value) / 10.0;
    }
  }

  private static int parseValue(String s, int index, int len) {
    int res = 0;
    boolean neg = false;
    if (s.charAt(index) == '-') {
      neg = true;
      index = index + 1;
    }
    for (int i = index; i < len; ++i) {
      if (s.charAt(i) == '.') {
        continue;
      }
      res = (res * 10) + (s.charAt(i) - '0');
    }
    if (neg) {
      res *= -1;
    }
    return res;
  }


  private static void processChunk(long startPos, long endPos) {
    try (FileChannel channel = FileChannel.open(filePath, StandardOpenOption.READ)) {
      channel.position(startPos);
      ByteBuffer buffer = ByteBuffer.allocate((int) (endPos - startPos + 1));
      channel.read(buffer);
      String chunk = new String(buffer.array());
      HashMap<String, Result> map = new HashMap<>();
      int sl = 0;
      do {
        int index = -1;
        int el = -1;
        for (int i = sl; i < chunk.length(); ++i) {
          if (chunk.charAt(i) == ';') {
            index = i;
          }
          if (chunk.charAt(i) == '\n') {
            el = i;
            break;
          }
        }
        if (el == -1) {
          el = chunk.length();
        }
        String key = chunk.substring(sl, index);
        int val = parseValue(chunk, index + 1, el);
        if (map.containsKey(key)) {
          Result res = map.get(key);
          res.count += 1;
          res.sum += val;
          if (val > res.max) {
            res.max = val;
          }
          if (val < res.min) {
            res.min = val;
          }
        } else {
          map.put(key, new Result());
        }
        sl = el + 1;
      } while (sl < chunk.length());
      results.add(map);
    }
    catch(Exception e) {
      System.out.println(e.getMessage());
    }
  }

  private static long findChunkEnd(long startPos) {
    try (FileChannel channel = FileChannel.open(filePath, StandardOpenOption.READ)) {
      if (startPos >= FILE_SIZE) {
        return -1;
      }

      long currentPos = startPos + CHUNK_SIZE;
      if (currentPos >= FILE_SIZE) {
        currentPos = FILE_SIZE - 1;
      }

      channel.position(currentPos);
      ByteBuffer buffer = ByteBuffer.allocate(1024);
      int readBytes = channel.read(buffer);

      if (readBytes > 0) {
        for (int i = 0; i < readBytes; i++) {
          if (buffer.get(i) == '\n') {
            break;
          }
          currentPos++;
        }
      }

      return currentPos;
    } catch (Exception e) {
      System.out.println(e.getMessage());
    }
    return -1;
  }

  private static class RecordProcessor implements Callable<Void> {
    private final long startPos;
    private final long endPos;

    RecordProcessor(long sp, long ep) {
      startPos = sp;
      endPos = ep;
    }

    @Override
    public Void call() throws Exception {
      processChunk(startPos, endPos);
      return null;
    }
  }

  public void print() {

  }

  public static void main(String[] args) {
    Collection<Callable<Void>> callables = new ArrayList<>();

    long startPos = 0;
    do {
      long endPos = findChunkEnd(startPos);
      callables.add(new RecordProcessor(startPos, endPos));
      startPos = endPos + 1;
    } while (startPos != FILE_SIZE);

    try {
      ExecutorService executorService = Executors.newWorkStealingPool(64);
      List<Future<Void>> taskFutureList = executorService.invokeAll(callables);
      for (Future<Void> f : taskFutureList) {
        f.get();
      }
      executorService.shutdownNow();
    } catch (Exception e) {
      System.err.println(e.getMessage());
    }
    Map<String, Result> finalResult = new TreeMap<>();
    for (var map : results) {
      for (String key : map.keySet()) {
        if (finalResult.containsKey(key)) {
          Result mag1 = finalResult.get(key);
          Result mag2 = map.get(key);
          mag1.min = Math.min(mag1.min, mag2.min);
          mag1.max = Math.max(mag1.max, mag2.max);
          mag1.sum = mag1.sum + mag2.sum;
          mag1.count = mag1.count + mag2.count;
          finalResult.put(key, mag1);
        }
        else {
          finalResult.put(key, map.get(key));
        }
      }
    }
    System.out.println(finalResult);
  }
}
