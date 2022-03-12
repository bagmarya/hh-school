package ru.hh.school.homework;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Array;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.slf4j.Logger;
import ru.hh.school.async.CF8Threads3;

import static java.util.Collections.reverseOrder;
import static java.util.Map.Entry.comparingByValue;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.*;
import static org.slf4j.LoggerFactory.getLogger;

public class Launcher {

  private static final Logger LOGGER = getLogger(Launcher.class);

  public static void main(String[] args) throws IOException {
    // Написать код, который, как можно более параллельно:
    // - по заданному пути найдет все "*.java" файлы
    // - для каждого файла вычислит 10 самых популярных слов (см. #naiveCount())
    // - соберет top 10 для каждой папки в которой есть хотя-бы один java файл
    // - для каждого слова сходит в гугл и вернет количество результатов по нему (см. #naiveSearch())
    // - распечатает в консоль результаты в виде:
    // <папка1> - <слово #1> - <кол-во результатов в гугле>
    // <папка1> - <слово #2> - <кол-во результатов в гугле>
    // ...
    // <папка1> - <слово #10> - <кол-во результатов в гугле>
    // <папка2> - <слово #1> - <кол-во результатов в гугле>
    // <папка2> - <слово #2> - <кол-во результатов в гугле>
    // ...
    // <папка2> - <слово #10> - <кол-во результатов в гугле>
    // ...
    //
    // Порядок результатов в консоли не обязательный.
    // При желании naiveSearch и naiveCount можно оптимизировать.

    Map<File, Map<String, Long>> byFilesTops = getByFilesTops("/home/masha/projects/0_masha/HH/hh-school/parallelism/src/main/java/ru/hh/school/parallelism");
    System.out.println(byFilesTops);

    Map<String, List<String>> byFoldersTops = getByFoldersTops(byFilesTops);
    System.out.println(byFoldersTops);

    Map<String, Long> wordResults = getWordResults(byFoldersTops);
    System.out.println(wordResults);

    for(Map.Entry<String, List<String>> folder : byFoldersTops.entrySet()){
      for(String word : folder.getValue()){
        System.out.printf("%s - %s - %d \n", folder.getKey(), word, wordResults.get(word));
      }
    }
  }

  private static Map<File, Map<String, Long>> getByFilesTops(String path) throws IOException {

    ExecutorService executor = Executors.newFixedThreadPool(8);

    Map<File, CompletableFuture<Map<String, Long>>> res = Files.walk(Paths.get(path))
            .filter(Files::isRegularFile)
            .filter(file -> file.toString().endsWith(".java"))
            .collect(Collectors.toMap(Path::toFile, p ->  CompletableFuture.supplyAsync (() -> naiveCount(p), executor)));

    Map<File, Map<String, Long>> byFilesTops = new HashMap<>();

    List<CompletableFuture> futures= new ArrayList<>(res.values());

    CompletableFuture<Void> allDoneFuture = CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]))
            .thenRun(() -> res.entrySet().stream().forEach(e -> {
              try {
                LOGGER.debug("добавляю файл " + e.getKey());
                byFilesTops.put(e.getKey(), e.getValue().get());
              } catch (InterruptedException | ExecutionException ex) {
                ex.printStackTrace();
              }
            }));

    allDoneFuture.join();
    executor.shutdown();
    return byFilesTops;
  }

  private static Map<String, List<String>> getByFoldersTops(Map<File, Map<String, Long>> byFilesTops) {

    Map<String, Map<String, Long>> byFoldersTops = new HashMap<>();

    byFilesTops.entrySet().stream().forEach(e -> {
      String pathToFolder = e.getKey().getParent();
      if (byFoldersTops.containsKey(pathToFolder)){
      byFoldersTops.put(pathToFolder, mergeCountMaps(byFoldersTops.get(pathToFolder), e.getValue()));
      }
      else {byFoldersTops.put(pathToFolder, e.getValue());}
    });

    return byFoldersTops.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> Launcher.countMapToTop10(e.getValue())));
  }

  private static Map<String, Long> getWordResults(Map<String, List<String>> byFoldersTops){

    ExecutorService executor = Executors.newFixedThreadPool(8);

    Map<String, CompletableFuture> res = byFoldersTops.entrySet().stream()
            .flatMap(e -> e.getValue().stream())
            .distinct()
            .collect(toMap(s -> s, s -> CompletableFuture.supplyAsync (() -> {
              try {
                return naiveSearch(s);
              } catch (IOException e) {
                LOGGER.debug("null is returned");
                e.printStackTrace();
                return null;
              }
            }, executor)));

    List<CompletableFuture> futures= new ArrayList<>(res.values());

    Map<String, Long> wordResults = new HashMap<>();

    CompletableFuture<Void> allDoneFuture = CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]))
            .thenRun(() ->  res.entrySet().stream().forEach(e -> {
              try {
                wordResults.put(e.getKey(), (Long) e.getValue().get());
              } catch (InterruptedException | ExecutionException ex) {
                ex.printStackTrace();
              }
            }));

    allDoneFuture.join();
    executor.shutdown();
    return wordResults;
    }

  private static Map<String, Long> mergeCountMaps (Map<String, Long> m1, Map<String, Long> m2){

    m1.entrySet().stream().forEach(e -> {
      if (m2.containsKey(e.getKey())) {m2.put(e.getKey(), e.getValue() + m2.get(e.getKey()));}
      else {m2.put(e.getKey(), e.getValue());}
    });

    return m2;
  }

  private static List<String> countMapToTop10 (Map<String, Long> countMap){
    return countMap.entrySet()
            .stream()
            .sorted(comparingByValue(reverseOrder()))
            .limit(10)
            .map(Map.Entry::getKey)
            .collect(toList());
  }

  private static Map<String, Long> naiveCount(Path path) {
    try {
      LOGGER.debug("обрабатывается файл " + path.toString());
      Map<String, Long> res = Files.lines(path)
              .flatMap(line -> Stream.of(line.split("[^a-zA-Z0-9]")))
              .filter(word -> word.length() > 3)
              .collect(groupingBy(identity(), counting()))
              .entrySet()
              .stream()
              .sorted(comparingByValue(reverseOrder()))
              .limit(10)
              .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
      LOGGER.debug("готов файл " + path.toString());
      return res;
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static long naiveSearch(String query) throws IOException {
    LOGGER.debug("Идет запрос слова " + query);
    Document document = Jsoup
      .connect("https://www.google.com/search?q=" + query)
      .userAgent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.116 Safari/537.36") //
      .get();
    LOGGER.debug("Обрабатываю результат для слова " + query);
    Element divResultStats = document.select("div#slim_appbar").first();
    String text = divResultStats.text();
    if(text.length() == 0){return 0L;}
    String resultsPart = text.substring(0, text.indexOf('('));
    return Long.parseLong(resultsPart.replaceAll("[^0-9]", ""));
  }

}
