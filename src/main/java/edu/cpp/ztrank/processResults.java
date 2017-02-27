package edu.cpp.ztrank;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.stream.Stream;

/**
 * Created by Zachary Rank on 2/27/17.
 */
public class processResults {
asd
    private static HashMap<Long, String> ID_to_Name = new HashMap<Long, String>();

    public static void main(String[] args) {

        //map movie id's to names
        mapMovieNames("./a3-dataset/movie_titles.txt");

        //pick top 10 movies


        //map top 10 movies to names


        //pick top 10 users


        //write top 10 users and movies to one file

    }

    public static void mapMovieNames(String filepath) {
        try (Stream<String> stream = Files.lines(Paths.get(filepath))) {
            stream.forEach(l -> {
                String[] entry = l.split(",");
                ID_to_Name.put(Long.parseLong(entry[0]), entry[2]);
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
