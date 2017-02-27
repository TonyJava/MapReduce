package edu.cpp.ztrank;

import org.apache.commons.lang.ArrayUtils;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Stream;

/**
 * Created by Zachary Rank on 2/27/17.
 */
public class processResults {
    private static TreeMap<Long, String> movieIDToMovieName = new TreeMap<>();
    private static TreeMap<Double, ArrayList<String>> movieRatingToMovieName = new TreeMap<>();
    private static TreeMap<Long, ArrayList<String>> userReviewCountToUserID = new TreeMap<>();

    public static void main(String[] args) {

        //map values from files
        mapMovieIDs("./a3-dataset/movie_titles.txt");
        mapMovieRatings("./MovieRatings/part-r-00000");
        mapUserReviewCounts("./UserRatings/part-r-00000");

        //write top 10 users and movies to one file
        int countMovies = 0;
        int countUsers = 0;

        try{
            PrintWriter writer = new PrintWriter("./output.txt", "UTF-8");
            writer.println("Outputs:\n--------");

            writer.println("Top 10 Movies Based Upon Average User Reviews:");
            Set<Double> ratings = movieRatingToMovieName.keySet();
            Object[] sortedRatings = ratings.toArray();
            Arrays.sort(sortedRatings);
            ArrayUtils.reverse(sortedRatings);
            int i = 0;
            while(countMovies < 10 && i < sortedRatings.length) {
                ArrayList<String> topMovies = movieRatingToMovieName.get(sortedRatings[i]);
                for(String s : topMovies) {
                    if(countMovies < 10) {
                        writer.println(i + ". " + s + ", Average Rating:" + sortedRatings[i]);
                        countMovies++;
                    }
                }
                i++;
            }

            writer.println("\nTop 10 Users Based Upon Most Reviews Given:");
            Set<Long> reviewCounts = userReviewCountToUserID.keySet();
            Object[] sortedReviewCounts = reviewCounts.toArray();
            Arrays.sort(sortedReviewCounts);
            ArrayUtils.reverse(sortedReviewCounts);
            i = 0;
            while(countUsers < 10 && i < sortedRatings.length) {
                ArrayList<String> topUsers = userReviewCountToUserID.get(sortedReviewCounts[i]);
                for(String s : topUsers) {
                    if(countUsers < 10) {
                        writer.println(i + ". " + s + ", Total Reviews:" + sortedReviewCounts[i]);
                        countUsers++;
                    }
                }
                i++;
            }

            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void mapMovieIDs(String filepath) {
        try (Stream<String> stream = Files.lines(Paths.get(filepath))) {
            stream.forEach(l -> {
                String[] entry = l.split(",");
                movieIDToMovieName.put(Long.parseLong(entry[0]), entry[2]);
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void mapMovieRatings(String filepath) {
        try (Stream<String> stream = Files.lines(Paths.get(filepath))) {
            stream.forEach(l -> {
                String[] entry = l.split("\\s+");
                Double rating = Double.parseDouble(entry[1]);
                if(!movieRatingToMovieName.containsKey(rating)) {
                    ArrayList<String> movies = new ArrayList<String>();
                    movies.add(movieIDToMovieName.get(Long.parseLong(entry[0])));
                    movieRatingToMovieName.put(rating, movies);
                } else {
                    ArrayList<String> movies = movieRatingToMovieName.get(entry[1]);
                    movies.add(movieIDToMovieName.get(Long.parseLong(entry[0])));
                    movieRatingToMovieName.put(rating, movies);
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void mapUserReviewCounts(String filepath) {
        try (Stream<String> stream = Files.lines(Paths.get(filepath))) {
            stream.forEach(l -> {
                String[] entry = l.split("\\s+");
                Long reviewCount = Long.parseLong(entry[1]);
                if(!userReviewCountToUserID.containsKey(reviewCount)) {
                    ArrayList<String> users = new ArrayList<String>();
                    users.add(entry[0]);
                    userReviewCountToUserID.put(reviewCount, users);
                } else {
                    ArrayList<String> users = userReviewCountToUserID.get(entry[1]);
                    users.add(entry[0]);
                    userReviewCountToUserID.put(reviewCount, users);
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
