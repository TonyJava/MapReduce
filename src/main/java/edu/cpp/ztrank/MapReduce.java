package edu.cpp.ztrank;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.util.ToolRunner;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Set;
import java.util.TreeMap;

import static org.apache.commons.io.FileUtils.forceDelete;

/**
 * Created by Zachary Rank on 2/27/17.
 */
public class MapReduce {
    private static TreeMap<Long, String> movieIDToMovieName = new TreeMap<>();
    private static TreeMap<String, String> movieNameToMovieYear = new TreeMap<>();
    private static TreeMap<Double, ArrayList<String>> movieRatingToMovieName = new TreeMap<>();
    private static TreeMap<Long, ArrayList<String>> userReviewCountToUserID = new TreeMap<>();

    public static void main(String[] args) throws Exception {
        //run jobs first
        int exitCode = ToolRunner.run(new jobsDriver(), args);
        if (exitCode == 1) System.exit(exitCode);

        //map values from files
        mapMovieIDs("./a3-dataset/movie_titles.txt");
        mapMovieRatings("./MovieRatings/part-r-00000");
        mapUserReviewCounts("./UserReviews/part-r-00000");

        //write top 10 users and movies to one file
        int countMovies = 0;
        int countUsers = 0;

        try {
            PrintWriter writer = new PrintWriter("./output.txt", "UTF-8");
            writer.println("Outputs:\n--------");

            writer.println("Top 10 Movies Based Upon Average User Reviews:");
            Set<Double> ratings = movieRatingToMovieName.keySet();
            Object[] sortedRatings = ratings.toArray();
            Arrays.sort(sortedRatings);
            ArrayUtils.reverse(sortedRatings);
            int i = 0;
            while (countMovies < 10 && i < sortedRatings.length) {
                ArrayList<String> topMovies = movieRatingToMovieName.get(sortedRatings[i]);
                for (String s : topMovies) {
                    if (countMovies < 10) {
                        writer.println(i + 1 + ". " + s + " (" + movieNameToMovieYear.get(s) + "), Average Rating: " + sortedRatings[i]);
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
            while (countUsers < 10 && i < sortedRatings.length) {
                ArrayList<String> topUsers = userReviewCountToUserID.get(sortedReviewCounts[i]);
                for (String s : topUsers) {
                    if (countUsers < 10) {
                        writer.println(i + 1 + ". " + s + ", Total Reviews: " + sortedReviewCounts[i]);
                        countUsers++;
                    }
                }
                i++;
            }

            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        File indexMovie = new File("./MovieRatings");
        File indexUser = new File("./UserReviews");
        forceDelete(indexMovie);
        forceDelete(indexUser);

        System.exit(0);
    }

    public static void mapMovieIDs(String filepath) {

        try (BufferedReader br = new BufferedReader(new FileReader(filepath))) {
            for (String line; (line = br.readLine()) != null; ) {
                String[] entry = line.split(",");
                movieIDToMovieName.put(Long.parseLong(entry[0]), entry[2]);
                movieNameToMovieYear.put(entry[2], entry[1]);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void mapMovieRatings(String filepath) {

        try (BufferedReader br = new BufferedReader(new FileReader(filepath))) {
            for (String line; (line = br.readLine()) != null; ) {
                String[] entry = line.split("\\s+");
                Double rating = Double.parseDouble(entry[1]);
                if (!movieRatingToMovieName.containsKey(rating)) {
                    ArrayList<String> movies = new ArrayList<>();
                    movies.add(movieIDToMovieName.get(Long.parseLong(entry[0])));
                    movieRatingToMovieName.put(rating, movies);
                } else {
                    ArrayList<String> movies = movieRatingToMovieName.get(Double.parseDouble(entry[1]));
                    movies.add(movieIDToMovieName.get(Long.parseLong(entry[0])));
                    movieRatingToMovieName.put(rating, movies);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void mapUserReviewCounts(String filepath) {
        try (BufferedReader br = new BufferedReader(new FileReader(filepath))) {
            for (String line; (line = br.readLine()) != null; ) {
                String[] entry = line.split("\\s+");
                Long reviewCount = Long.parseLong(entry[1]);
                if (!userReviewCountToUserID.containsKey(reviewCount)) {
                    ArrayList<String> users = new ArrayList<>();
                    users.add(entry[0]);
                    userReviewCountToUserID.put(reviewCount, users);
                } else {
                    ArrayList<String> users = userReviewCountToUserID.get(Long.parseLong(entry[1]));
                    users.add(entry[0]);
                    userReviewCountToUserID.put(reviewCount, users);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
