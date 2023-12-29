from movielens_tests import *
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types  as t

def write_csv(df, file_name):
    # Write the DataFrame to a CSV file
    df.write.format("csv").option("header", "true").mode("overwrite").save(f'results/{file_name}')
    print(f"SAVED {file_name}")

# Create a SparkSession
spark = SparkSession.builder.appName('MovieLens').getOrCreate()

# Read the movies.dat file
movies_df = spark.read.load("downloads/movies.dat", format="csv", sep="::", inferSchema=True)
# Specify column names for movies_df
movies_df = movies_df.toDF("MovieID", "Title", "Genres")

# Read the ratings.dat file
ratings_df = spark.read.load("downloads/ratings.dat", format="csv", sep="::", inferSchema=True)
# Specify column names for ratings_df
ratings_df = ratings_df.toDF("UserID", "MovieID", "Rating", "Timestamp")
# Convert the string timestamp to seconds since the epoch:
ratings_df = ratings_df.withColumn('DateTime', F.to_timestamp(ratings_df["Timestamp"].cast(dataType=t.TimestampType())))
# Drop the original string timestamp column:
ratings_df = ratings_df.drop("Timestamp")

# Run tests on input files
expect_column_is_int(ratings_df, 'Rating')       
expect_values_between(ratings_df, "UserID", 1, 6040)
expect_values_between(ratings_df, "MovieID", 1, 3952)
expect_values_between(ratings_df, "Rating", 1, 5)
expect_min_ratings_per_user(ratings_df)
expect_not_null_column_values(movies_df, "MovieID")
expect_not_null_column_values(movies_df, "Title")
expect_unique_column_values(movies_df, "MovieID")
expect_unique_column_values(movies_df, "Title")       
expect_imdb_format_movie_titles(movies_df)
expect_test_results() # display test results summary

# Register DataFrames as temporary views
movies_df.createOrReplaceTempView("movies")
ratings_df.createOrReplaceTempView("ratings")

# Execute Spark SQL query
movie_results_df = spark.sql("""
    SELECT 
        m.*,
        MAX(Rating) AS MaxRating, 
        MIN(Rating) AS MinRating,
        ROUND( AVG(Rating), 2 ) AS AvgRating
    FROM movies m 
    LEFT JOIN ratings r 
        ON m.MovieID = r.MovieID
    GROUP BY 
        m.MovieID,
        m.Title,
        m.Genres
    ORDER BY
        m.MovieID
""")

# Execute Spark SQL query
user_results_df = spark.sql("""
    WITH movies_ranked AS (
        SELECT 
            r.UserID,
            m.Title,
            r.Rating,
            RANK() OVER ( PARTITION BY r.UserID ORDER BY r.Rating DESC ) AS TitleRank
        FROM ratings r
        LEFT JOIN movies m
            ON r.MovieID = m.MovieID
        )
    SELECT *
    FROM movies_ranked
    WHERE TitleRank < 4
    ORDER BY
        UserID
""")

write_csv(movies_df, "original_movies.csv")
write_csv(ratings_df, "original_ratings.csv")
write_csv(movie_results_df, "aggregate_ratings_per_movie.csv")
write_csv(user_results_df, "top_3_movies_per_user.csv")