from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types  as t

tests_count = 0
tests_passed_count = 0

def expect_column_is_int(df, colName):
    global tests_count
    tests_count += 1
    sourceDataType = dict(df.dtypes)[colName]
    if sourceDataType != 'int':
        print(f"FAILED: COLUMN \"{colName}\" IS NOT AN INTEGER")
    else:
        global tests_passed_count
        tests_passed_count += 1

def expect_values_between(df, colName, lower_bound, upper_bound):
    global tests_count
    tests_count += 1
    failed_values_count = df.filter(~F.col(colName).between(lower_bound, upper_bound)).count()
    if failed_values_count > 0:
        print(f"FAILED: {failed_values_count} VALUES IN COLUMN \"{colName}\" OUTSIDE EXPECTED RANGE")
    else:
        global tests_passed_count
        tests_passed_count += 1

def expect_min_ratings_per_user(target_value=20):
    global tests_count
    tests_count += 1
    # Count ratings per user
    rating_counts_per_user = ratings_df.groupBy("userId").count()

    # Find the minimum of the group count
    min_rating_count = rating_counts_per_user.selectExpr("min(count) as min_count").first()[0]

    if min_rating_count < target_value:
        print(f"FAILED: USER(S) IN RATINGS FILE DO NOT HAVE EXPECTED MINIMUM OF {target_value} RATINGS")
    else:
        global tests_passed_count
        tests_passed_count += 1

def expect_not_null_column_values(df, colName):
    global tests_count
    tests_count += 1
    # Count duplicates in column
    null_count = df.filter(F.col(colName).isNull()).count()
    
    if null_count > 0:
        print(f"FAILED: COLUMN \"{colName}\" CONTAINS NULL VALUES")
    else:
        global tests_passed_count
        tests_passed_count += 1
        
def expect_unique_column_values(df, colName):
    global tests_count
    tests_count += 1
    # Count duplicates in column
    duplicate_count = df.groupBy(colName).count().filter(F.col("count") > 1).agg(F.count("*").alias("num_duplicates")).first()[0]
    
    if duplicate_count > 0:
        print(f"FAILED: \"{colName}\" IS NOT UNIQUE")
    else:
        global tests_passed_count
        tests_passed_count += 1

def expect_imdb_format_movie_titles():
    global tests_count
    tests_count += 1
    imdb_pattern = r".*\([0-9]{4}\)$"   # Regex pattern for title and year
    # Count format mismatch
    invalid_titles_count = movies_df.filter(~movies_df.Title.rlike(imdb_pattern)).count()
    if invalid_titles_count > 0:
        print(f"FAILED: TITLE COLUMN IN MOVIES FILE CONTAINS VALUES INCONSISTENT WITH FORMAT \"TITLE (RELEASE YEAR)\"")
    else:
        global tests_passed_count
        tests_passed_count += 1

def write_csv(df, file_name):
    # Write the DataFrame to a CSV file
    df.write.format("csv").option("header", "true").mode("overwrite").save(file_name)
    print(f"SAVED {file_name}")

# Create a SparkSession
spark = SparkSession.builder.appName('MovieLens').getOrCreate()

# Read the movies.dat file
movies_df = spark.read.load("movies.dat", format="csv", sep="::", inferSchema=True)
# Specify column names for movies_df
movies_df = movies_df.toDF("MovieID", "Title", "Genres")

# Read the ratings.dat file
ratings_df = spark.read.load("ratings.dat", format="csv", sep="::", inferSchema=True)
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
expect_min_ratings_per_user()
expect_not_null_column_values(movies_df, "MovieID")
expect_not_null_column_values(movies_df, "Title")
expect_unique_column_values(movies_df, "MovieID")
expect_unique_column_values(movies_df, "Title")       
expect_imdb_format_movie_titles()

if tests_passed_count < tests_count:
    tests_failed_count = tests_count - tests_passed_count
    raise Exception(f"{tests_failed_count}/{tests_count} TESTS FAILED! RESOLVE BAD DATA TO PROCEED!")
else:
    print(f"{tests_passed_count}/{tests_count} TESTS PASSED! PERFORMING ANALYSIS...")

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