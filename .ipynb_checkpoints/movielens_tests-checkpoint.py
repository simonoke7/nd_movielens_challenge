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

def expect_min_ratings_per_user(df, target_value=20):
    global tests_count
    tests_count += 1
    # Count ratings per user
    counts_per_user = df.groupBy("userId").count()

    # Find the minimum of the group count
    min_rating_count = counts_per_user.selectExpr("min(count) as min_count").first()[0]

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

def expect_imdb_format_movie_titles(df):
    global tests_count
    tests_count += 1
    imdb_pattern = r".*\([0-9]{4}\)$"   # Regex pattern for title and year
    # Count format mismatch
    invalid_titles_count = df.filter(~df.Title.rlike(imdb_pattern)).count()
    if invalid_titles_count > 0:
        print(f"FAILED: TITLE COLUMN IN MOVIES FILE CONTAINS VALUES INCONSISTENT WITH FORMAT \"TITLE (RELEASE YEAR)\"")
    else:
        global tests_passed_count
        tests_passed_count += 1
        
def expect_test_results():
    global tests_count
    global tests_passed_count
    if tests_passed_count < tests_count:
        tests_failed_count = tests_count - tests_passed_count
        raise Exception(f"{tests_failed_count}/{tests_count} TESTS FAILED! RESOLVE BAD DATA TO PROCEED!")
    else:
        print(f"{tests_passed_count}/{tests_count} TESTS PASSED! PERFORMING ANALYSIS...")