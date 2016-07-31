library(SparkR)

#No need, only for testing
sparkR.session()
lines <- SparkR:::textFile(sc, "/home/student/assignment/train_v2.csv")
first(lines)
count(lines)

#Create custome schema
customSchema <- structType(
    structField("ID", "integer"),
    structField("user", "integer"),
    structField("movie", "integer"),
    structField("rating", "double"))

#create dataframe
df <- read.df("/home/student/assignment/train_v2.csv", source = "com.databricks.spark.csv", schema = customSchema, header="true")
df$ID <- NULL
randomRDD <- randomSplit(df, c(7,3), 0)
cache(randomRDD[[1]])
cache(randomRDD[[2]])

#ALS Part
sparkals <- spark.als(randomRDD[[1]], "user", "movie", "rating")
