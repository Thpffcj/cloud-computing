from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors
import pymongo

# db.addUser("steam",{roles:[ {role:"root",db:"steam_db"} ]})
#mongodb连接
client = pymongo.MongoClient('mongodb://steam:steam@***.***.***.***:27017/steam_db')
db = client.steam_db
regions = db.China
test_collection = regions.test_collection
train_collection = regions.train_collection
print(train_collection.find()[0])

# kmeans_path = "./kmeans"
model_path = "./kmeans_model"

def getData(collection):
    return map(lambda r: (Vectors.dense([r["author"]["num_games_owned"], 
                                         r["author"]["num_reviews"], 
                                         r["author"]["playtime_forever"], 
#                                          r["review"], 
                                         r["voted_up"],
                                         r["votes_up"], 
                                         r["votes_funny"], 
                                         r["comment_count"]]),), collection.find())
    
spark = SparkSession\
        .builder\
        .appName("KMeansExample")\
        .getOrCreate()
train_data = getData(train_collection)
test_data = getData(test_collection)

# Loads data.
# dataset = spark.read.format("libsvm").load("sample_kmeans_data.txt")
# data = [(Vectors.dense([0.0, 0.0]),), (Vectors.dense([1.0, 1.0]),),
#         (Vectors.dense([9.0, 8.0]),), (Vectors.dense([8.0, 9.0]),)]
# data = [(Vectors.dense([0.0, 0.0, 0.0]),), (Vectors.dense([0.1, 0.1, 0.1]),), (Vectors.dense([0.2, 0.2, 0.2]),),
#         (Vectors.dense([9.0, 9.0, 9.0]),), (Vectors.dense([9.1, 9.1, 9.1]),), (Vectors.dense([9.2, 9.2, 9.2]),)]
train_dataset = spark.createDataFrame(train_data, ["features"])
test_dataset = spark.createDataFrame(test_data, ["features"])

# Trains a k-means model.
kmeans = KMeans().setK(5).setSeed(1)
# kmeans = KMeans.load(kmeans_path)
model = kmeans.fit(train_dataset)
# model = KMeansModel.load(model_path)
clusterSizes = model.summary.clusterSizes
print(clusterSizes)

# print("点（-3 -3）所属族:" + model.predict((Vectors.dense([1, 1, 1, 1, 1, 1, 1]),)))

# Make predictions
predictions = model.transform(test_dataset)
# print(predictions.collect())

# Evaluate clustering by computing Silhouette score
evaluator = ClusteringEvaluator()

silhouette = evaluator.evaluate(predictions) #轮廓系数 silhouette coefficient 0.8758693672037696
print("Silhouette with squared euclidean distance = " + str(silhouette))

# Shows the result.
centers = model.clusterCenters()
print("Cluster Centers: ")
for center in centers:
    print(center)
    
# kmeans.save(kmeans_path)
# model.save(model_path)

kmeans_centers = regions.kmeans_centers
kmeans_centers.drop()
i = 0
for center in centers:
    json = {
        "num_games_owned":center[0],
        "num_reviews":center[1],
        "playtime_forever":center[2],
        "voted_up":center[3],
        "votes_up":center[4],
        "votes_funny":center[5],
        "comment_count":center[6],
        "num_of_reviews":clusterSizes[i],
    }
    i+=1
    kmeans_centers.insert_one(json)

spark.stop()