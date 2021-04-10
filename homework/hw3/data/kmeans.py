import numpy as np
import matplotlib.pyplot as plt
import pandas as pd

import sys
import math
from pyspark import SparkConf, SparkContext

MAX_ITER = 20
DIMENSIONS = 58
DATA_FILE = 'data/q1/data.txt'
C_FILE = 'data/q1/random_centroids.txt'


#  Reads in the initial values of the 10 random centroids.
def read_initial_centroids(infile):
    initial_centroids = []
    with open(infile) as in_file:
        initial_centroids = [[float(x) for x in line.split()] for line in in_file.readlines()]
    return initial_centroids

def euclidean_distance_cost(a, b):
    distance = 0
    for a_i, b_i in zip(a, b):
        distance += (a_i - b_i) ** 2
    return distance

def manhattan_distance(a, b):
    distance = 0
    for a_i, b_i in zip(a, b):
        distance += abs(a_i - b_i)
    return distance 

#  Calculates the nearest centroid to the given point.
#  Represents chosen centroid as an index into the centroid list
def nearest_centroid(point, centroids, distance_function):
    closest = -1
    shortest_distance = float('inf')
    for i in range(len(centroids)):
        distance = manhattan_distance(point, centroids[i]) if distance_function else euclidean_distance_cost(point, centroids[i]) 
        if distance < shortest_distance:
            closest = i
            shortest_distance = distance
    return (closest, (point, shortest_distance))

#  Computes sum of 2 DIMENSION-dimensional points
def add_points(total, curr):
    for i in range(DIMENSIONS):
        total[i] += curr[i] 
    return total

#  Computes the new centroids by taking mean of clusters
def compute_new_centroids(clusters, counts):
    for i in range(len(clusters)):
        clusters[i] = (clusters[i][0], list(map(lambda x: x / counts[clusters[i][0]], clusters[i][1])))
    return [x[1] for x in clusters]


def k_means_clustering(initial_centroids_file, dist_function):
    centroids = read_initial_centroids(initial_centroids_file)
    iteration_costs = []
    for _ in range(MAX_ITER):
        clusters_rdd = data_rdd.map(lambda point: nearest_centroid(point, centroids, dist_function))  # Each element of clusters_rdd is a tuple: (index of centroid, (point, cost))
        iteration_costs.append(clusters_rdd.map(lambda point: point[1][1]).reduce(lambda total, x: total + x))  # Calculates the total iteration cost by summing individual costs
        summed_clusters_rdd = clusters_rdd.map(lambda point: (point[0], point[1][0]))\
                                        .reduceByKey(lambda total, curr: add_points(total, curr)) 
        centroids = compute_new_centroids(summed_clusters_rdd.collect(), clusters_rdd.countByKey())  # Recompute centroids by calculating mean of each cluster
    return iteration_costs


conf = SparkConf()
sc = SparkContext(conf=conf)
data_rdd = sc.textFile(DATA_FILE).map(lambda line: [float(x) for x in line.split()])  # Each element of data_rdd is a list of floats that represents the point 
#eucl = k_means_clustering(C_FILE, False)
man = k_means_clustering(C_FILE, True)

iterations = []
for i in range(1, 21):
    iterations.append(i)

print("COSTS: " + str(man))

plt.plot(iterations, man)
plt.xlabel("Iteration")
plt.ylabel("Costs")
plt.show()

sc.stop()