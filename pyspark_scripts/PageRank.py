
def compute_contribs(pair):
    [url, [links, rank]] = pair  # split key-value pair
    return [(dest, rank/len(links)) for dest in links]


# RDD of (url, neighbors) pairs
links = sc.parallelize ([("aaa", ["bbb", "ccc"]),  ("bbb", ["aaa"]), ("ccc", ["aaa"])])
links.collect()




# RDD of (url, rank) pairs
ranks = links.map(lambda url_neighbors: (url_neighbors[0], 1.0))
ranks.collect()






NUM_ITERATIONS=10

for i in range(NUM_ITERATIONS):
    # [url, [links, rank]] schema for compute contribs
    joinedRDD=links.join(ranks)
    print ("join result " + str(joinedRDD.collect()))
    contribs = links.join(ranks).flatMap(compute_contribs)
    print ("contribs " + str(contribs.collect()))
    ranks = contribs.reduceByKey(lambda x, y: x + y).mapValues(lambda x: 0.15 + 0.85 * x)








ranks.collect()
