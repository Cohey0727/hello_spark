#Boilerplate stuff:
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("DegreesOfSeparation")
sc = SparkContext(conf = conf)

# The characters we wish to find the degree of separation between:
startCharacterID = 5306 #SpiderMan
targetCharacterID = 14  #ADAM 3,031 (who?)

# Our accumulator, used to signal when we find the target character during
# our BFS traversal.
hitCounter = sc.accumulator(0)

WHITE = 0
GRAY = 1
BLACK = 2

def convertToBFS(line):
    fields = line.split()
    characterID = int(fields[0])
    connections = []
    for connection in fields[1:]:
        connections.append(int(connection))

    color = WHITE
    distance = 9999

    if (characterID == startCharacterID):
        color = GRAY
        distance = 0

    return (characterID, (connections, distance, color))


def createStartingRdd():
    inputFile = sc.textFile("./dataset/marvel-graph.txt")
    return inputFile.map(convertToBFS)


def search(distance, connections):
    results = []
    for connection in connections:
        newCharacterID = connection
        newDistance = distance + 1
        newColor = GRAY
        if (targetCharacterID == connection):
            hitCounter.add(1)

        newEntry = (newCharacterID, ([], newDistance, newColor))
        results.append(newEntry)

    return results


def bfsMap(node):
    characterID = node[0]
    data = node[1]
    connections = data[0]
    distance = data[1]
    color = data[2]

    results = []

    #If this node needs to be expanded...
    if (color == GRAY):
        results = search(distance, connections)
        color = BLACK

    #Emit the input node so we don't lose it.
    results.append((characterID, (connections, distance, color)))
    return results

def bfsReduce(data1, data2):
    connections1 = data1[0]
    connections2 = data2[0]
    distance1 = data1[1]
    distance2 = data2[1]
    color1 = data1[2]
    color2 = data2[2]

    distance = min(distance1, distance2)
    color = max(color1, color2)
    connections = [*connections1, *connections2]

    # Preserve darkest color
    return (connections, distance, color)


#Main program here:
iterationRdd = createStartingRdd()

for iteration in range(0, 10):
    print("Running BFS iteration# " + str(iteration+1))

    # Create new vertices as needed to darken or reduce distances in the
    # reduce stage. If we encounter the node we're looking for as a GRAY
    # node, increment our accumulator to signal that we're done.
    mapped = iterationRdd.flatMap(bfsMap)

    # Note that mapped.count() action here forces the RDD to be evaluated, and
    # that's the only reason our accumulator is actually updated.
    print("Processing " + str(mapped.count()) + " values.")

    if (hitCounter.value > 0):
        print("Hit the target character! From " + str(hitCounter.value) \
            + " different direction(s).")
        break

    # Reducer combines data for each character ID, preserving the darkest
    # color and shortest path.
    iterationRdd = mapped.reduceByKey(bfsReduce)
