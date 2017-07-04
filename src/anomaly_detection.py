from collections import defaultdict
import simplejson as json
import sys


# Set empty variables and lists
d = 0
t = 0
list_output = []
friends = defaultdict(list)
list_file = []
output_str = ''
all_parts = {}

# Create output file of purchase anomalies
def write_output(output_path,list_output):
        with open(output_path, 'w') as outfile:
            outfile.write(str(list_output))

# Gets the average & standard deviation of the purchses inside network
def get_sd(list_t):
    step1 = 0
    sd = 0
    total = 0
    cnt = len(list_t)
    avg = 0.00
    for h in list_t:
        total += float(h['amount'])
    avg = float('%.2f'%(total / cnt))
    for f in list_t:
        step1 += (float(f['amount']) - avg) ** 2
    sd = float('%.2f'%(((1 / cnt) * step1) ** 0.5))
    return avg, sd


# Checks each purchase to detect an anomaly
def get_output(i, avg, sd):
    global output_str
    part1 = {}
    if float(i['amount']) > 0 and float(i['amount']) > avg + (3 * sd) and sd != 0:
        for k, v in iter(i.items()):
            part1[k] = v
            part2 = {'mean': "%.2f"%avg, 'sd': "%.2f"%sd}
            all_parts = {**part1, **part2}

        # Write lines for output file
        output_str = output_str +'{'
        for key, val in iter(all_parts.items()):
            output_str = output_str + '"'+key+'"' + ':' + '"'+str(val)+'"' + ', '
            if key == "sd":
                output_str = output_str[:-2]
                output_str = output_str +'}'
                output_str = output_str + '\n'

# Create connections between two friends
def addEdge(id1, id2):
    friends[id1].append(id2)
    friends[id2].append(id1)


# Remove connections between two people
def removeEdge(id1, id2):
    friends[id1].remove(id2)
    friends[id2].remove(id1)


# Takes parameters from input file
# 'd' defines degrees of a social network,
# 't' defines total number of purchases in a network to check
def get_parameters(data):
    d = int(data[0]['D'])
    t = int(data[0]['T'])
    return d, t


# Creates a list of 't' purchases within a user's social network
def get_transactions(data, j, network_list):
    list_t = []
    for w in reversed(range(1, j, 1)):
        if data[w]["event_type"] == "purchase":
            if data[w]["id"] in network_list:
                if len(list_t) < t:
                    list_t.append(data[w])
    return list_t


# A function to perform a Depth-Limited search
# from given source user 'id'
def DLS(src, maxDepth, network):
    # Create person's network
    network.add(src)

    # If reached the maximum depth, stop recursing.
    if maxDepth <= 0: return False
    # Recur for all the vertices adjacent to this vertex
    for i in friends[src]:
        DLS(i, maxDepth - 1, network)


# IDDFS searches with recursive DLS() on each level of depth
def IDDFS(src, d):
    network = set()
    maxDepth = d + 1

    for i in range(maxDepth):
        DLS(src, i, network)
    network.remove(src)
    return network


# Opens input files & loads json data and runs sort()
def filter_json(input1, input2):
    global d
    global t

    content = open(input1, "r").read()
    data = [json.loads(str(i)) for i in content.strip().split('\n') if i.strip()]

    content2 = open(input2, "r").read()
    data2 = [json.loads(str(i)) for i in content2.strip().split('\n') if i.strip()]

    d, t = get_parameters(data)
    sort(data, data2)


# Checks event_type for "purchase", "befriend", or "unfriend"
# then runs functions for that event_type
def sort(data, data2):
    global t
    global d
    global list_output
    global list_file
    o = 0

    for j, i in enumerate(data):

        try:

            if i["event_type"] == "purchase":
                try:
                    if i == data[-1] and o >= 1:
                        network_list = IDDFS(i["id"], d)

                        list_t = get_transactions(data, j, network_list)

                        avg, sd = get_sd(list_t)

                        get_output(i, avg, sd)

                except KeyError:
                    continue
            elif i["event_type"] == 'befriend':
                addEdge(i["id1"], i["id2"])

            elif i["event_type"] == "unfriend":
                removeEdge(i["id1"], i["id2"])

        except KeyError:
            continue

        # Checks if at eof of data, if true adds data2 line by line to data
        if i == data[-1]:
            if o == len(data2) - 1:
                data.append(data2[o])
                o += 1
            if o < len(data2) - 1:
                data.append(data2[o])
                o += 1



def main(argv):
    batch_data = argv[1]
    stream_data = argv[2]
    output_path = argv[3]
    filter_json(batch_data, stream_data)
    write_output(output_path, output_str)


if __name__ == "__main__":
    main(sys.argv)
