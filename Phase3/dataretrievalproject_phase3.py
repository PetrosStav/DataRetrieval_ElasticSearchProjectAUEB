# Import all necessary modules
from elasticsearch import Elasticsearch
import subprocess
import numpy as np
import time

# Initialize an Elasticsearch object without arguments (with defaults to localhost:9200)
es = Elasticsearch()


# Method that tests the queries in testingQueries.txt, which were given in phase 1, to the index that was created in
# phase1, then stores the results in the file qresults_18316_phase2.txt
def test_queries_phase1():
    # Open the testingQueries file with utf-8 encoding
    with open('./testingQueries.txt', encoding='utf-8') as file:
        # Create and open the qresults_18316_phase1 file with utf-8 encoding
        with open('./qresults_18316_phase1.txt', 'w', encoding='utf-8') as outfile:
            # Iterate all the lines of the testingQueries file, enumerating them
            for idx, line in enumerate(file):
                # Create the query_id variable according to the number of the line we are at (idx)
                query_id = 'Q0%d' % (idx + 1) if (idx + 1 < 10) else 'Q%d' % (idx + 1)
                # If we are in the first line
                if idx == 0:
                    # Remove the first 5 characters of the line and set it as the query_text
                    # (1st char is the utf-8 flag, 2-4th chars is the query id, 5th char is a whitespace)
                    query_text = line[5:]
                else:
                    # For every other line
                    # Remove the first 4 characters of the line and set it as the query_text
                    # (1-3th chars is the query id, 4th char is a whitespace)
                    query_text = line[4:]
                # Create the dictionary that has the query properties and the query_text
                query = {
                    "query": {
                        "match": {
                            "text": query_text
                        }
                    },
                    "size": 10000
                }
                # Invoke the api call to run the query for the index and document type specified as arguments,
                # as well as the query dictionary and store the results in the variable res
                res = es.search(index='dataretrievalproject', doc_type='project', body=query, scroll='1m')
                # Create a list of the ids of all the similar documents returned in the result
                ids = [item['_id'] for item in res['hits']['hits']]
                # Create a list of the scores of all the similar documents returned in the result
                scores = [item['_score'] for item in res['hits']['hits']]
                # Delete the first id from the result
                del ids[0]
                # Delete the first score from the result
                del scores[0]
                # For every pair of id and score
                for rank_, (id_, score_) in enumerate(zip(ids, scores)):
                    # Write to the qresults file the query_id, the id, the rank (incrementing number of pair) and the score
                    outfile.write('%s Q0 %s %s %s STANDARD\n' % (query_id, str(id_), str(rank_ + 1), str(score_)))
                # Get the rest from scroll
                scroll = res['_scroll_id']
                # Get the rest results
                res = es.scroll(scroll_id=scroll, scroll='1m')
                # Create a list of the ids of all the similar documents returned in the result
                ids = [item['_id'] for item in res['hits']['hits']]
                # Create a list of the scores of all the similar documents returned in the result
                scores = [item['_score'] for item in res['hits']['hits']]
                # For every pair of id and score
                for rank_, (id_, score_) in enumerate(zip(ids, scores)):
                    # Write to the qresults file the query_id, the id, the rank (incrementing number of pair) and the score
                    outfile.write(
                        '%s Q0 %s %s %s STANDARD\n' % (query_id, str(id_), str(9999 + rank_ + 1), str(score_)))


# Method that tests the queries in testingQueries_2.txt, which were created in phase 2, to the index that was created in
# phase1, then stores the results in the file qresults_18316_phase2.txt
def test_queries_phase2():
    # Open the testingQueries_2 file with utf-8 encoding
    with open('./testingQueries_2.txt', encoding='utf-8') as file:
        # Create and open the qresults_18316_phase2 file with utf-8 encoding
        with open('./qresults_18316_phase2.txt', 'w', encoding='utf-8') as outfile:
            # Iterate all the lines of the testingQueries_2 file, enumerating them
            for idx, line in enumerate(file):
                # Create the query_id variable according to the number of the line we are at (idx)
                query_id = 'Q0%d' % (idx + 1) if (idx + 1 < 10) else 'Q%d' % (idx + 1)
                # Set the line as the query_text
                query_text = line
                # Create the dictionary that has the query properties and the query_text
                query = {
                    "query": {
                        "match": {
                            "text": query_text
                        }
                    },
                    "size": 10000
                }
                # Invoke the api call to run the query for the index and document type specified as arguments,
                # as well as the query dictionary and store the results in the variable res
                res = es.search(index='dataretrievalproject', doc_type='project', body=query, scroll='1m')
                # Create a list of the ids of all the similar documents returned in the result
                ids = [item['_id'] for item in res['hits']['hits']]
                # Create a list of the scores of all the similar documents returned in the result
                scores = [item['_score'] for item in res['hits']['hits']]
                # Delete the first id from the result
                del ids[0]
                # Delete the first score from the result
                del scores[0]
                # For every pair of id and score
                for rank_, (id_, score_) in enumerate(zip(ids, scores)):
                    # Write to the qresults file the query_id, the id, the rank (incrementing number of pair) and the score
                    outfile.write('%s Q0 %s %s %s STANDARD\n' % (query_id, str(id_), str(rank_ + 1), str(score_)))
                # Get the rest from scroll
                scroll = res['_scroll_id']
                # Get the rest results
                res = es.scroll(scroll_id=scroll, scroll='1m')
                # Create a list of the ids of all the similar documents returned in the result
                ids = [item['_id'] for item in res['hits']['hits']]
                # Create a list of the scores of all the similar documents returned in the result
                scores = [item['_score'] for item in res['hits']['hits']]
                # For every pair of id and score
                for rank_, (id_, score_) in enumerate(zip(ids, scores)):
                    # Write to the qresults file the query_id, the id, the rank (incrementing number of pair) and the score
                    outfile.write(
                        '%s Q0 %s %s %s STANDARD\n' % (query_id, str(id_), str(9999 + rank_ + 1), str(score_)))


# Method that uses the similarity scores from the results of the queries from phase1 and those from phase2, to get a
# better similarity score according to the formula given:
#   sim_score_3(q,d) = lam * sim_score_1(q,d) + (1 - lam) * sim_score_2(q,d)
# where lam is a parameter / weight for each sim_score
def get_score(lam=0.5):
    # Initialize sim_score_3 as a dictionary
    sim_score_3 = dict()
    # Put query ids Q01..Q10 as keys in sim_score_3 with dictionaries as values
    for i in range(10):
        sim_score_3['Q%02d' % (i + 1)] = dict()
    # For each index i for the query ids
    for i in range(10):
        # Get the intersecting document ids of sim_score_1 and sim_score_2 for the query id of index i
        # and iterate for each document id
        for d in intersecting_per_q[i]:
            # Put the new similarity score that is derived from the formula to the sim_score_3 dictionary for the query
            # id of index i and for document id d
            sim_score_3['Q%02d' % (i + 1)][d] = lam * float(sim_score_1['Q%02d' % (i + 1)][d]) + (1 - lam) * \
                                                float(sim_score_2['Q%02d' % (i + 1)][d])
    # For every query id q in sim_score_3
    for q in sim_score_3.keys():
        # Sort the sim_score_3 dictionary of q key using the similarity score in reverse order
        sim_score_3[q] = sorted(sim_score_3[q].items(), key=lambda x: x[1], reverse=True)

    # Create a temporary file for the retrieved documents with their similarity score of sim_score_3
    with open('./qresults_phase3.txt', 'w', encoding='utf-8') as outfile:
        # For every query id q in sim_score_3
        for q in sim_score_3.keys():
            # Get the first (and best) 20 results / retrieved documents with their similarity scores for query id q
            res = sim_score_3[q][:20]
            # Get the documents ids from res
            ids = [e[0] for e in res]
            # Get the similarity scores from res
            scores = [e[1] for e in res]
            # For every pair of id and score
            for rank_, (id_, score_) in enumerate(zip(ids, scores)):
                # Write to the qresults file the query_id, the id, the rank (incrementing number of pair) and the score
                outfile.write(
                    '%s Q0 %s %s %s STANDARD\n' % (q, str(id_), str(rank_ + 1), str(score_)))
    # Run trec_eval with the subprocess module using the '-m map' configuration to only get the map score for the 20
    # first document for each query id in temp file qresults_phase3.txt
    p = subprocess.Popen(["trec_eval.exe", "-m", "map", "qrels.txt", "qresults_phase3.txt"], stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE, shell=True)
    # Get the output pipeline and the error pipeline
    (output, err) = p.communicate()
    p_status = p.wait()
    # Decode the output using utf-8 as a list
    o = output.decode('utf-8').split()
    # Get the MAP score from the result
    score = o[2]
    # Return the MAP score
    return score


""" Main code """

# Test the queries from phase 1 and create the according file
test_queries_phase1()
print("Created file qresults_18316_phase1.txt")

# Sleep for 2 seconds
time.sleep(2)

# Test the queries from phase 2 and create the according file
test_queries_phase2()
print("Created file qresults_18316_phase2.txt")

# Sleep for 2 seconds
time.sleep(2)

# Initialize sim_score_1 as a dictionary
sim_score_1 = dict()
# Initialize sim_score_2 as a dictionary
sim_score_2 = dict()

# Put query ids Q01..Q10 as keys in sim_score_1 and sim_score_2 with dictionaries as values
for i in range(10):
    sim_score_1['Q%02d' % (i + 1)] = dict()
    sim_score_2['Q%02d' % (i + 1)] = dict()

# Open the file with the sim_scores from phase 1 queries
with open('qresults_18316_phase1.txt', encoding='utf-8') as file:
    # For each line in the file
    for line in file:
        # Split the line
        line = line.split(' ')
        # Add to the dictionary with the correct query id - document id - score
        #   line[0] is the query id (Q01..Q10)
        #   line[2] is the document id
        #   line[4] is the sim score
        sim_score_1[line[0]][line[2]] = line[4]

# Open the file with the sim_scores from phase 2 queries
with open('qresults_18316_phase2.txt', encoding='utf-8') as file:
    # For each line in the file
    for line in file:
        # Split the line
        line = line.split(' ')
        # Add to the dictionary with the correct query id - document id - score
        #   line[0] is the query id (Q01..Q10)
        #   line[2] is the document id
        #   line[4] is the sim score
        sim_score_2[line[0]][line[2]] = line[4]

# Get the document ids of each query id for sim_score_1
documents_per_q_1 = [sim_score_1[e].keys() for e in
                     ['Q01', 'Q02', 'Q03', 'Q04', 'Q05', 'Q06', 'Q07', 'Q08', 'Q09', 'Q10']]

# Get the document ids of each query id for sim_score_2
documents_per_q_2 = [sim_score_2[e].keys() for e in
                     ['Q01', 'Q02', 'Q03', 'Q04', 'Q05', 'Q06', 'Q07', 'Q08', 'Q09', 'Q10']]

# Initialize a list that will contain the intersecting document ids from sim_score_1 and sim_score_2 for each query id
# (this is needed, as there are document ids that are not returned because of zero similarity score)
intersecting_per_q = []
# For each index i for the query ids
for i in range(10):
    # Append to the list the intersection of the sets of the document ids for the ith query id for sim_score_1
    # and sim_score_2
    intersecting_per_q.append(set(documents_per_q_1[i]).intersection(set(documents_per_q_2[i])))

# Get 1000 values from the linear space 0.0 - 1.0 as lambda values in order to find the best lambda value and create
# a list of all of them
lam = np.linspace(0.0, 1.0, 1000)
# Initialize a list to store all the MAP scores for the different lambda values
lam_scores = []
# For every lambda value in lam array
for l in lam:
    # Print the lambda value that is being tested
    print('Testing lam %f' % l, end='')
    # Get the MAP score for the l lambda value
    score = float(get_score(l))
    # Print the MAP score for the lambda value
    print(' -- Score: %f' % score)
    # Add the lambda value and the its MAP score on the list
    lam_scores.append((l, score))
# Print a separator
print('-' * 30)
# Sort the lambda-MAP score array in reverse order according to the MAP
lam_scores = sorted(lam_scores, key=lambda x: x[1], reverse=True)
# Print the best lambda value and its MAP score
print('Best --', 'Lam:', lam_scores[0][0], 'Score:', lam_scores[0][1])
