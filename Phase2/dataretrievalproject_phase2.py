# Import all necessary modules
from elasticsearch import Elasticsearch
import os
import time

# Initialize an Elasticsearch object without arguments (with defaults to localhost:9200)
es = Elasticsearch()
# Set a variable for the index name
_index = 'dataretrievalproject'
# Set a variable for the index created from the collection2
_index2 = 'dataretrievalproject_collection2'
# Set a variable for the document type name
_doc = 'project'
# Set a variable for the returned documents number (size)
_size = 21

# query number to rcn match, corresponding to the table given
query_rcns = {
    'Q01': 193378,
    'Q02': 213164,
    'Q03': 204146,
    'Q04': 214253,
    'Q05': 212490,
    'Q06': 210133,
    'Q07': 213097,
    'Q08': 193715,
    'Q09': 197346,
    'Q10': 199879
}


# Method that creates the index from the collection2 that will be used in sub-question B
def create_index_for_collection2():
    # Create the mapping settings for the documents, specifying the fields and their type
    # as well as the english analyzer and search_analyzer for the field "text"
    mapping = {
        "mappings": {
            "project": {
                "properties": {
                    "rcn": {
                        "type": "integer"
                    },
                    "text": {
                        "type": "text",
                        "analyzer": "english",
                        "search_analyzer": "english"
                    }
                }
            }
        }
    }
    # Invoke the api call for the creation of the index with the given name from the argument
    res = es.indices.create(index=_index2, ignore=400, body=mapping)
    # Print the response of the API
    print(res)

    # Sleep main thread for 2 seconds
    time.sleep(2)

    # Index the collection_2 files

    # Set the path of the files
    path = "./Collection_2/"
    # Find the number of files
    num_of_files = len(os.listdir(path))

    i = 0
    # Iterate all the files in the path
    for file in os.listdir(path):
        i += 1
        # If a file's name ends with ".txt"
        if file.endswith('.txt'):
            # Open it with utf-8 encoding
            with open(path + file, encoding='utf-8') as openfile:
                # Read the file's text
                text = openfile.readline()
                # Get the rcn from the file name
                rcn = file[:-4]
                # Create a dictionary from the fields extracted
                document = {
                    'rcn': int(rcn),
                    'text': text,
                }
                # Invoke the api call for the indexing of the document with the given name of the index,
                # the document type and the dictionary containing the data of the document
                es.index(index=_index2,
                         doc_type=_doc, id=int(rcn), body=document)
                # Every 50 documents / files print the progress to the console
                if i % 50 == 0:
                    print('\rProgress %f%%' % ((float(i) / num_of_files) * 100), end='')

    # Sleep main thread for 2 seconds
    time.sleep(2)

    # Change simscore to TF-IDF

    # Invoke the api call to close the index, so that the next method can run
    es.indices.close(index=_index2)
    # Create the settings needed for the similarity score change according to the
    # argument the user has input, or else classic as a default (TF-IDF)
    settings = {
        "index": {
            "similarity": {
                "default": {
                    "type": "classic"
                }
            }
        }
    }
    # Invoke the api call to change settings (or put settings) for a specified index in elasticsearch
    res = es.indices.put_settings(index=_index2, body=settings)
    print(res)
    # Invoke the api call to open the closed index, so that it is usable (read/write)
    es.indices.open(index=_index2)


# Method that deletes the index from the collection2 that is used in sub-question B
def delete_index_for_collection2():
    # Invoke the api call for deletion of the index with the given name from the argument
    res = es.indices.delete(index=_index2)
    # Return the results of the api call
    return res


# Method that creates the queries from the collection2 that is used in sub-questions A and B
def create_queries_collection2():
    # Initialize a list of all the query ids
    query_ids = ['Q01', 'Q02', 'Q03', 'Q04', 'Q05', 'Q06', 'Q07', 'Q08', 'Q09', 'Q10']
    # Open the txt file that the queries will be writen to with utf-8 encoding
    with open('./testingQueries_2.txt', 'w', encoding='utf-8') as outfile:
        # For every query id
        for idx, query_id in enumerate(query_ids):
            # Get the corresponding query rcn
            query_rcn = query_rcns[query_id]
            # Initialize the keywords list
            keywords = []
            # Open the correct file from the collection 2 and get its' keywords
            with open('./Collection_2/%d.txt' % query_rcn) as file:
                # For every line in the file
                for line in file:
                    # Get the keywords of the line splitting them by spaces
                    keywords = line.strip()
            # Write the keywords to the file
            outfile.write(keywords)
            # Write a newline to the file except if last query_id
            if idx != 9:
                outfile.write('\n')


# Method that tests the queries from the collection2 to the given index
#  used in sub-questions A and B (use post argument accordingly)
def test_queries_collection2(index, doc, size=21, post='a'):
    # Open the queries from the collection2 that were created using utf-8 encoding
    with open('./testingQueries_2.txt', encoding='utf-8') as file:
        # Open a file to write the results
        with open('./qresults_%d%s.txt' % (size - 1, post), 'w', encoding='utf-8') as outfile:
            # For every line in the queries file
            for idx, line in enumerate(file):
                # Find the query id
                query_id = 'Q0%d' % (idx + 1) if (idx + 1 < 10) else 'Q%d' % (idx + 1)
                # Get the text for the query
                query_text = line
                # Create the query dictionary that will be used as payload for the elasticsearch api
                query = {
                    "query": {
                        "match": {
                            "text": query_text
                        }
                    },
                    "size": size
                }
                # Invoke the api call to search elasticsearch using the query and get the results to the specified index
                res = es.search(index=index, doc_type=doc, body=query)
                # Get the ids from the results
                ids = [item['_id'] for item in res['hits']['hits']]
                # Get the scores from the results
                scores = [item['_score'] for item in res['hits']['hits']]
                # Delete the first id and score
                del ids[0]
                del scores[0]
                # Write the remaining results in the file using the correct format
                for rank_, (id_, score_) in enumerate(zip(ids, scores)):
                    outfile.write('%s Q0 %s %s %s STANDARD\n' % (query_id, str(id_), str(rank_ + 1), str(score_)))


# Method that test the queries from phrase 1 to the given index using More Like This (sub-question C)
#  using the arguments: max query terms, min term freq, min doc freq, max doc freq, min should match
def test_queries_mlt(index, doc, size=21, max_q_t=25, min_t_f=2, min_d_f=5, max_d_f=0, min_s_m='30%'):
    # Open the queries file from the phase 1 using utf-8 encoding
    with open('./testingQueries.txt', encoding='utf-8') as file:
        # Open a file to write the results whose name is specified by the MLT arguments
        with open('./mlt_tests/qresults_%d_mlt_maqt%d_mitf%d_midf%d_madf%d_mism%s.txt' % (
        size - 1, max_q_t, min_t_f, min_d_f, max_d_f, min_s_m[:-1]), 'w', encoding='utf-8') as outfile:
            # For every line in the queries file
            for idx, line in enumerate(file):
                # Find the query id
                query_id = 'Q0%d' % (idx + 1) if (idx + 1 < 10) else 'Q%d' % (idx + 1)
                # Get the query text
                if idx == 0:
                    query_text = line[5:]
                else:
                    query_text = line[4:]
                # If the max document frequency argument is 0 (which the default value), don't specify it in the
                # dictionary, because instead of it being unbounded, it will instead set it really to 0,
                # which return no results
                if max_d_f == 0:
                    # Create the MLT query dictionary used in the api call as a payload using the MLT arguments
                    query = {
                        "query": {
                            "more_like_this": {
                                "fields": ["text"],
                                "like": query_text,
                                "max_query_terms": max_q_t,
                                "min_term_freq": min_t_f,
                                "min_doc_freq": min_d_f,
                                "minimum_should_match": min_s_m
                            }
                        },
                        "size": size
                    }
                else:
                    # Create the MLT query dictionary used in the api call as a payload using the MLT arguments
                    query = {
                        "query": {
                            "more_like_this": {
                                "fields": ["text"],
                                "like": query_text,
                                "max_query_terms": max_q_t,
                                "min_term_freq": min_t_f,
                                "min_doc_freq": min_d_f,
                                "max_doc_freq": max_d_f,
                                "minimum_should_match": min_s_m
                            }
                        },
                        "size": size
                    }
                # Invoke the api call to search elasticsearch using the query and get the results to the specified index
                res = es.search(index=index, doc_type=doc, body=query)
                # Get the ids from the results
                ids = [item['_id'] for item in res['hits']['hits']]
                # Get the scores from the results
                scores = [item['_score'] for item in res['hits']['hits']]
                # Delete the first id and score
                del ids[0]
                del scores[0]
                # Write the remaining results in the file using the correct format
                for rank_, (id_, score_) in enumerate(zip(ids, scores)):
                    outfile.write('%s Q0 %s %s %s STANDARD\n' % (query_id, str(id_), str(rank_ + 1), str(score_)))


'''Main code'''

'''Delete the index created from the collection2'''

delete_index_for_collection2()

'''Create an index from the collection2'''

# create_index_for_collection2()

'''Create the queries from collection2'''

create_queries_collection2()

'''Test the queries for sub-question A'''

test_queries_collection2(_index, _doc, _size, post='a')
print('Tested queries for A')

'''Test the queries for sub-question B'''

test_queries_collection2(_index2, _doc, _size, post='b')
print('Tested queries for B')

'''Test the mlt queries for sub-question C'''

# Create lists for all the values to check for the MLT arguments
# Max Query Terms
maqts = [40, 25, 10]
# Min Term Frequency
mitfs = [5, 2, 1]
# Min Document Frequency
midfs = [10, 5, 1]
# Max Document Frequency
madfs = [1000, 500, 0]
# Min Should Match
misms = ['10%', '20%', '30%']
# Check every combination of the above available MLT arguments
for maqt in maqts:
    for mitf in mitfs:
        for midf in midfs:
            for madf in madfs:
                for mism in misms:
                    test_queries_mlt(_index, _doc, _size, maqt, mitf, midf, madf, mism)
print('Tested MLT queries for C')
