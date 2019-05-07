# Import all necessary modules
from elasticsearch import Elasticsearch
import os
import xmltodict
import time

# Initialize an Elasticsearch object without arguments (with defaults to localhost:9200)
es = Elasticsearch()
# Set a variable for the index name
_index = 'dataretrievalproject'
# Set a variable for the document type name
_doc = 'project'


# Method that creates an index in elasticsearch
def create_index(index):
    # Create the mapping settings for the documents, specifying the fields and their type
    # as well as the english analyzer and search_analyzer for the field "text"
    mapping = {
        "mappings": {
            "project": {
                "properties": {
                    "rcn": {
                        "type": "integer"
                    },
                    "acronym": {
                        "type": "text"
                    },
                    "text": {
                        "type": "text",
                        "analyzer": "english",
                        "search_analyzer": "english"
                    },
                    "identifier": {
                        "type": "text"
                    }
                }
            }
        }
    }
    # Invoke the api call for the creation of the index with the given name from the argument
    res = es.indices.create(index=index, ignore=400, body=mapping)
    # Return the result of the api call
    return res


# Method that deletes an index in elasticsearch
def delete_index(index):
    # Invoke the api call for deletion of the index with the given name from the argument
    res = es.indices.delete(index=index)
    # Return the results of the api call
    return res


# Method that loads (indexes) the files as documents in elasticsearch
def index_files(index, doc):
    # Set the path of the files
    path = "./Parsed files/"
    # Find the number of files
    num_of_files = len(os.listdir(path))

    i = 0
    # Iterate all the files in the path
    for file in os.listdir(path):
        i += 1
        # If a file's name ends with ".xml"
        if file.endswith('.xml'):
            # Open it with utf-8 encoding
            with open(path + file, encoding='utf-8') as openfile:
                # Parse the xml to a dictionary with xmltodict
                dic = xmltodict.parse(openfile.read())
                # Get all the fields from the dictionary that we need
                rcn = dic['project']['rcn']
                acronym = dic['project']['acronym']
                objective = dic['project']['objective']
                title = dic['project']['title']
                identifier = dic['project']['identifier']
                # Combine the title and the objective as a new field called "text"
                text = title + ' ' + objective
                # Create a dictionary from the fields extracted
                document = {
                    'rcn': int(rcn),
                    'acronym': acronym,
                    'text': text,
                    'identifier': identifier
                }
                # Invoke the api call for the indexing of the document with the given name of the index,
                # the document type and the dictionary containing the data of the document
                es.index(index=index,
                         doc_type=doc, id=int(rcn), body=document)
                # Every 50 documents / files print the progress to the console
                if i % 50 == 0:
                    print('\rProgress %f%%' % ((float(i) / num_of_files)*100), end='')


'''
    Choices for similarity score:

    'BM25' for BM25 -- also the default setting
    'classic' for TF-IDF
'''

# Method that changes the similarity score in an elasticsearch's index specified
def change_simscore(index, simscore='classic'):
    # Invoke the api call to close the index, so that the next method can run
    es.indices.close(index=index)
    # Create the settings needed for the similarity score change according to the
    # argument the user has input, or else classic as a default (TF-IDF)
    settings = {
        "index": {
            "similarity": {
                "default": {
                    "type": "%s" % simscore
                }
            }
        }
    }
    # Invoke the api call to change settings (or put settings) for a specified index in elasticsearch
    res = es.indices.put_settings(index=index, body=settings)
    # Invoke the api call to open the closed index, so that it is usable (read/write)
    es.indices.open(index=index)
    # Return the result of the api call to change the settings
    return res


# Method that runs the queries from the testingQueries.txt file for the index specified and creates the qresults file
# The user can specify the size,
# which is 21 in order to return the 20-most related documents, because the first is the query itself and it is discarded
# and 31 in order to return the 30-most related documents, for the same reason
def test_queries(index, doc, size=21):
    # Open the testingQueries file with utf-8 encoding
    with open('./testingQueries.txt', encoding='utf-8') as file:
        # Create and open the qresults_{size-1} file with utf-8 encoding
        with open('./qresults_%d.txt' % (size-1), 'w', encoding='utf-8') as outfile:
            # Print a message to the user
            print('First matches for each query -- MUST BE THE QUERY DOCUMENT\nID     SCORE')
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
                    "size": size
                }
                # Invoke the api call to run the query for the index and document type specified as arguments,
                # as well as the query dictionary and store the results in the variable res
                res = es.search(index=index, doc_type=doc, body=query)
                # Create a list of the ids of all the similar documents returned in the result
                ids = [item['_id'] for item in res['hits']['hits']]
                # Create a list of the scores of all the similar documents returned in the result
                scores = [item['_score'] for item in res['hits']['hits']]
                # Print the first id and score which must be the query itself, for validation purposes
                print(ids[0], scores[0])
                # Delete the first id from the result
                del ids[0]
                # Delete the first score from the result
                del scores[0]
                # For every pair of id and score
                for rank_, (id_, score_) in enumerate(zip(ids, scores)):
                    # Write to the qresults file the query_id, the id, the rank (incrementing number of pair) and the score
                    outfile.write('%s Q0 %s %s %s STANDARD\n' % (query_id, str(id_), str(rank_ + 1), str(score_)))


# Main code to get the results
# The unnecessary method calls can be commented out 
# (ex. if it is not needed to delete and re-create the index and load all the files, they should be commented out)

# print(delete_index('_index'))

# print('Deleted Index')

# time.sleep(1)

print(create_index(_index))

print('Created Index')

time.sleep(1)

print('Indexing Files...')

index_files(_index, _doc)

print('\nIndexed Files')

time.sleep(1)

print(change_simscore(_index, 'classic'))

print("Changed similarity score")

time.sleep(1)

test_queries(_index, _doc, 21)

test_queries(_index, _doc, 31)

print("Tested queries")
