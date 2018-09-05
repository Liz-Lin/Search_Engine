import json
import re, os
import sys
import collections
import operator

import pymongo
from pymongo import MongoClient
from pymongo import UpdateOne


#beautiful soup and nltk
from bs4 import BeautifulSoup
import nltk
import pprint
import math

nltk.download('wordnet')
from nltk.stem.wordnet import WordNetLemmatizer
lmtzr = WordNetLemmatizer()


client = MongoClient()
client = MongoClient('localhost', 27017)
db = client['test-database']
posts = db.inverted_indexs.find()

global docID_tf_dic
N = 12406
global datastore

#for post in posts:
#	print(post)
stopWords = set(['a', 'about', 'above', 'after', 'again', 'against', 'all', 'am', 'an',
				 'and', 'any', 'are', 'aren', 'as', 'at', 'be', 'because', 'been', 'before',
				 'being', 'below', 'between', 'both', 'but', 'by', 'can', 'cannot', 'could',
				 'couldn', 'd', 'did', 'didn', 'do', 'does', 'doesn', 'doing', 'don', 'down',
				 'during', 'each', 'few', 'for', 'from', 'further', 'had', 'hadn', 'has', 'hasn',
				 'have', 'haven', 'having', 'he', 'her', 'here', 'hers', 'herself', 'him', 'himself',
				 'his', 'how', 'i', 'if', 'in', 'into', 'is', 'isn', 'it', 'its', 'itself', 'let',
				 'll', 'm', 'me', 'more', 'most', 'mustn', 'my', 'myself', 'no', 'nor', 'not', 'of',
				 'off', 'on', 'once', 'only', 'or', 'other', 'ought', 'our', 'ours', 'ourselves', 'out',
				 'over', 'own', 're', 's', 'same', 'shan', 'she', 'should', 'shouldn', 'so', 'some',
				 'such', 't', 'than', 'that', 'the', 'their', 'theirs', 'them', 'themselves', 'then',
				 'there', 'these', 'they', 'this', 'those', 'through', 'to', 'too', 'under', 'until',
				 'up', 've', 'very', 'was', 'wasn', 'we', 'were', 'weren', 'what', 'when', 'where',
				 'which', 'while', 'who', 'whom', 'why', 'with', 'won', 'would', 'wouldn', 'you',
				 'your', 'yours', 'yourself', 'yourselves'])



def cal_tf_idf_score(df,tf):
	tf_score = 1 + math.log10(tf)
	idf_score = math.log10(N/df)
	tf_idf = float("{0:.2f}".format(tf_score * idf_score))
	return tf_idf;

def parse_query_and_search(query):
	tokens = re.split('[^a-z0-9]', query.lower())
	for word in tokens:
		if word in stopWords:
			continue
		else:
			single_word_and_search(word)



def single_word_and_search(single_word):
	global docID_tf_dic
	lemmed_word = lmtzr.lemmatize(single_word)
	results = db.inverted_indexs.find_one({"term": lemmed_word})
	df = results["df"]
	
	for doc_id, tf , indicator in results["pl"]:
		tf_idf = cal_tf_idf_score(df,tf)
		if (docID_tf_dic.has_key(doc_id)):
			docID_tf_dic[doc_id] += tf_idf;
		else:
			docID_tf_dic[doc_id] = tf_idf;


def rank_scores_and_return_k(query):
	global datastore
	new_list = docID_tf_dic.items()
	new_list.sort(key=operator.itemgetter(1), reverse=True)
	# check len of the doc list first 
	print "\n\n\nHere is the result of: ", query
	if (len(new_list)<15):
		max_range = len(new_list)
	else:
		max_range =15
	for k in range(max_range):
		doc = new_list[k][0]
		tf_idf = new_list[k][1]
		print "k:", (k+1), " tf_idf:",tf_idf ," doc_id:", doc, " URL:", datastore[doc]




if __name__ == '__main__':
	base_path = "/Users/lizhenlin/Desktop/search_engine/WEBPAGES_RAW/" #sys.argv[1]
	with open("/Users/lizhenlin/Desktop/search_engine/WEBPAGES_RAW/bookkeeping.json") as data_file :  #sys.argv[2]
		datastore = json.load(data_file)

	done = False 
	while (done == False):
		query = raw_input("please enter your query: ")
		if (query == "QUIT"):
			done = True
		else:
			docID_tf_dic = {}
			parse_query_and_search(query)
			rank_scores_and_return_k(query)


