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

nltk.download('wordnet')
from nltk.stem.wordnet import WordNetLemmatizer
lmtzr = WordNetLemmatizer()


# BASE_PATH = "/Users/qiaohe/cs121/WEBPAGES_RAW/"
# /Users/qiaohe/cs121/WEBPAGES_RAW/bookkeeping.json

##################################################################################


client = MongoClient()
client = MongoClient('localhost', 27017)
db = client['test-database']
db.inverted_indexs.drop()
inverted_indexs = db.inverted_indexs
operations = []

inverted_indexs.create_index("term")

def parse_the_file_and_save_to_mongodb (file_path, doc_id):
	global operations
	with open(file_path, 'r') as f:
		soup = BeautifulSoup(f, 'html.parser')
	word_list = re.findall(r'[^\W_\s]+', soup.get_text().encode('ascii','ignore'))
	token_dict = {}
	for word in word_list:
		lemmed_word = lmtzr.lemmatize(word.lower())
		if (token_dict.has_key(lemmed_word)):
			token_dict[lemmed_word] +=  1
		else:
			token_dict[lemmed_word] = 1 
	for key, value in token_dict.iteritems():
		operations.append(
			UpdateOne ( {'term':key},
			{
				'$inc': {'df': 1}, 
			  	'$push': {'pl': [doc_id,value]}
			}, 
			upsert=True )
			)
	if(len(operations) >= 1000):
		db.inverted_indexs.bulk_write (operations,ordered=False)
		operations = []
		

if __name__ == '__main__':
	base_path = "/Users/lizhenlin/Desktop/search_engine/WEBPAGES_RAW/" #sys.argv[1]
	with open("/Users/lizhenlin/Desktop/search_engine/WEBPAGES_RAW/bookkeeping.json") as data_file :  #sys.argv[2]
		datastore = json.load(data_file)
	count =0
	for x in datastore:
		# print "this is key: ",x
		if (re.search("html", datastore[x]) != None):
			count += 1
	print 'There are: ', count
			



	
















