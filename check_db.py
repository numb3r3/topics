import os
import time

from pymongo import MongoClient
from nltk.stem.wordnet import WordNetLemmatizer
import gridfs

try:
    import cPickle as pickle
except:
    import pickle

from settings import Settings

reviews_collection = MongoClient(Settings.MONGO_CONNECTION_STRING)[Settings.REVIEWS_DATABASE][
        Settings.REVIEWS_COLLECTION]
reviews_cursor = reviews_collection.find()

tags_db = MongoClient(Settings.MONGO_CONNECTION_STRING)[Settings.TAGS_DATABASE]
tags_fs = gridfs.GridFS(tags_db)

reviewsCount = reviews_cursor.count()
# reviews_cursor.batch_size(5000)

lem = WordNetLemmatizer()

done = 0
start = time.time()

lefts = []
for review in reviews_cursor:
    bid = review['business']
    if tags_db.fs.files.find({"filename": bid}).count() == 0:
        lefts.append(bid)

    done += 1
    if done % 100 == 0:
        end = time.time()
        os.system('cls')
        print 'Done ' + str(done) + ' out of ' + str(reviewsCount) + ' in ' + str((end - start))

print 'left: %d' % len(lefts)