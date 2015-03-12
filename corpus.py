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


# tags_collection = MongoClient(Settings.MONGO_CONNECTION_STRING)[Settings.TAGS_DATABASE][Settings.REVIEWS_COLLECTION]
tags_db = MongoClient(Settings.MONGO_CONNECTION_STRING)[Settings.TAGS_DATABASE]
tags_fs = gridfs.GridFS(tags_db)

corpus_collection = MongoClient(Settings.MONGO_CONNECTION_STRING)[Settings.TAGS_DATABASE][Settings.CORPUS_COLLECTION]

reviews_cursor = tags_db.fs.files.find(no_cursor_timeout=True)
reviewsCount = reviews_cursor.count()
# reviews_cursor.batch_size(5000)

lem = WordNetLemmatizer()

done = 0
start = time.time()

processed = []

for review in reviews_cursor:
    fname = review['filename']
    if fname in processed:
        continue
    processed.append(fname)
    num_reviews = review['num_reviews']
    d = tags_fs.get(review['_id']).read()
    
    review_words = pickle.loads(d)

    nouns = []
    words = [word for word in review_words if word["pos"] in ["NN", "NNS"]]

    for word in words:
        nouns.append(lem.lemmatize(word["word"]))

    corpus_collection.insert({
        # "reviewId": review["reviewId"],
        "business": fname,
        "num_reviews": int(num_reviews),
        # "text": review["text"],
        "words": nouns
    })

    done += 1
    if done % 100 == 0:
        end = time.time()
        os.system('cls')
        print 'Done ' + str(done) + ' out of ' + str(reviewsCount) + ' in ' + str((end - start))