import multiprocessing
import time
import sys

import nltk
from pymongo import MongoClient
import gridfs

try:
    import cPickle as pickle
except:
    import pickle

from settings import Settings


def load_stopwords():
    stopwords = {}
    with open('stopwords.txt', 'rU') as f:
        for line in f:
            stopwords[line.strip()] = 1

    return stopwords


def worker(identifier, skip, count):
    done = 0
    start = time.time()

    stopwords = load_stopwords()
    reviews_collection = MongoClient(Settings.MONGO_CONNECTION_STRING)[Settings.REVIEWS_DATABASE][
        Settings.REVIEWS_COLLECTION]

    tags_db = MongoClient(Settings.MONGO_CONNECTION_STRING)[Settings.TAGS_DATABASE]
    tags_fs = gridfs.GridFS(tags_db)
    # tags_collection = MongoClient(Settings.MONGO_CONNECTION_STRING)[Settings.TAGS_DATABASE][
    #     Settings.REVIEWS_COLLECTION]

    batch_size = 5
    for batch in range(0, count, batch_size):
        reviews_cursor = reviews_collection.find().skip(skip + batch).limit(batch_size)

        # option 1: turn off the time out request, e.g., find(timetout=False)
        # option 2: estimate the size of a bache, 10 minutes to finish
        # for review in reviews_collection.find().skip(skip + batch).batch_size(batch_size):
        for review in reviews_cursor:
            words = []

            reviews = review['text'].split('\n\n')
            for r in reviews:
                
                sentences = nltk.sent_tokenize(r.lower())

                for sentence in sentences:
                    tokens = nltk.word_tokenize(sentence)
                    text = [word for word in tokens if word not in stopwords]
                    tagged_text = nltk.pos_tag(text)

                    for word, tag in tagged_text:
                        words.append({"word": word, "pos": tag})


            # sentences = nltk.sent_tokenize(review["text"].lower())

            # for sentence in sentences:
            #     tokens = nltk.word_tokenize(sentence)
            #     text = [word for word in tokens if word not in stopwords]
            #     tagged_text = nltk.pos_tag(text)

            #     for word, tag in tagged_text:
            #         words.append({"word": word, "pos": tag})

            # tags_collection.insert({
            #     "reviewId": review["reviewId"],
            #     "business": review["business"],
            #     # "text": review["text"],
            #     "words": words,
            #     "num_reviews": review['num_reviews']
            # })

            tags_fs.put(pickle.dumps(words), filename=review["business"], num_reviews=str(review['num_reviews']))

            done += 1
            if done % 10 == 0:
                end = time.time()
                print 'Worker' + str(identifier) + ': Done ' + str(done) + ' out of ' + str(count) + ' in ' + (
                    "%.2f" % (end - start)) + ' sec ~ ' + ("%.2f" % (done / (end - start))) + '/sec'
                sys.stdout.flush()


def main():
    reviews_collection = MongoClient(Settings.MONGO_CONNECTION_STRING)[Settings.REVIEWS_DATABASE][
        Settings.REVIEWS_COLLECTION]
    reviews_cursor = reviews_collection.find()
    count = reviews_cursor.count()
    workers = 5
    batch = count / workers
    left = count % workers

    jobs = []
    for i in range(workers):
        size = count/workers
        if i == workers - 1:
            size += left

        p = multiprocessing.Process(target=worker, args=((i + 1), i * batch, size))
        jobs.append(p)
        p.start()

    for j in jobs:
        j.join()
        print '%s.exitcode = %s' % (j.name, j.exitcode)


if __name__ == '__main__':
    main()