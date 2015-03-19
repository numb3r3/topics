import logging

from gensim.models import LdaModel
from gensim import corpora
from gensim.corpora import BleiCorpus

from pymongo import MongoClient

from settings import Settings

from train import *

class IDCorpus(object):
    def __init__(self, cursor, reviews_dictionary, corpus_path):
        self.cursor = cursor
        self.reviews_dictionary = reviews_dictionary
        self.corpus_path = corpus_path

    def __iter__(self):
        self.cursor.rewind()
        for review in self.cursor:
            yield (review['business'], int(review['num_reviews']), self.reviews_dictionary.doc2bow(review["words"]))

    def serialize(self):
        BleiCorpus.serialize(self.corpus_path, self, id2word=self.reviews_dictionary)

        return self



def main():
    logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)

    dictionary_path = "models/dictionary.dict"
    corpus_path = "models/corpus.lda-c"
    lda_num_topics = 40
    lda_model_path = "models/lda_model_%d_topics.lda" % lda_num_topics

    dictionary = corpora.Dictionary.load(dictionary_path)
    corpus = corpora.BleiCorpus(corpus_path)
    lda = LdaModel.load(lda_model_path)

    corpus_collection = MongoClient(Settings.MONGO_CONNECTION_STRING)[Settings.TAGS_DATABASE][
        Settings.CORPUS_COLLECTION]
    reviews_cursor = corpus_collection.find()


    for bid, num, bow in IDCorpus(reviews_cursor, dictionary, corpus_path):
        dist = [0]*lda_num_topics
        for i, p in lda[bow]:
            dist[i] = p
        # print rid, 
        # print dist
        print '%s\t%s\t%s' % (bid, num, ','.join(['%.3f'%p for p in dist]))
        # print sum(dist)


if __name__ == '__main__':
    main()


