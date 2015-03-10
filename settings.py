class Settings:
    def __init__(self):
        pass

    DATASET_FILE = '../yelp_nyc'
    MONGO_CONNECTION_STRING = "mongodb://localhost:27017/"
    REVIEWS_DATABASE = "yelp_nyc_restaurants"
    TAGS_DATABASE = "Tags"
    REVIEWS_COLLECTION = "Reviews"
    CORPUS_COLLECTION = "Corpus"