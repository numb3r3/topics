import os
import time
import json

from pymongo import MongoClient

from settings import Settings


dataset_file = Settings.DATASET_FILE
reviews_collection = MongoClient(Settings.MONGO_CONNECTION_STRING)[Settings.REVIEWS_DATABASE][
    Settings.REVIEWS_COLLECTION]

count = 0
done = 0
start = time.time()

for dirname, dirnames, filenames in os.walk(dataset_file):
                count = len(filenames)
                for filename in filenames:
                    file_path = os.path.join(dirname, filename)
                    biz = json.load(open(file_path, 'r'))

                    if 'id' in biz:
                        bid = biz['id']
                    else:
                        bid = biz['product_id']


                    if len(biz['reviews']) <= 50:
                        continue

                    text = ''
                    for r in biz['reviews']:
                        rid = r['review_id']
                        r['business_id'] = bid

                        content = r['content']
                        text += content + '\n\n'

                        # date = r['date']

                    reviews_collection.insert({
                        "reviewId": bid,
                        "business": bid,
                        "text": text,
                        "num_reviews": len(biz['reviews'])
                    })

                    done += 1
                    if done % 100 == 0:
                        end = time.time()
                        os.system('cls')
                        print 'Done ' + str(done) + ' out of ' + str(count) + ' in ' + str((end - start))
