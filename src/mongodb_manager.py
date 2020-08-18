import configs as cfgs

from pymongo import MongoClient
from datetime import datetime, timedelta

class MongoDBManager:
  def __init__(self):
    self.client = None

  def open_db(self):
    self.client = MongoClient(cfgs.mtel_mongodb)
    return
  
  def close_db(self):
    self.client.close()
    return

  def get_title_of_movie(self, mv_id):
    self.open_db()
    db = self.client.major

    films_collection = db.films
    pipline = [
      { '$project': {'ID': 1, 'Title': 1} },
      { '$match': {'ID': mv_id} }
    ]
    film_docs = films_collection.aggregate(pipline)
    try:
      title = list(film_docs)[0]['Title']
    except:
      title = None
    self.close_db()
    return title

  def get_all_order_in_time_period_with_filter(self, datetime_after=None):
    if not datetime_after:
      datetime_after = datetime.now() - timedelta(hours=12)
      
    self.open_db()
    db = self.client.major

    pipline = [
      {'$project': {
          'movie_id': 1, 
          'email': 1, 
          'VistaBookingNumber': 1, 
          'paid': 1, 
          'seats': 1,
          'created_time': 1
        }
      },
      {'$match': {
          'paid': True,
          'movie_id': cfgs.target_mv_id,
          'created_time': {'$gte': datetime_after}
        }
      }
    ]

    order_collection = db.orders
    order_docs = order_collection.aggregate(pipline)
    order_map = []
    for detail in order_docs:
      temp = {
        'movie_id'            : detail['movie_id'],
        'email'               : detail['email'],
        'vista_booking_number': detail['VistaBookingNumber'],
        'n_seat'              : len(detail['seats']),
        'created_time'        : detail['created_time']
      }
      order_map.append(temp)
    return order_map
  