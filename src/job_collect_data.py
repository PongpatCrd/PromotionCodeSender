import configs as cfgs
from sqlite_manager import SqliteManager
from mongodb_manager import MongoDBManager

import helper_functions as hpf
from datetime import datetime

if __name__=='__main__':
  # init
  sqlite_manager = SqliteManager()
  mongo_db_manager = MongoDBManager()
  
  sqlite_manager.init_table()
  sqlite_manager.import_code_to_send_code(cfgs.code_excel_path)

  mv_title = mongo_db_manager.get_title_of_movie(cfgs.target_mv_id)
  # init

  # find new order in mongodb and insert to queue_to_send
  last_booking_at = sqlite_manager.get_queue_to_send_last_booking_at()
  if last_booking_at:
    last_booking_at = datetime.strptime(last_booking_at, '%Y-%m-%d')

  vista_booking_numbers = sqlite_manager.get_queue_to_send_vista_booking_number() 

  order_details = mongo_db_manager.get_all_order_in_time_period_with_filter(last_booking_at)
  
  new_queue_to_send = []
  if order_details:
    for order in order_details:
      if order['vista_booking_number'] not in vista_booking_numbers:
        new_queue_to_send.append({
          'email': order['email'],
          'vista_booking_number': order['vista_booking_number'],
          'booking_at': order['created_time'].strftime('%Y-%m-%d %H:%M:%S.%f'),
          'n_seat': order['n_seat']
        })
  
  sqlite_manager.insert_queue_to_send_detail(new_queue_to_send)
  # find new order in mongodb and insert to queue_to_send
  