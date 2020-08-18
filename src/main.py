import configs as cfgs
from sqlite_manager import SqliteManager
from mongodb_manager import MongoDBManager

import helper_functions as hpf
from datetime import datetime
import time

while True:
  print("================ New =================")
  print(f"Start datetime {datetime.now()}")
  complete_start_time = time.time()
  
  # init
  start_time = time.time()

  sqlite_manager = SqliteManager()
  mongo_db_manager = MongoDBManager()
  
  sqlite_manager.init_table()
  sqlite_manager.import_code_to_send_code(cfgs.code_excel_path)

  mv_title = mongo_db_manager.get_title_of_movie(cfgs.target_mv_id)

  end_time = time.time()
  print(f"initial database time usage: {end_time - start_time} second.")
  # init

  # find new order in mongodb and insert to queue_to_send
  start_time = time.time()

  last_booking_at = sqlite_manager.get_queue_to_send_last_booking_at()
  if last_booking_at:
    last_booking_at = last_booking_at.split(' ')[0]
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

  end_time = time.time()
  print(f"find new order in mongodb time usage: {end_time - start_time} second.")
  # find new order in mongodb and insert to queue_to_send

  # send email
  start_time = time.time()

  queue_to_sends = sqlite_manager.get_queue_to_send_detail(cfgs.queue_to_send_batch)
  code_to_send = sqlite_manager.get_code_to_send_detail(cfgs.code_to_send_batch)

  email_details = []
  queue_to_send_id_update = []
  code_to_send_detail_update = []
  data_error = {
    'queue_to_send_id': None,
    'reason': None,
    'datetime': None
  }
  
  code_idx = 0
  for queue_to_send in (queue_to_sends):
    temp_send_detail = {
      'quere_to_send_id': queue_to_send['id'],
      'mv_name': mv_title,
      'receiver_email': queue_to_send['email'],
      'codes': []
    }
    queue_to_send_id_update.append(queue_to_send['id'])
    
    for _ in range(int(queue_to_send['n_seat'])):
      try:
        temp_send_detail['codes'].append(code_to_send[code_idx]['code'])
        code_to_send_detail_update.append({
          'code_to_send_id' : code_to_send[code_idx]['id'],
          'queue_to_send_id': queue_to_send['id']
        })
        code_idx += 1
      except:
        data_error['queue_to_send_id'] = queue_to_send['id']
        data_error['reason'] = "Code not enough"
        data_error['datetime'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        break
    
    if data_error['datetime']:
      if not sqlite_manager.last_error_found_is_today():
        hpf.send_email_error(data_error)
        sqlite_manager.insert_error_logs(data=data_error)
      break
    else:
      email_details.append(temp_send_detail)

  # error_queue_to_send_ids = hpf.send_email_reward(email_details)
  error_queue_to_send_ids = []
  
  end_time = time.time()
  print(f"send email time usage: {end_time - start_time} second.")
  # send email

  # update sqlite data
  start_time = time.time()
  
  cut_out_error_queue_to_send_id_update = hpf.get_only_diff_list(set(queue_to_send_id_update), set(error_queue_to_send_ids))
  cut_out_error_code_to_send_detail_update = []
  for detail in code_to_send_detail_update:
    if detail['queue_to_send_id'] not in error_queue_to_send_ids:
      cut_out_error_code_to_send_detail_update.append(detail)
  
  sqlite_manager.update_queue_to_send(cut_out_error_queue_to_send_id_update)
  sqlite_manager.update_code_to_send(cut_out_error_code_to_send_detail_update)

  end_time = time.time()
  print(f"update database time usage: {end_time - start_time} second.")
  # update sqlite data

  complete_end_time = time.time()
  print(f"until complete time usage: {complete_end_time - complete_start_time} second.")
  print(f"total send: {len(email_details)}")
  print("========================================================================")
  
  time.sleep(cfgs.sleep_time_each_round_in_sec)
