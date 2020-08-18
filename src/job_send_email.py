import configs as cfgs
from sqlite_manager import SqliteManager
from mongodb_manager import MongoDBManager
import helper_functions as hpf

from datetime import datetime

if __name__=='__main__':
  # init
  sqlite_manager = SqliteManager()
  mongo_db_manager = MongoDBManager()

  mv_title = mongo_db_manager.get_title_of_movie(cfgs.target_mv_id)
  # init

  # send email
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

  error_queue_to_send_ids = hpf.send_email_reward(email_details)
  
  cut_out_error_queue_to_send_id_update = get_only_diff_list(set(queue_to_send_id_update), set(error_queue_to_send_ids))
  cut_out_error_code_to_send_detail_update = []
  for detail in code_to_send_detail_update:
    if detail['queue_to_send_id'] not in error_queue_to_send_ids:
      cut_out_error_code_to_send_detail_update.append(detail)
  # send email

  # update sqlite data
  sqlite_manager.update_queue_to_send(cut_out_error_queue_to_send_id_update)
  sqlite_manager.update_code_to_send(cut_out_error_code_to_send_detail_update)
  # update sqlite data
  