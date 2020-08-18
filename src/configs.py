import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# MongoDB
# prod
# mtel_mongodb = 'xxxx'

# test
mtel_mongodb = 'xxxx'
# MongoDB

# Sqlite
sqlite_db_name = os.path.join(BASE_DIR, 'database.db')
# Sqlite

# ETC
email_sender = 'test@majorcineplex.com'
email_server = ''
email_server = 'mail_server'
email_server_port = 'port in (int)'

email_sender_error = 'test@majorcineplex.com'
email_receiver_error = ['test@majorcineplex.com']
email_cc_error = ['test@majorcineplex.com']
email_server_error = ''
email_server_error = 'mail_server'
email_server_port_error = 'port in (int)'

n_try_to_send = 3
sleep_time_each_round_in_sec = 60
sleep_time_each_email_send_in_sec = 1 # not over 3
queue_to_send_batch = 10 # 0 mean no limit
code_to_send_batch = 0  # 0 mean no limit
code_excel_path = os.path.join(BASE_DIR, 'code_list.xlsx')
target_mv_id = 'HO00003840'
# ETC