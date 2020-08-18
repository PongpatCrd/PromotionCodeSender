import configs as cfgs
import helper_functions as hpf

import sqlite3
import pandas as pd
import os
from datetime import datetime

class SqliteManager:
  def __init__(self):
    self.conn = None

  def open_db(self):
    self.conn = sqlite3.connect(cfgs.sqlite_db_name)
    return

  def close_db(self):
    self.conn.commit()
    self.conn.close()
    return
  
  def init_table(self):
    sql_create_table_map_table_name = {
      'queue_to_send': '''
        CREATE TABLE queue_to_send (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          email VARCHAR(255),
          vista_booking_number VARCHAR(25) NOT NULL,
          booking_at DATETIME NOT NULL, 
          is_send BOOLEAN DEFAULT false,
          n_seat INTEGER DEFAULT 0,
          created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );
      ''',
      'code_to_send': '''
        CREATE TABLE code_to_send (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          code VARCHAR(255) NOT NULL,
          is_send BOOLEAN DEFAULT false,
          queue_to_send_id INTEGER,
          expire DATETIME,
          file_datetime DATETIME,
          created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );
      ''',
      'error_logs'  :'''
        CREATE TABLE error_logs (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          reason TEXT,
          queue_to_send_id INTEGER,
          created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );
      '''
    }

    self.open_db()
    cursor = self.conn.execute(''' SELECT DISTINCT name FROM sqlite_master WHERE type='table' ''')
    all_table = [row[0] for row in cursor.fetchall()]
    
    for table_name in sql_create_table_map_table_name.keys():
      if table_name not in all_table:
        self.conn.execute(sql_create_table_map_table_name[table_name])
    self.close_db()

  def get_all_code_to_send_code(self):
    sql = '''
    SELECT code 
    FROM code_to_send
    WHERE is_send IS FALSE
    '''

    self.open_db()
    cursor = self.conn.execute(sql)
    codes = [row[0] for row in cursor.fetchall()]
    self.close_db()
    return codes

  def get_last_file_datetime(self):
    sql = '''
      SELECT file_datetime
      FROM code_to_send
      ORDER BY id DESC
      LIMIT 1
    '''
    self.open_db()

    cursor = self.conn.execute(sql)
    try:
      last_file_datetime = cursor.fetchone()[0]
    except:
      last_file_datetime = None
    
    self.close_db()
    return last_file_datetime

  def import_code_to_send_code(self, excel_path):
    if os.path.isfile(excel_path):
      current_file_datetime = datetime.fromtimestamp(os.path.getmtime(excel_path)).strftime('%Y-%m-%d %H:%M:%S')
      last_file_datetime = self.get_last_file_datetime()
    else:
      raise Exception('Excel file is not exist.')
    
    if not last_file_datetime or current_file_datetime != last_file_datetime:
      excel = pd.ExcelFile(excel_path)
      sheet_names = excel.sheet_names

      use_col_map = {
        "no": "A",
        "code": "B"
      }

      use_col_d_type_map = {
        "No": str,
        "Code": str
      }

      usecols = list(use_col_map.values())
      usecols = str(usecols)[1:-1]
      usecols = usecols.replace("'", "")
      
      in_excel_codes = []
      for sheet_name in sheet_names:
        excel_sheet = pd.read_excel(excel, header=0, sheet_name=sheet_name, usecols=usecols, converters=use_col_d_type_map, na_filter=False)   
        
        for index, row in excel_sheet.iterrows():
          try:
            if row[1]:
              in_excel_codes.append(row[1])
          except:
            pass
      
      if in_excel_codes:
        in_db_codes = self.get_all_code_to_send_code()
        only_code_not_in_db = hpf.get_only_diff_list(main_set=set(in_excel_codes), cut_out_set=set(in_db_codes))

        if only_code_not_in_db:
          sql = 'INSERT INTO code_to_send (code, file_datetime) VALUES '
          for code in only_code_not_in_db:
            sql += f'("{code}", "{current_file_datetime}"),'
          sql = sql[:-1]

          self.open_db()
          self.conn.execute(sql)
          self.close_db()
      excel.close()
    else:
      pass
    return

  def get_queue_to_send_detail(self, limit=0):
    sql = '''
    SELECT id, email, n_seat
    FROM queue_to_send
    WHERE is_send IS FALSE 
    ORDER BY id
    '''

    if limit > 0:
      sql += f"LIMIT {limit}"

    self.open_db()
    cursor = self.conn.execute(sql)
    details = []
    total_n_seat = 0
    for row in cursor.fetchall():
      details.append({
        'id'    : row[0],
        'email' : row[1],
        'n_seat': row[2]
      })
      total_n_seat += int(row[2])

    cfgs.code_to_send_batch = total_n_seat
    self.close_db()
    return details

  def get_code_to_send_detail(self, limit=0):
    sql = '''
    SELECT id, code
    FROM code_to_send
    WHERE is_send IS FALSE 
    ORDER BY id
    '''

    if limit > 0:
      sql += f"LIMIT {limit}"

    self.open_db()
    cursor = self.conn.execute(sql)
    details = []
    for row in cursor.fetchall():
      details.append({
        'id'  : row[0],
        'code': row[1]
      })
    self.close_db()

    if not details:
      details = None

    return details

  def get_queue_to_send_vista_booking_number(self):
    sql = '''
      SELECT vista_booking_number
      FROM queue_to_send
    '''

    self.open_db()
    cursor = self.conn.execute(sql)
    vista_booking_numbers = [row[0] for row in cursor.fetchall()]
    self.close_db()
    return vista_booking_numbers
    
  def update_queue_to_send(self, id_list):
    if not id_list:
      return

    sql = '''
      UPDATE queue_to_send
      SET is_send = 1
      WHERE id IN 
    '''

    if id_list:
      temp_value = ""
      for _id in id_list:
        temp_value += f"'{_id}',"
      
      temp_value = temp_value[:-1]
      sql += f"({temp_value})"
    else:
      sql += "('')"
    
    self.open_db()
    self.conn.execute(sql)
    self.close_db()
    return

  def update_code_to_send(self, code_to_send_detail_update):
    if not code_to_send_detail_update:
      return

    sql_list = []
    for detail in code_to_send_detail_update:
      sql = '''
      UPDATE code_to_send
      SET 
        is_send = 1,
        queue_to_send_id = '{0}'
      WHERE id = '{1}'
      '''.format(detail['queue_to_send_id'], detail['code_to_send_id'])
      
      sql_list.append(sql)
    
    self.open_db()
    for sql in sql_list:
      self.conn.execute(sql)
    self.close_db()
    return

  def insert_queue_to_send_detail(self, datas):
    '''
    datas: [
      {
        'email': ???(str),
        'vista_booking_number': ???(str),
        'booking_at': ???(str %Y-%m-%d %H:%M:%S.%f),
        'n_seat': ??(int)
      },...
    ]
    '''

    if not datas:
      return

    sql = '''
    INSERT INTO queue_to_send (email, vista_booking_number, booking_at, n_seat) VALUES 
    '''

    for data in datas:
      sql += "('{0}', '{1}', '{2}', '{3}'),".format(
        data['email'], 
        data['vista_booking_number'], 
        data['booking_at'],
        data['n_seat']
      )
    sql = sql[:-1]

    self.open_db()
    self.conn.execute(sql)
    self.close_db()
    return
    
  def get_queue_to_send_last_booking_at(self):
    sql = '''
      SELECT booking_at
      FROM queue_to_send
      ORDER BY booking_at DESC
      LIMIT 1
    '''

    self.open_db()
    
    cursor = self.conn.execute(sql)
    try:
      last_booking_at = cursor.fetchone()[0]
    except:
      last_booking_at = None

    self.close_db()
    return last_booking_at

  def insert_error_logs(self, data):
    '''
      data: {
        'queue_to_send_id': ???(str),
        'reason': ???(str)
        'datetime': ????(str)
      }
    '''

    sql = f'''
      INSERT INTO error_logs (queue_to_send_id, reason) 
      VALUES ('{data['queue_to_send_id']}', '{data['reason']}')
    '''

    self.open_db()
    self.conn.execute(sql)
    self.close_db()
    return
    
  def last_error_found_is_today(self):
    sql = '''
    SELECT 
      created_at, 
      datetime('now','localtime') as datetime_now
    FROM error_logs
    ORDER BY id DESC
    LIMIT 1
    '''

    self.open_db()
    cursor = self.conn.execute(sql)
    try:
      last_error_compare_datetime = cursor.fetchone()
    except:
      last_error_compare_datetime = None
    self.close_db()
    
    if not last_error_compare_datetime or last_error_compare_datetime[0][:10] != last_error_compare_datetime[1][:10]:
      return False
    else:
      return True
