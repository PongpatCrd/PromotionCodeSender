import configs as cfgs

import time
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

def get_only_diff_list(main_set, cut_out_set):
  return list(main_set - cut_out_set)

def send_email_reward(datas):
  '''
  datas: [
    {
      'quere_to_send_id': ???,
      'mv_name': ???,
      'receiver_email': ???,
      'codes': [???, ???, ???](list)
    }
  ]
  '''
  # create connection and start TLS for security 
  s = smtplib.SMTP(cfgs.email_server, cfgs.email_server_port) 
  s.ehlo()
  s.starttls() 

  current_data_idx = 0

  for data in datas:
    msg = MIMEMultipart() 
    msg['From'] = cfgs.email_sender
    msg['To']   = data['receiver_email']
      
    # string to store the body of the mail 
    body = "You got these code from buying ticket for '{}'".format(data['mv_name'])
    body += "\nYour code list\n"

    for code in data['codes']:
      body += f"===> {code}\n"
      
    # attach the body with the msg instance 
    msg.attach(MIMEText(body, 'plain')) 
    
    # subject and attachment file 
    msg['Subject'] = "Your code reward!!"

    # creates SMTP session 
    is_send = False
    for i in range(cfgs.n_try_to_send):
      try:
        # # create connection and start TLS for security 
        # s = smtplib.SMTP(cfgs.email_server, cfgs.email_server_port) 
        # s.ehlo()
        # s.starttls() 
          
        # Converts the Multipart msg into a string 
        text = msg.as_string() 
          
        # sending the mail 
        s.sendmail(cfgs.email_sender, data['receiver_email'], text) 
        
        if cfgs.sleep_time_each_email_send_in_sec > 3:
          cfgs.sleep_time_each_email_send_in_sec = 3
        time.sleep(cfgs.sleep_time_each_email_send_in_sec)
          
        # # terminating the session 
        # s.quit()
        
        is_send = True
        current_data_idx += 1
        break
      except:
        pass
    
    if not is_send:
      break
  # terminating the session 
  s.quit()
  
  error_queue_to_send_ids = []
  while True:
    try:
      error_queue_to_send_ids.append(datas[current_data_idx]['queue_to_send_id'])
      current_data_idx += 1
    except:
      break
  return error_queue_to_send_ids

def send_email_error(data):
  '''
  data: {
    'queue_to_send_id': ???(str),
    'reason': ???(str)
    'datetime': ????(str)
  }
  '''
  if data:
    msg = MIMEMultipart() 
    msg['From'] = cfgs.email_sender_error 
    msg['To'] = ", ".join(cfgs.email_receiver_error) 
    msg['Cc'] = ", ".join(cfgs.email_cc_error)
      
    # string to store the body of the mail 
    body = '''
    Code sender got error for queue id '{0}'\n
    With reason: {1}\n
    error datetime: {2}
    '''.format(data['queue_to_send_id'], data['reason'], data['datetime'])
      
    # attach the body with the msg instance 
    msg.attach(MIMEText(body, 'plain')) 
    
    # subject and attachment file 
    msg['Subject'] = f"Code reward sender error report for {data['datetime']}"
      
    # creates SMTP session 
    s = smtplib.SMTP(cfgs.email_server_error, cfgs.email_server_port_error) 
      
    # start TLS for security 
    s.ehlo()
    s.starttls() 
      
    # Converts the Multipart msg into a string 
    text = msg.as_string() 
      
    # sending the mail 
    s.sendmail(cfgs.email_sender, cfgs.email_receiver_error, text) 
      
    # terminating the session 
    s.quit() 
  return
