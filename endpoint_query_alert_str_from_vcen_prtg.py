from flask import Flask, request
from urllib.parse import unquote, urlparse
import json
import re
import datetime
from flask_sslify import SSLify #SSL Flask
import ssl #SSL Flask
import requests

import cassandra_function

Backend_API = Flask(__name__)

# SSL flask
context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
context.load_cert_chain('certinet/domain.crt', 'certinet/domain.key')
# context = ('SendMailSOConfirm/inet.crt', 'SendMailSOConfirm/inet.key')
sslify = SSLify(Backend_API)

# Return to client with json format and setup status code ether
def response_json(data, code):
    response_data = Backend_API.response_class(
        response = json.dumps(data),
        status = code,
        mimetype = 'application/json'
    )
    return response_data

@Backend_API.route('/api/v1/vcenter', methods=['POST'])
def vcenter_alert():
    try:
        # get byte encode by utf-8
        data = request.get_data()
        data_decode = data.decode("utf-8")
        data_decode = unquote(data_decode)

        # protect python message error
        data_raw = data_decode.replace("%","#")
        data_raw = data_decode.replace("\\","/")
        _data_received = json.loads(data_raw)
        
        # PRTG input alert pattern
        # {"vmname" : "NTNX-BQF902600008-A-CVM", "alert_message" : "Metric CPU Usage = 77%"}
        print("_data_received :", _data_received)

        # basic filter cut out
        if (0):
            response_json('finish - not condition', 204)

        # extract word to new json
        _data_extracted = {}
        _data_extracted['timestamp'] = time.time() #python time

        try:
            _data_extracted['vmname'] = _data_received['vmname']
        except:
            _data_extracted['vmname'] = ''
        try:
            _data_extracted['sensortype'] = _data_received["alert_message"].split(" ")[1]
        except:
            _data_extracted['sensortype'] = ''
        try:    
            _data_extracted['percent'] = re.findall('\d+',_data_received["alert_message"])[0]
        except:
            _data_extracted['percent'] = ''

        print(_data_extracted)

    except Exception as e:
        print('ERROR >> ',e)

    finally:
        return response_json('finish', 200)

@Backend_API.route('/api/v1/receive_prtg_alert', methods=['POST'])
def prtg_alert():
    print("===========================================================")
    def get_prtg_alert_and_send_to_sender():
        try:

            # request api to sender module
            def ToSenderAPI(json_data):
                # url endpoint
                url = 'http://xxxxxxxxxxxxxxxx:4444/api/v2/sender'
                # header
                headers = {
                    'Content-Type': 'application/json'
                }
                # send request
                try:
                    requests.request("POST", url, headers=headers, json=json_data, timeout=0.01, verify=False)
                    print("data has sent to sender API")
                except:
                    # error via timeout
                    print("can't send json data to sendder API")
                    pass
            
            # get byte encode by utf-8
            data = request.get_data()
            data_decode = data.decode("utf-8")
            data_decode = unquote(data_decode)

            # protect python message error
            data_raw_replaced = data_decode.replace("%","$")
            data_raw_replaced_2 = data_raw_replaced.replace("\\","/")
            _data_received = json.loads(data_raw_replaced_2)
            
            # PRTG input alert pattern
            # {"device":"_[TEST-bamkungjaja] : IP(172.11.0.1)","name":"Memory","status":"Down","message":"90 #","group":"TEST#123456789","down":"apidown","time":"01:00:04 AM"}
            # print(_data_received)

            # basic filter cut out
            if (0):
                response_json('finish - not condition', 204)

            # extract word to new json
            _data_extracted = {}
            _data_extracted['disk_drive'] = ''
            _data_extracted['timestamp'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S+0000") #python time
            #.strftime("%Y/%m/%Y, %H:%M:%S")
            # '2021-05-27 21:53:44+0000');
            try:
                _data_extracted['vm_name'] = re.search("_\[(.*?)\]", _data_received['device']).group(1)
            except:
                _data_extracted['vm_name'] = ''
            try:
                _data_extracted['ipprivate'] = re.search("IP\((.*?)\)", _data_received['device']).group(1)
            except:
                _data_extracted['ipprivate'] = ''
            try:
                _data_extracted['ippublic'] = re.search("# \((.*?)\)", _data_received['device']).group(1)
            except:
                _data_extracted['ippublic'] = ''

            # add ['sensortype'] in _data_extracted
            try:
                if 'CPU' in _data_received['name'] :
                    _data_extracted['sensortype'] = 'CPU'
                elif 'Memory' in _data_received['name'] :
                    _data_extracted['sensortype'] = 'Memory'
                # Disk bata test
                elif 'Disk' in _data_received['name'] :
                    _data_extracted['sensortype'] = 'Disk'
                    _data_extracted['disk_drive'] = re.search("Disk Free: (.*?) [L(]", _data_received['name']).group(1)
                # re.search(r'(?<=test :)[^.\s]*',text)
                else:
                    _data_extracted['sensortype'] = ''
            except:
                _data_extracted['sensortype'] = ''

            # add ['percent'] in _data_extracted
            try:
                data_value = _data_received['message'].split(' $')[0].strip()
                # print(data_value)
                if data_value.isdigit():
                    if _data_extracted['sensortype'] == 'CPU' :
                        _data_extracted['data_value'] = str(data_value)
                    if _data_extracted['sensortype'] == 'Memory' :
                        _data_extracted['data_value'] = str(100 - int(data_value))
                    elif _data_extracted['sensortype'] == 'Disk' :
                        _data_extracted['data_value'] = str(100 - int(data_value))
                else:
                    if _data_extracted['sensortype'] == 'CPU' :
                        # print(_data_extracted['percent'] + "  " + _data_extracted['vmname'])
                        _data_extracted['data_value'] = str(data_value)
                    if _data_extracted['sensortype'] == 'Memory' :
                        # print(_data_extracted['percent'] + "  " + _data_extracted['vmname'])
                        _data_extracted['data_value'] = str(data_value)
                    elif _data_extracted['sensortype'] == 'Disk' :
                        # print(_data_extracted['percent'] + "  " + _data_extracted['vmname'])
                        _data_extracted['data_value'] = str(data_value)
            except:
                _data_extracted['data_value'] = ''

            # add ['customername'],['cno'],['project']
            try:
                _tempcusname = _data_received['group'].split('#')
                try: 
                    _data_extracted['customername'] = _tempcusname[0].strip()
                except:
                    _data_extracted['customername'] = ''
                try: 
                    _data_extracted['cno'] = _tempcusname[1].strip()
                except:
                    _data_extracted['cno'] = ''
                try: 
                    _data_extracted['project'] = _tempcusname[2].strip()
                except:
                    _data_extracted['project'] = ''
                try: 
                    _data_extracted['projectvm_cus'] = ''
                except:
                    _data_extracted['projectvm_cus'] = ''
            except:
                _data_extracted['customername'] = ''
                _data_extracted['cno'] = ''
                _data_extracted['project'] = ''
                _data_extracted['projectvm_cus'] = ''

        # ================================ add ===============================
            # defalut value
            _data_extracted['moni_isstatussend'] = False

            try:
                _temp_plateform = _data_received['platform_host']
                if _temp_plateform == 'https://xxxxxxxxxxxxxxxx':
                    _data_extracted['platform'] = 'PRTG_NX'
                else:
                    _data_extracted['platform'] = 'UNKNOW'
            except:
                _data_extracted['platform'] = 'UNKNOW'
            try:
                _data_extracted['remark'] = ''
            except:
                _data_extracted['remark'] = ''   

        # # =================== moni =========================
        #     try:
        #         _data_extracted['moni_cancelsend'] = '0'
        #     except:
        #         _data_extracted['moni_cancelsend'] = '0'
        #     try:
        #         _data_extracted['moni_findoneid'] = ''
        #     except:
        #         _data_extracted['moni_findoneid'] = ''
                
        # # =================== moni =========================

        # # =================== mail ========================= 
        #     try:
        #         _data_extracted['mail_cancelsend'] = '0'
        #     except:
        #         _data_extracted['mail_cancelsend'] = '0'
        #     try:
        #         _data_extracted['mail_isvmproject'] = True
        #     except:
        #         _data_extracted['mail_isvmproject'] = False
        #     try:
        #         _data_extracted['mail_to'] = ''
        #     except:
        #         _data_extracted['mail_to'] = ''
        #     try:
        #         _data_extracted['mail_cc'] = ''
        #     except:
        #         _data_extracted['mail_cc'] = ''
        #     try:
        #         _data_extracted['mail_isstatussend'] = True
        #     except:
        #         _data_extracted['mail_isstatussend'] = False         
        # # =================== mail ========================= 

        # # ================================ add ===============================                                                                                                                # +_data_extracted['timestamp']+", "

            uuid_v5 = cassandra_function.gen_uuid_v5()
            
            print("Completely query result : ", _data_extracted)

            try:
                if _data_extracted['cno'] != "" and _data_extracted['vm_name'] != "": 
                    if "CPU" or "Memory" or "Disk" in _data_extracted['sensortype']:
                        query_str_log = "INSERT INTO dev_moni.event_received_log (event_id, event_timestamp, vm_name, ipprivate, ippublic, sensortype, data_value, disk_drive, cno, projectvm_cus, project, monitor_platform) VALUES ("+str(uuid_v5)+", '"+str(_data_extracted['timestamp'])+"', '"+_data_extracted['vm_name']+"', '"+_data_extracted['ipprivate']+"', '"+_data_extracted['ippublic']+"', '"+_data_extracted['sensortype']+"', '"+_data_extracted['data_value']+"', '"+_data_extracted['disk_drive']+"', '"+_data_extracted['cno']+"', '"+_data_extracted['projectvm_cus']+"', '"+_data_extracted['project']+"', '"+_data_extracted['platform']+"');"             
                        cassandra_function.insert_cassandra_db(query_str_log)

                        query_str_log_1day = "INSERT INTO dev_moni.event_received_log_1day (event_id, event_timestamp, vm_name, ipprivate, ippublic, sensortype, data_value, disk_drive, cno, projectvm_cus, project, monitor_platform) VALUES ("+str(uuid_v5)+", '"+str(_data_extracted['timestamp'])+"', '"+_data_extracted['vm_name']+"', '"+_data_extracted['ipprivate']+"', '"+_data_extracted['ippublic']+"', '"+_data_extracted['sensortype']+"', '"+_data_extracted['data_value']+"', '"+_data_extracted['disk_drive']+"', '"+_data_extracted['cno']+"', '"+_data_extracted['projectvm_cus']+"', '"+_data_extracted['project']+"', '"+_data_extracted['platform']+"');"             
                        cassandra_function.insert_cassandra_db(query_str_log_1day)
                        print("Already send data to CassandraDB")

                        # ToSenderAPI(_data_extracted)
                        return _data_extracted
                else:
                    query_str_log_error = "INSERT INTO dev_moni.event_received_log_error (event_id, event_timestamp, vm_name, ipprivate, ippublic, sensortype, data_value, disk_drive, cno, projectvm_cus, project, monitor_platform) VALUES ("+str(uuid_v5)+", '"+str(_data_extracted['timestamp'])+"', '"+_data_extracted['vm_name']+"', '"+_data_extracted['ipprivate']+"', '"+_data_extracted['ippublic']+"', '"+_data_extracted['sensortype']+"', '"+_data_extracted['data_value']+"', '"+_data_extracted['disk_drive']+"', '"+_data_extracted['cno']+"', '"+_data_extracted['projectvm_cus']+"', '"+_data_extracted['project']+"', '"+_data_extracted['platform']+"');"
                    cassandra_function.insert_cassandra_db(query_str_log_error)
                    print("Don't have CNO or vmname")

                    return "don't keep this data"
            except:
                print("Can't filter those key value")
                return "don't keep this data"
        except:
            print("have some problem")
            return "have problem with this section"
            


    query_result = get_prtg_alert_and_send_to_sender()

    try:
        if query_result != "don't keep this data":
            return response_json(query_result, 200)
        elif query_result == "don't keep this data":
            return query_result
        elif query_result == "have problem with this section":
            return response_json('Bad request', 400)
    except Exception as e:
        print("error >> ", e)
        return response_json('Bad request', 400)
    finally:
        print("===========================================================")

def main():
    Backend_API.run(debug=True, threaded=True, host='0.0.0.0', port='3333', ssl_context = context)

if __name__ == '__main__':
    main()
