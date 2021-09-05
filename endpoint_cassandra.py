# Library
from flask import Flask, request
from urllib.parse import unquote, urlparse
import json
import re
import time
from flask_sslify import SSLify #SSL Flask
import ssl #SSL Flask
import cassandra_function

Backend_API = Flask(__name__)

# SSL flask
context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
context.load_cert_chain('certinet/domain.crt', 'certinet/domain.key')
sslify = SSLify(Backend_API)

# Return to client with json format and setup status code ether
def response_json(data, code):
    response_data = Backend_API.response_class(
        response = json.dumps(data),
        status = code,
        mimetype = 'application/json'
    )
    return response_data

@Backend_API.route('/api/v1/get_casdb_cancel_mail', methods=['GET'])
def get_all_casdb_cancel_mail():

    query_str = "SELECT json * FROM mail_cancel_sent"
    try:
        data_fetch = list(cassandra_function.fetch_all_data_all_row_from_log(query_str))
        if data_fetch != "can fetch DB":
            # print("all_row_data : ", row)
            return response_json(data_fetch, 200)
        else:
            return response_json("Bad request", 400)

    except Exception as e:
        print('ERROR >> ',e)
        return response_json("Bad request", 400)

@Backend_API.route('/api/v1/update_casdb_cancel_mail', methods=['POST'])
def update_cancel_mail():
    try:
        raw_data = request.get_data()
        data_decode = raw_data.decode("utf-8")
        data_decode = unquote(data_decode) # Full error message
        data_json = json.loads(data_decode)

        print("data_decode : ", data_json)

        vm_name = data_json["vm_name"]
        cancelsend_stat = data_json["cancelsend_stat"]

        query_str = "UPDATE dev_moni.mail_cancel_send SET cancelsend_stat = "+str(cancelsend_stat)+" isavtive = '0' "+"WHERE vm_name = '"+vm_name+"';"
        result_update = cassandra_function.update_cassandra_db(query_str)
        
        if result_update == "can update DB":
            return response_json("OK", 200)
        else:
            return response_json("Bad request", 400)

    except Exception as e:
        print('ERROR >> ',e)
        return response_json("Bad request", 400)

@Backend_API.route('/api/v1/get_casdb_all', methods=['GET'])
def get_all_data_from_log():
    # get data from cassandra in json type
    query_str = "SELECT json * FROM event_received_log"
    try:
        row = list(cassandra_function.fetch_all_data_all_row_from_log(query_str))
        # print("all_row_data : ", row)
        return response_json(row, 200)
    except Exception as e:
        print('ERROR >> ',e)
        return response_json("Bad request", 400)

def main():
    Backend_API.run(debug=True, threaded=True, host='0.0.0.0', port='5001', ssl_context = context)

if __name__ == '__main__':
    main()