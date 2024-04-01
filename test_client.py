# -*- coding: utf-8 -*-
"""
Created on Fri Apr 15 17:03:50 2022

@author: admin
"""

import requests
import json

data =     {
     'customer_id': 28,
     'borrow_id': 95,
     'id_card_no': '3367281420',
     'customer_mobile': '639996908979',
     'bank_account': '09996908979',
     'market_id': 10002,
     'device_sno': 'cbc5cc4e42f4e2d8',
     'device_mac':'02:00:00:00:00:00',
     'router_mac': 'abcd',
     'company_name': 'Marygene store',
     'position_type': 10,
     'proxy':'',
     'ip': '192.168.254.101',
     'longitude': 80.94613,
     'latitude': 22.533586,
     'face_sim':0.5,
     'contact_list':['639996908979','639998728036','639292978593','639108900421','639177959549','639953989396','639178234760','639178941519'],
     'emergency':['639672408619','639086837044','639174773882','639073803393','639974054711'],
     'customer_name':'Marygene Valdez Paraguas',
     'customer_birthday':'1976-08-26',
     'sim':[],
     'total_storage': 7936532480,
     'model_name': 'iPhone 6s',
     'device_id': 22,
     'device_platform': 'android'
     }

'''
data = {
     'borrow_id': 886,
     'customer_id': 28,
     'id_card_no': '3367281420',
     'customer_mobile': '639996908979',
     'bank_account': '09996908979',
     'market_id': 10002,
     'device_sno': 'cbc5cc4e42f4e2d8',
     'customer_name':'Marygene Valdez Paraguas',
     'customer_birthday':'1976-08-26',
     'sim':[],
     'device_id': 875,
     'emergency':['639672408619','639086837044','639174773882','639073803393','639974054711'],
     'device_platform': 'ios'
     }
'''

headers = {'Content-Type': 'application/json'}
url = 'http://192.168.7.89:8082/ios_new_customer'
res = requests.post(url, headers=headers, data=json.dumps(data), timeout=60)
print (res.text)





