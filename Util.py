#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Apr  2 09:33:50 2019

@author: lilnfang
"""

import xml.etree.ElementTree as ET
import pandas as pd
import sys
from sqlalchemy import create_engine
from enum import IntEnum
from pathlib import Path
import requests
import json
import phonenumbers
import numpy as np
import datetime
from pymysql.converters import escape_string


class StatusType(IntEnum):
    RELEASE = 1 # 直接放款
    REVIEW = 2 # 转审核
    REJECT = 3 # 拒绝

class CustomerType(IntEnum):
    IOS_NEW = 10 # ios新客
    IOS_OLD = 20 # ios复贷
    

class util_control(object):
    
    def __init__(self, logger, path, new_key_list, old_key_list):
        self.engine = None
        self.logger = logger
        self.config = {}
        self._read_config(path)
        if len(self.config) == 0:
            self.logger.error("read config error!")
            sys.exit()
        self.new_key_list = new_key_list
        self.old_key_list = old_key_list
        self.temp_path = Path('temp')
        self.temp_path.mkdir(parents=True, exist_ok=True)
        #with open(category, 'r') as f:
        #    self.categorical_dict = json.load(f)
        self._create_db()
        self.base = self.config['base']
        self.pdo = self.config['pdo']
        self.zone_2016 = (817,926,936,994,997,977,937,905,945,995,975,978,956,976,979,813,913,950,900,914,911,998,940,924,931,942,944,925,934,912,915,916,917,918,973,974,948,949,997,996,943,946,
                          905,906,907,908,909,910,919,920,921,922,923,926,947,927,928,929,930,932,933,935,936,937,938,939,999,989)
        self.area_code_list = ['32', '33', '34', '35', '36', '38',
                          '42', '43', '44', '45', '46', '47', '48', '49',
                          '52', '53', '54', '55', '56',
                          '62', '63', '64', '65', '68',
                          '72', '74', '75', '77', '78',
                          '82', '83', '84', '85', '86', '87', '88'
                          ]
        self.service_phone = ['1473713',  '133', '143', '117', '1900'
                         '222', '211', '2629', '223',
                         '6387000',
                         '87000', '802', '801', '808',
                         '9081300'
                         ]
        self.name_list = ['mama', 'papa', 'nanay', 'tatay', 'mommy', 'daddy', 'ate', 'mother', 'kuya', 'asawa ko', 'bunso', 'mama globe']
        self.app_list =  ['pesoLending', 'pesoq', 'juanhand', 'cashme', 'madali', 'start loan', 'happypera', 'fastcash', 'okPeso', 
                          'supercash', 'malicash', 'happycredit', 'mmloan', 'bootcash', 'cashoyo', 'suncash', 'cashbee', 'cashalo', 'pesopera', 'pesohaus', 'cashmore']
        self.app_type_dict = {}
        self.app_type_dict['tool_list'] = ['business', 'events', 'tools', 'productivity', 'weather', 'parenting', 'house_and_home']
        self.app_type_dict['education_list'] = ['art_and_design', 'books_and_reference', 'comics', 'education', 'news_and_magazines', 'libraries_and_demo']
        self.app_type_dict['entertainment_list'] = ['entertainment', 'game_action', 'game_adventure', 'game_arcade', 'game_board', 'game_card',
                                                  'game_casino', 'game_casual', 'game_educational', 'game_music', 'game_puzzle', 'game_racing',
                                                  'game_role_playing', 'game_simulation', 'game_sports', 'game_strategy', 'game_trivia',
                                                  'game_word']
        self.app_type_dict['photography_list'] = ['beauty', 'photography']
        self.app_type_dict['lifestyle_list'] = ['food_and_drink', 'medical', 'shopping', 'lifestyle']
        self.app_type_dict['musicaudio_list'] = ['video_players', 'music_and_audio']
        self.app_type_dict['finance_list'] = ['finance']
        self.app_type_dict['health_list'] = ['health_and_fitness', 'sports']
        self.app_type_dict['social_list'] = ['communication', 'dating', 'social']
        self.app_type_dict['travel_list'] = ['maps_and_navigation', 'travel_and_local', 'auto_and_vehicles']
        self.app_type_dict['personalization_list'] = ['personalization']
        self.app_type_dict['name_tool_list'] = ['screen', 'lock', 'theme', 'samsung max', 'clean master', 'wireless', 'time zone', 'dlna', 'pldt',
                                              'mail', 'gateway', 'proxy', 'hd', 'cleaner', 'security', 'compass', 'kid', 'parent', 'my phone']
        self.app_type_dict['name_education_list'] = ['reader']
        self.app_type_dict['name_entertainment_list'] = ['poker', 'game', '2048', 'puzzle', 'iwant', 'my cat', 'golden ball', 'netflix',
                                                       'snaptube', 'casino', 'asphalt nitro', 'spider-man']
        self.app_type_dict['name_photography_list'] = ['photo', 'camera', 'soloop', 'gallery']
        self.app_type_dict['name_lifestyle_list'] = ['shop']
        self.app_type_dict['name_musicaudio_list'] = ['piano', 'music', 'nba', 'cinema', 'youtube', 'video']
        self.app_type_dict['name_finance_list'] = ['loan', 'peso', 'money', 'lending', 'cash', 'credit', 'wallet', 'dollar', 'sss mobile app',
                                                 'ecebuana', 'payment']
        self.app_type_dict['name_social_list'] = ['mate', 'chat', 'hooq', 'totok', 'instagram', 'vplus', 'sms']
        self.app_type_dict['name_travel_list'] = ['travel']
        self.app_type_dict['name_personalization_list'] = ['touchpal', 'keyboard', 'font']
        self.system_memory_dict = {'12':7,'13':7,'14':7,'15':9,'16':9,'17':10}
        
        
    '''
    def __del__(self):
        self.engine.dispose()
    '''
    def _read_config(self, path):
        try:
            tree = ET.parse(path)
            root = tree.getroot()
            self.config['user'] = root.find('user').text
            self.config['passwd'] = root.find('passwd').text
            self.config['host'] = root.find('host').text
            self.config['port'] = root.find('port').text
            self.config['name'] = root.find('name').text
            self.config['url'] = root.find('advance_url').text
            self.config['key'] = root.find('advance_key').text
            self.config['base'] = int(root.find('base').text)
            self.config['pdo'] = int(root.find('pdo').text)
            self.config['model'] = root.find('model').text
            self.config['ios_model'] = root.find('ios_model').text
            self.config['old_model'] = root.find('old_model').text
        except Exception as e:
            self.logger.error(e)
            self.config = {}
    
    def _create_db(self):
        self.engine = create_engine(
        "mysql+pymysql://%s:%s@%s:%s/%s"
        % (self.config["user"], self.config["passwd"], self.config["host"], self.config["port"], self.config["name"]),
        pool_recycle=3600,pool_size=50,pool_pre_ping=False
        )
    
    def _handle_sql(self, sql):
        res = pd.read_sql(sql, self.engine)
        return res
    
    def _write_sql(self, df, name, uid):
        try:
            df.to_sql(name, self.engine, if_exists='append', index=False)
        except Exception as e:
            self.logger.error(e)
            df.to_csv(self.temp_path / (name+'_'+str(uid)+'.csv'), index=False)
    
    def _execute_sql(self, sql):
        try:
            db_con = self.engine.connect()
            db_con.execute(sql)
            db_con.close()
        except Exception as e:
            self.logger.error(e)
    
    def check_params(self, keys, customer_type):
        key_list = self.new_key_list if customer_type == CustomerType.IOS_NEW else self.old_key_list
        for key in key_list:
            if key not in keys:
                self.logger.error(f'{key} not in params!')
                return False
        return True
    
    def get_sys_black_list(self, type_, market_id, loan_type, content):
        if isinstance(content, list):
            content_str = str(tuple(content))
            if len(content) == 1:
                content_str = content_str[0:-2]
                content_str += ')'
            sql = f"select count(distinct content) as black_count from sys_black_list where type = {type_} and market_id = {market_id} and loan_type = {loan_type} and content in {content_str}"
        else:
            content = escape_string(content)
            sql = f"select count(distinct content) as black_count from sys_black_list where type = {type_} and market_id = {market_id} and loan_type = {loan_type} and content = '{content}'"
        data = self._handle_sql(sql)
        return data
    
    def get_sys_white_list(self, customer_mobile):
        sql = f"select count(*) as white_count from sys_white_phones where phone = '{customer_mobile}'"
        data = self._handle_sql(sql)
        return data
    
    def get_threshold(self, customer_type, is_white):
        tb_name = 'threshold_white' if is_white else 'threshold'
        sql = f"select rule_name,thresh_value,status from {tb_name} where customer_type = {customer_type}"
        data = self._handle_sql(sql)
        return data
    
    def get_gps_borrow(self, longitude, latitude, date, market_id, borrow_id):
        date = date.strftime('%Y-%m-%d %H:%M:%S')
        sql = f"select customer_id,create_time from borrow where create_time > '{date}' and market_id = {market_id} and id != {borrow_id} \
                and cast(longitude as DECIMAL(9,4)) = {longitude} and cast(latitude as DECIMAL(9,4)) = {latitude}"
        data = self._handle_sql(sql)
        return data
    
    def get_emergency_customer(self, customer_mobile, emergency_list, market_id):
        emergency_str = str(tuple(emergency_list))
        if len(emergency_list) == 1:
            emergency_str = emergency_str[0:-2]
            emergency_str += ')'
        sql = f"select count(*) as customer_count from customer where market_id = {market_id} and customer_full_mobile != '{customer_mobile}' and \
            (contact1_mobile in {emergency_str} or contact2_mobile in {emergency_str} or contact3_mobile in {emergency_str} or contact4_mobile in {emergency_str} or contact5_mobile in {emergency_str})"
        data = self._handle_sql(sql)
        return data
    
    def get_emergency_overdue(self, emergency_list, market_id):
        emergency_str = str(tuple(emergency_list))
        if len(emergency_list) == 1:
            emergency_str = emergency_str[0:-2]
            emergency_str += ')'
        sql = f"select a.customer_id,a.over_due_days from borrow_period a,customer b where a.period_status = 70 and a.customer_id = b.id and b.customer_full_mobile in {emergency_str} and b.market_id = {market_id}"
        data = self._handle_sql(sql)
        return data
    
    def get_customer_overdue_10d(self, customer_mobile, market_id):
        sql = f"select count(*) as overdue_count from borrow_period a,customer b where a.period_status = 70 and a.over_due_days > 10 and a.customer_id = b.id and (b.contact1_mobile = '{customer_mobile}' or b.contact2_mobile = '{customer_mobile}' or b.contact3_mobile = '{customer_mobile}' or b.contact4_mobile = '{customer_mobile}' or b.contact5_mobile = '{customer_mobile}') and b.market_id = {market_id}"
        data = self._handle_sql(sql)
        return data
    
    def get_id_borrow(self, id_card_no, borrow_id):
        sql = f"select a.customer_id,a.borrow_status,a.market_id from borrow a,customer b where a.id != {borrow_id} and a.borrow_status in (10,20,30,40,50,60,70) and a.customer_id = b.id and b.id_card_no = '{id_card_no}'"
        data = self._handle_sql(sql)
        return data
    
    def get_mobile_sim_borrow(self, customer_mobile, sim_list, borrow_id):
        if customer_mobile not in sim_list:
            sim_list.append(customer_mobile)
        sim_str = str(tuple(sim_list))
        if len(sim_list) == 1:
            sim_str = sim_str[0:-2]
            sim_str += ')'
        sql = f"select a.customer_id,a.borrow_status,a.market_id from borrow a,customer b where a.id != {borrow_id} and a.borrow_status in (10,20,30,40,50,60,70) and a.customer_id = b.id and b.customer_full_mobile in {sim_str}"
        data = self._handle_sql(sql)
        return data
    
    def get_device_borrow(self, device_sno, borrow_id):
        sql = f"select a.customer_id,a.borrow_status,a.market_id from borrow a,device b where a.id != {borrow_id} and a.id = b.borrow_id and b.device_sno = '{device_sno}'"
        data = self._handle_sql(sql)
        return data
    
    def get_bank_borrow(self, bank_account, borrow_id):
        sql = f"select a.customer_id,a.borrow_status,a.market_id from borrow a,customer_bank_card b where a.id != {borrow_id} and a.borrow_status in (10,20,30,40,50,60,70) and a.customer_id = b.customer_id and b.bank_account = '{bank_account}'"
        data = self._handle_sql(sql)
        return data
    
    def get_name_birth_borrow(self, customer_name, customer_birthday, borrow_id):
        sql = f"select a.customer_id,a.borrow_status,a.market_id from borrow a,customer b where a.id != {borrow_id} and a.borrow_status in (10,20,30,40,50,60,70) and a.customer_id = b.id and b.customer_name = '{customer_name}' and b.customer_birthday = '{customer_birthday}'"
        data = self._handle_sql(sql)
        return data
    
    def get_other_device_borrow(self, device_sno, customer_mobile, borrow_id):
        sql = f"select a.customer_id,a.borrow_status,a.market_id from borrow a,device b where a.id != {borrow_id} and a.borrow_status in (10,20,30,40,50,60,70) and a.id = b.borrow_id and b.device_sno != '{device_sno}' and b.customer_mobile = '{customer_mobile}'"
        data = self._handle_sql(sql)
        return data
    
    def get_contact_borrow(self, contact_list, market_id):
        contact_str = str(tuple(contact_list))
        if len(contact_list) == 1:
            contact_str = contact_str[0:-2]
            contact_str += ')'
        sql = f"select b.customer_mobile,a.borrow_status from borrow a,customer b where a.market_id = {market_id} and a.customer_id = b.id and b.customer_full_mobile in {contact_str}"
        data = self._handle_sql(sql)
        return data
    
    def get_device_mobile_count(self, device_sno, market_id):
        sql = f"select count(distinct customer_mobile) as mobile_count from device a,borrow b where a.device_sno = '{device_sno}' and b.market_id = {market_id} and a.borrow_id = b.id"
        data = self._handle_sql(sql)
        return data
    
    def get_customer_device(self, customer_id, borrow_id):
        sql = f"select device_sno from device where borrow_id != {borrow_id} and borrow_id != -1 and customer_id = {customer_id} order by create_time desc"
        data = self._handle_sql(sql)
        return data
    
    def get_id_borrow_count(self, id_card_no, market_id, borrow_id):
        sql = f"select count(distinct a.customer_id) as customer_count from borrow a,customer b where a.id != {borrow_id} and a.market_id = {market_id} and a.borrow_status in (10,20,30,40,50,60,70) and a.customer_id = b.id and b.id_card_no = '{id_card_no}'"
        data = self._handle_sql(sql)
        return data
    
    def get_mobile_sim_borrow_count(self, customer_mobile, sim_list, market_id, borrow_id):
        if customer_mobile not in sim_list:
            sim_list.append(customer_mobile)
        sim_str = str(tuple(sim_list))
        if len(sim_list) == 1:
            sim_str = sim_str[0:-2]
            sim_str += ')'
        sql = f"select count(distinct a.customer_id) as customer_count from borrow a,customer b where a.id != {borrow_id} and a.market_id = {market_id} and a.borrow_status in (10,20,30,40,50,60,70) and a.customer_id = b.id and b.customer_full_mobile in {sim_str}"
        data = self._handle_sql(sql)
        return data
    
    def get_device_borrow_count(self, device_sno, market_id, borrow_id):
        sql = f"select count(distinct a.customer_id) as customer_count from borrow a,device b where a.id != {borrow_id} and a.market_id = {market_id} and a.borrow_status in (10,20,30,40,50,60,70) and a.id = b.borrow_id and b.device_sno = '{device_sno}'"
        data = self._handle_sql(sql)
        return data
    
    def get_bank_borrow_count(self, bank_account, market_id, borrow_id):
        sql = f"select count(distinct a.customer_id) as customer_count from borrow a,customer_bank_card b where a.id != {borrow_id} and a.market_id = {market_id} and a.borrow_status in (10,20,30,40,50,60,70) and a.customer_id = b.customer_id and b.bank_account = '{bank_account}'"
        data = self._handle_sql(sql)
        return data
    
    def get_name_birth_borrow_count(self, customer_name, customer_birthday, market_id, borrow_id):
        sql = f"select count(distinct a.customer_id) as customer_count from borrow a,customer b where a.id != {borrow_id} and a.market_id = {market_id} and a.borrow_status in (10,20,30,40,50,60,70) and a.customer_id = b.id and b.customer_name = '{customer_name}' and b.customer_birthday = '{customer_birthday}'"
        data = self._handle_sql(sql)
        return data
    
    def get_other_device_borrow_count(self, device_sno, customer_mobile, market_id, borrow_id):
        sql = f"select count(distinct a.customer_id) as customer_count from borrow a,device b where a.id != {borrow_id} and a.market_id = {market_id} and a.borrow_status in (10,20,30,40,50,60,70) and a.id = b.borrow_id and b.device_sno != '{device_sno}' and b.customer_mobile = '{customer_mobile}'"
        data = self._handle_sql(sql)
        return data
    
    def set_risk_control_result(self, result_dict):
        data = pd.DataFrame([result_dict])
        borrow_id = result_dict['borrow_id']
        self._write_sql(data,'risk_control_result',borrow_id)
    
    def set_feature_result(self, feature):
        data = pd.DataFrame([feature])
        customer_id = feature['customer_id']
        self._write_sql(data,'feature',customer_id)
    
    def set_advance_common_phone(self, result_dict):
        temp = result_dict['data']
        del result_dict['data']
        del result_dict['extra']
        if isinstance(temp, dict):
            result_dict['status'] = temp['status']
            result_dict['description'] = temp['message']
            result_dict['last_seen'] = temp['lastSeen']
            result_dict['photo'] = temp['photo']
        data = pd.DataFrame([result_dict])
        borrow_id = result_dict['borrow_id']
        self._write_sql(data,'advance_common_phone_detection',borrow_id)
    
    # advance common phone detection
    def advance_request(self, url, key, phone):
        if not phone.startswith('+52'):
            phone = '+52' + phone
        data = {'phone':phone}
        headers = {'Content-Type':'application/json','X-ADVAI-KEY':key}
        result = None
        try:
            res =  requests.post(url, headers=headers, data=json.dumps(data), timeout=60)
            if res.status_code == 200:
                result = json.loads(res.text)
            else:
                self.logger.error(f'advance request error! code: {res.status_code}, content: {res.text}')
        except Exception as e:
            self.logger.error(e)
        return result, phone
    
    # 座机号码
    def fixed_line(self, x):
        fixed_line_flag = 0
        sub_str = x[0:2]
        if x.startswith('02'):
            if len(x) == 7 or len(x) == 8 or len(x) == 9:
                fixed_line_flag = 1
        elif x.startswith('2'):
            if len(x) == 6 or len(x) == 7 or len(x) == 8:
                fixed_line_flag = 1
        elif sub_str in self.area_code_list:
            if len(x) == 7 or len(x) == 8 or len(x) == 9:
                fixed_line_flag = 1
        return fixed_line_flag
    
    # 运营商判断
    def telephone_type(self, x):
        phone_type = 'unknown'
        if x.startswith('6390'):
            if x[4] in ('5', '6'):
                phone_type = 'GLOBE'
            elif x[4] in ('7', '8', '9'):
                phone_type = 'SMART'
        elif x.startswith('6391'):
            if x[4] in ('0', '2', '8', '9'):
                phone_type = 'SMART'
            elif x[4] in ('5', '6', '7'):
                phone_type = 'SMART'
        elif x.startswith('6392'):
            if x[4] in ('0', '1', '8', '9'):
                phone_type = 'SMART'
            elif x[4] in ('2', '3', '5'):
                phone_type = 'SUN CELLULAR'
            elif x[4] in ('6', '7'):
                phone_type = 'GLOBE'
        elif x.startswith('6393'):
            if x[4] in ('0', '8', '9'):
                phone_type = 'SMART'
            elif x[4] in ('1', '2', '3'):
                phone_type = 'SUN CELLULAR'
            elif x[4] in ('5', '6', '7'):
                phone_type = 'GLOBE'
        elif x.startswith('6394'):
            if x[4] in ('2', '3'):
                phone_type = 'SUN CELLULAR'
            elif x[4] in ('5'):
                phone_type = 'GLOBE'
            elif x[4] in ('6', '7', '8', '9'):
                phone_type = 'SMART'
        elif x.startswith('6395'):
            if x[4] in ('0', '1'):
                phone_type = 'SMART'
            elif x[4] in ('3', '4', '5', '6', '7'):
                phone_type = 'GLOBE'
        elif x.startswith('6396'):
            if x[4] in ('1', '3', '4'):
                phone_type = 'SMART'
            elif x[4] in ('5', '6', '7'):
                phone_type = 'GLOBE'
        elif x.startswith('6397'):
            if x[4] in ('3'):
                phone_type = 'EXPRESS'
            elif x[4] in ('5', '6', '7'):
                phone_type = 'GLOBE'
            elif x[4] in ('8', '9'):
                phone_type = 'NEXT'
        elif x.startswith('6399'):
            if x[4] in ('5', '6', '7'):
                phone_type = 'GLOBE'
            elif x[4] in ('8', '9'):
                phone_type = 'SMART'
        return phone_type
    
    # 处理通讯录号码格式
    def deal_contact_num(self, contact_df, feature={}):
        count = len(contact_df)
        distinct_count = len(contact_df['phone'].unique())
        result_contact_list = []
        imobile_contact_amount = 0
        fixed_line_num = 0
        service_num = 0
        familyname_num = 0
        appname_num = 0
        zone_num_dict = {}
        phone_2016_list = []
        tel_dict = {'GLOBE':0,'EXPRESS':0,'SMART':0,'SUN CELLULAR':0,'unknown':0}
        for index,row in contact_df.iterrows():
            if row['contact_name'] in self.name_list:
                familyname_num += 1
            elif row['contact_name'] in self.app_list:
                appname_num += 1
            phone = row['phone']
            #phone = phone.replace(' ','')
            #phone = phone.replace('+','')
            try:
                parsed_phone = phonenumbers.parse(phone, 'PH')
                if phonenumbers.is_valid_number_for_region(parsed_phone, 'PH'):
                    #if len(phone) == 10 and not phone.startswith('63'):
                    #    phone = '63' + phone
                    result_contact_list.append(phone)
                    if len(phone) < 10 and self.fixed_line(phone):
                        fixed_line_num += 1
                    if phone in self.service_phone:
                        service_num += 1
                    if len(phone) == 12:
                        zone_num = phone[2:5]
                        if zone_num not in zone_num_dict.keys():
                            zone_num_dict[zone_num] = []
                        zone_num_dict[zone_num].append(phone)
                        if phone.startswith('639'):
                            imobile_contact_amount += 1
                            tel_dict[self.telephone_type(phone)] += 1
                        if int(zone_num) in self.zone_2016:
                            phone_2016_list.append(phone)
            except Exception:
                pass
        feature['distinct_contact_amount'] = distinct_count
        feature['invalid_contact_amount'] = count - len(result_contact_list)
        feature['imobile_contact_amount'] = imobile_contact_amount
        feature['contact_area_code_cnt'] = len(zone_num_dict)
        feature['contact_area_code_2016_percent'] = len(set(phone_2016_list)) / distinct_count
        feature['contact_area_code_phone_avg'] = len(set(result_contact_list)) / len(zone_num_dict) if len(zone_num_dict) > 0 else np.nan
        feature['fixed_line_num'] = fixed_line_num
        feature['service_num'] = service_num
        feature['invalid_number_num2'] = count - imobile_contact_amount - fixed_line_num - service_num
        feature['telsmart_num'] = tel_dict['SMART']
        feature['telglobe_num'] = tel_dict['GLOBE']
        feature['telsun_num'] = tel_dict['SUN CELLULAR']
        feature['telexpress_num'] = tel_dict['EXPRESS']
        feature['telunknown_num'] = tel_dict['unknown']
        feature['telmobile_num_rate'] = imobile_contact_amount / count
        feature['telsmart_num_rate'] = tel_dict['SMART'] / count
        feature['telglobe_num_rate'] = tel_dict['GLOBE'] / count
        feature['telsun_num_rate'] = tel_dict['SUN CELLULAR'] / count
        feature['telexpress_num_rate'] = tel_dict['EXPRESS'] / count
        feature['telunknown_num_rate'] = tel_dict['unknown'] / count
        feature['familyname_num'] = familyname_num
        feature['appname_num'] = appname_num
        feature['familyname_num_rate'] = familyname_num / count
        feature['appname_num_rate'] = appname_num / count
    
    def get_valid_phone(self, contact_list):
        valid_phone_count = 0
        for phone in contact_list:
            try:
                parsed_phone = phonenumbers.parse(phone, 'PH')
                if phonenumbers.is_valid_number_for_region(parsed_phone, 'PH'):
                    valid_phone_count += 1
            except Exception:
                pass
        valid_phone_ratio = valid_phone_count / len(contact_list) if len(contact_list) > 0 else np.nan
        return valid_phone_count, valid_phone_ratio
    
    def get_model_feature(self, feature_name, feature):
        if len(feature) > 0:
            if feature.get('memoryCardSizeUse') == None:
                feature['memoryCardSizeUse'] = np.nan
            for fname in feature_name:
                if feature.get(fname) == None:
                    feature[fname] = np.nan
        feature_df = pd.DataFrame([feature])
        if not feature_df.empty:
            # feature_df.fillna(-1, inplace=True)
            feature_df['ramTotalSize'] = feature_df['ramTotalSize'] / (1024*1024*1024)
            feature_df['memoryCardSize'] = feature_df['memoryCardSize'] / (1024*1024*1024)
            feature_df['memoryCardSizeUse'] = feature_df['memoryCardSizeUse'] / (1024*1024*1024)
            feature_df['rom_used_space'] = feature_df['memoryCardSize'] - feature_df['memoryCardSizeUse']
            feature_df = feature_df[feature_name]
        return feature_df
    
    def get_emergency_list(self, customer_id, market_id, emergency_list, start_date):
        emergency_str = str(tuple(emergency_list))
        if len(emergency_list) == 1:
            emergency_str = emergency_str[0:-2]
            emergency_str += ')'
        sql = f"select a.loan_type,a.create_time,b.contact1_mobile,b.contact2_mobile,b.contact3_mobile,b.contact4_mobile,b.contact5_mobile from borrow a,customer_urgency_contact_borrow b where a.id = b.borrow_id and a.market_id = {market_id} and a.customer_id != {customer_id} and \
            a.create_time > '{start_date}' and (b.contact1_mobile in {emergency_str} or b.contact2_mobile in {emergency_str} or b.contact3_mobile in {emergency_str} or b.contact4_mobile in {emergency_str} or b.contact5_mobile in {emergency_str})"
        emergency_df = self._handle_sql(sql)
        return emergency_df
    
    def get_last_borrow_emergency(self, customer_id):
        sql = f"select b.contact1_mobile,b.contact2_mobile,b.contact3_mobile,b.contact4_mobile,b.contact5_mobile from borrow a,customer_urgency_contact_borrow b where a.id = b.borrow_id and a.customer_id = {customer_id} and a.borrow_status in (1000,1010) order by a.create_time desc"
        emergency_df = self._handle_sql(sql)
        return emergency_df
            
    def model_score(self, model, feature):
        p = model.predict(feature)[0]
        log_odds = np.log(p / (1-p))
        score = self.base + self.pdo * (-log_odds) / np.log(2)
        return round(score)
    
    def get_contact_list(self, device_id, divider):
        table_index = np.floor(device_id / divider) + 1
        table = f'device_contact_{int(table_index)}'
        sql = f"select phone,contact_name,in_time from {table} where device_id = {device_id}"
        concat_df = self._handle_sql(sql)
        return concat_df
    
    def get_app_list(self, device_id, divider):
        table_index = np.floor(device_id / divider) + 1
        table = f'device_app_{int(table_index)}'
        sql = f"select app_name,package_name,in_time from {table} where device_id = {device_id}"
        app_df = self._handle_sql(sql)
        return app_df
    
    def get_install_app(self):
        sql = "select loan,app_name,overdue_diff from customer_install_app_info_overdue"
        install_app_df = self._handle_sql(sql)
        if not install_app_df.empty:
            install_app_df['loan'] = install_app_df['loan'].apply(lambda x:int.from_bytes(x, byteorder='big'))
        return install_app_df
    
    def get_google_play(self, app_id_list):
        app_id_str = str(tuple(app_id_list))
        if len(app_id_list) == 1:
            app_id_str = app_id_str[0:-2]
            app_id_str += ')'
        sql = f"select genre_id,google_app_id_hash from google_play_simple_info where google_app_id_hash in {app_id_str}"
        google_play_df = self._handle_sql(sql)
        if not google_play_df.empty:
            google_play_df['genre_id'] = google_play_df['genre_id'].str.lower()
        return google_play_df.rename(columns={'google_app_id_hash':'app_id'})
    
    def get_app_type_feature(self, app_df, key):
        key_list = self.app_type_dict[key]
        temp = app_df[app_df['genre_id'].isin(key_list)]
        app_num = len(temp)
        name_key_list = self.app_type_dict.get('name_'+key)
        if name_key_list is not None:
            temp = app_df[app_df['app_name'].isin(key_list)]
            app_num += len(temp)
        return app_num
    
    def get_device_raw(self, device_id):
        sql = f"select content from device_raw where device_id = {device_id}"
        content_df = self._handle_sql(sql)
        return content_df
    
    def get_basic_info(self, customer_id):
        sql = f"select monthly_income,customer_sex,customer_birthday,marital_status,id_type,edu_status,profession_type,child_count,create_time from customer where id = {customer_id}"
        df = self._handle_sql(sql)
        return df
    
    def get_borrow_feature(self, customer_id):
        sql = f"select a.id,a.principal_amount,a.interest_amount,b.over_due_days,a.borrow_status,a.create_time from \
                (select id,principal_amount,interest_amount,borrow_status,create_time from borrow where customer_id = {customer_id}) a \
                left join \
                (select borrow_id,over_due_days from borrow_period) b \
                ON a.id = b.borrow_id \
                order by create_time asc"
        df = self._handle_sql(sql)
        return df
    
    def get_last_overdue(self, customer_id):
        sql = f"select over_due_days from borrow_period where customer_id = {customer_id} and period_status in (1000,1010) order by create_time desc"
        df = self._handle_sql(sql)
        return df
    
    def get_monthly_income(self, x):
        income = 0
        if x == 1:
            income = 5000
        elif x == 2:
            income = 12500
        elif x == 3:
            income = 17500
        elif x == 4:
            income = 22500
        elif x == 5:
            income = 27500
        elif x == 6:
            income = 32500
        elif x == 7:
            income = 37500
        return income
    
    def get_operate_track(self, customer_id, record_time):
        month_start = datetime.datetime(record_time.year, record_time.month, 1)
        last_month = month_start - datetime.timedelta(days=1)
        table = f"operate_track_{record_time.strftime('%Y_%m')}"
        sql = f"select session_id,operation_time,page_code,action_type,duration_seconds from {table} where customer_id = {customer_id}"
        try:
            df1 = self._handle_sql(sql)
        except:
            df1 = pd.DataFrame()
        table = f"operate_track_{last_month.strftime('%Y_%m')}"
        sql = f"select session_id,operation_time,page_code,action_type,duration_seconds from {table} where customer_id = {customer_id}"
        df2 = self._handle_sql(sql)
        df = pd.concat([df1, df2])
        return df
    
    def get_system_size(self, system_version):
        system_size = None
        temp = system_version.split('.')
        version = temp[0]
        if version.isdigit():
            if self.system_memory_dict.get(version) is not None:
                system_size = self.system_memory_dict.get(version)
            else:
                if int(version) < 12:
                    system_size = 7
                elif int(version) > 17:
                    system_size = 10
        return system_size
        
        
