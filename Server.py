#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Apr  2 09:33:50 2019

@author: lilnfang
"""

from flask import Flask, request
import logging
from Util import util_control,StatusType,CustomerType
import json
import pandas as pd
import datetime
import re
import lightgbm as lgb
import zlib
import numpy as np
import traceback


app = Flask("risk control")
logging.basicConfig(level = logging.INFO, format = '%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')
logger = logging.getLogger()
new_key_list = ['customer_id','borrow_id','id_card_no','customer_mobile','bank_account','market_id','device_sno','device_mac','router_mac','company_name','position_type','proxy',
            'ip','longitude','latitude','face_sim','contact_list','emergency','customer_name','customer_birthday','sim','total_storage','model_name','device_id','device_platform','system_version']
old_key_list = ['borrow_id','customer_id','id_card_no','customer_mobile','bank_account','market_id','device_sno','customer_name','customer_birthday','sim',
                'device_id','emergency']
uc = util_control(logger, 'config.xml', new_key_list, old_key_list)
model = lgb.Booster(model_file='model/'+uc.config['model'])
ios_model = lgb.Booster(model_file='model/'+uc.config['ios_model'])
old_model = lgb.Booster(model_file='model/'+uc.config['old_model'])
logger.info(f"model file: {uc.config['model']} {uc.config['ios_model']} {uc.config['old_model']}")
if not hasattr(app, "extensions"):
    app.extensions = {}
app.extensions["uc"] = uc
app.extensions["model"] = model
app.extensions["ios_model"] = ios_model
app.extensions["old_model"] = old_model
    

@app.after_request
def after_request(response):
    response.headers.add(
        "Access-Control-Allow-Headers", "Content-Type,Authorization,session_id"
    )
    response.headers.add(
        "Access-Control-Allow-Methods", "GET,PUT,POST,DELETE,OPTIONS,HEAD"
    )
    # 这里不能使用add方法，否则会出现 The 'Access-Control-Allow-Origin' header contains multiple values 的问题
    response.headers["Access-Control-Allow-Origin"] = "*"
    return response


'''
params:
    {
     'customer_id': 123456,
     'borrow_id': 123567,
     'id_card_no': '12344556',
     'customer_mobile': '12345567',
     'bank_account': '1234456',
     'market_id': 123456,
     'device_sno': 'abcd',
     'device_mac':'abcd',
     'router_mac': 'abcd',
     'company_name': 'abcd',
     'position_type': 10,
     'proxy':'http',
     'ip': '192.168.0.1',
     'longitude': 80.94613,
     'latitude': 22.533586,
     'face_sim':0.5,
     'contact_list':[],
     'emergency':[],
     'customer_name':'abcd',
     'customer_birthday':'1988-10-13',
     'sim':[],
     'total_storage': 2000000000,
     'model_name': 'iPhone 7 Plus',
     'device_id': 123456,
     'device_platform': 'ios',
     'system_version': '17.2.1'
     }
'''
@app.route("/ios_new_customer", methods=["POST"])
def ios_new_customer():
    res = {'status': 200}
    params = json.loads(request.get_data(as_text=True))
    logger.info(f'parameter: {params}')
    uc = app.extensions['uc']
    if not uc.check_params(keys=list(params.keys()), customer_type=CustomerType.IOS_NEW):
        res['status'] = 1001
        res['message'] = 'parameter key error!'
        return json.dumps(res)
    model = app.extensions['ios_model'] if params['device_platform'] == 'ios' else app.extensions['model']
    # 紧急联系人不能为空
    if len(params['emergency']) == 0:
        res['status'] = 1003
        res['message'] = 'emergency is empty!'
        return json.dumps(res)
    record_time = pd.to_datetime(datetime.datetime.now())
    # 跑模型
    feature_params = {
                     'customer_id': params['customer_id'],
                     'market_id': params['market_id'],
                     'device_id': params['device_id'],
                     'borrow_id': params['borrow_id'],
                     'loan_type': CustomerType.IOS_NEW,
                     'emergency': params['emergency'],
                     'system_version': params['system_version']
                     }
    score = -1
    feature = {}
    try:
        feature = calc_feature(feature_params, uc)
        feature_df = uc.get_model_feature(model.feature_name(), feature)
        if feature_df.empty:
            logger.error('feature is empty!')
        else:
            score = uc.model_score(model, feature_df)
    except Exception as e:
        msg = traceback.format_exc()
        logger.error('run model error:' + str(e) + "\n" + msg)
    # 读数据库
    try:
        white_df = uc.get_sys_white_list(params['customer_mobile'])
        is_white = white_df['white_count'].item() > 0
        thresh_df = uc.get_threshold(customer_type=CustomerType.IOS_NEW, is_white=is_white)
        # 10: 身份证号
        id_black_df = uc.get_sys_black_list(10, params['market_id'], CustomerType.IOS_NEW, params['id_card_no'])
        # 20：手机号
        mobile_list = params['sim'].copy()
        mobile_list.append(params['customer_mobile'])
        mobile_black_df = uc.get_sys_black_list(20, params['market_id'], CustomerType.IOS_NEW, mobile_list)
        emergency_black_df = uc.get_sys_black_list(20, params['market_id'], CustomerType.IOS_NEW, params['emergency'])
        contact_black_df = pd.DataFrame([0], columns=['black_count'])
        if len(params['contact_list']) > 0:
            contact_black_df = uc.get_sys_black_list(20, params['market_id'], CustomerType.IOS_NEW, params['contact_list'])
        # 30：银行卡号
        bank_black_df = uc.get_sys_black_list(30, params['market_id'], CustomerType.IOS_NEW, params['bank_account'])
        # 60：设备黑名单
        device_black_df = uc.get_sys_black_list(60, params['market_id'], CustomerType.IOS_NEW, params['device_sno'])
        # 70：ip黑名单
        ip_black_df = uc.get_sys_black_list(70, params['market_id'], CustomerType.IOS_NEW, params['ip'])
        # 80：device_mac黑名单
        device_mac_black_df = uc.get_sys_black_list(80, params['market_id'], CustomerType.IOS_NEW, params['device_mac'])
        # 90：router_mac黑名单
        router_mac_black_df = uc.get_sys_black_list(90, params['market_id'], CustomerType.IOS_NEW, params['router_mac'])
        # 100：公司黑名单
        #company_black_df = uc.get_sys_black_list(100, params['market_id'], CustomerType.IOS_NEW, params['company_name'])
        # 110：职业黑名单
        position_black_df = uc.get_sys_black_list(110, params['market_id'], CustomerType.IOS_NEW, str(params['position_type']))
        params['longitude'] = float(format(params['longitude'],'.4f'))
        params['latitude'] = float(format(params['latitude'],'.4f'))
        date_3 = record_time - datetime.timedelta(days=3)
        date_1 = record_time - datetime.timedelta(days=1)
        gps_mobile_3d = uc.get_gps_borrow(params['longitude'],params['latitude'],date_3,params['market_id'],params['borrow_id'])
        emergency_customer = uc.get_emergency_customer(params['customer_mobile'],params['emergency'],params['market_id'])
        emergency_overdue = uc.get_emergency_overdue(params['emergency'],params['market_id'])
        emergency_customer_overdue = uc.get_customer_overdue_10d(params['customer_mobile'],params['market_id'])
        id_borrow = uc.get_id_borrow(params['id_card_no'],params['borrow_id'])
        mobile_sim_borrow = uc.get_mobile_sim_borrow(params['customer_mobile'],params['sim'],params['borrow_id'])
        device_borrow = uc.get_device_borrow(params['device_sno'],params['borrow_id'])
        bank_borrow = uc.get_bank_borrow(params['bank_account'],params['borrow_id'])
        name_birth_borrow = uc.get_name_birth_borrow(params['customer_name'],params['customer_birthday'],params['borrow_id'])
        other_device_borrow = uc.get_other_device_borrow(params['device_sno'],params['customer_mobile'],params['borrow_id'])
        contact_borrow = pd.DataFrame([], columns=['customer_mobile','borrow_status'])
        if len(params['contact_list']) > 0:
            contact_borrow = uc.get_contact_borrow(params['contact_list'],params['market_id'])
        device_mobile = uc.get_device_mobile_count(params['device_sno'],params['market_id'])
    except Exception as e:
        logger.error('read data error:' + str(e))
        res['status'] = 1002
        res['message'] = 'read data error!'
        return json.dumps(res)
    
    # 阈值
    thresh_dict = dict(zip(thresh_df['rule_name'],thresh_df['thresh_value']))
    status_dict = dict(zip(thresh_df['rule_name'],thresh_df['status']))
    result_dict = {'age':0,'id_black_list':0,'mobile_black_list':0,'bank_black_list':0,'device_black_list':0,'ip_black_list':0,'device_mac_black_list':0,
                     'router_mac_black_list':0,'company_black_list':0,'position_black_list':0,'socks':0,'http':0,'vpn':0,'gps_3d':0,'gps_1d':0,'emergency_black_list':0,
                     'emergency_customer':0,'emergency_overdue':0,'face':0,'emergency_overdue_days':0,'emergency_customer_overdue_10d':0,
                     'id_same_app':0,'id_diff_app_overdue':0,'mobile_sim_same_app':0,'mobile_sim_diff_app_overdue':0,'device_same_app':0,'device_diff_app_overdue':0,
                     'bank_same_app':0,'bank_diff_app_overdue':0,'name_birth_same_app':0,'name_birth_diff_app_overdue':0,'other_device_same_app':0,
                     'other_device_diff_app_overdue':0,'contact_num':0,'contact_borrow':0,'contact_overdue':0,'contact_black_list':0,'device_borrow':0,'device_mobile':0,
                     'valid_phone_ratio':0,'valid_phone_count':0,'mobile_white_list':0,'total_storage':0,'model_name':0,'common_phone_detection':0,'user_memory_use':0
                     }
    
    # 规则1：不合法年龄
    age_thresh = thresh_dict['age'].split(',')
    age = record_time.year - pd.to_datetime(params['customer_birthday']).year
    if age < int(age_thresh[0]) or age > int(age_thresh[1]):
        result_dict['age'] = 1
    # 规则2：身份证命中黑名单
    if id_black_df['black_count'].item() > 0:
        result_dict['id_black_list'] = 1
    # 规则3：手机号、sim卡命中黑名单
    if mobile_black_df['black_count'].item() > 0:
        result_dict['mobile_black_list'] = 1
    # 规则4：银行卡命中黑名单
    if bank_black_df['black_count'].item() > 0:
        result_dict['bank_black_list'] = 1
    # 规则5：设备黑名单
    if device_black_df['black_count'].item() > 0:
        result_dict['device_black_list'] = 1
    # 规则6：ip黑名单
    if ip_black_df['black_count'].item() > 0:
        result_dict['ip_black_list'] = 1
    # 规则7：设备mac黑名单
    if device_mac_black_df['black_count'].item() > 0:
        result_dict['device_mac_black_list'] = 1
    # 规则8： router mac黑名单
    if router_mac_black_df['black_count'].item() > 0:
        result_dict['router_mac_black_list'] = 1
    # 规则9：公司黑名单
    #if company_black_df['black_count'].item() > 0:
    #    result_dict['company_black_list'] = 1
    # 规则10：职业黑名单
    if position_black_df['black_count'].item() > 0:
        result_dict['position_black_list'] = 1
    # 规则11：使用SOCKS代理
    if params['proxy'] == 'socks':
        result_dict['socks'] = 1
    # 规则12：使用HTTP代理
    elif params['proxy'] == 'http':
        result_dict['http'] = 1
    # 规则13：使用VPN代理
    elif params['proxy'] == 'vpn':
        result_dict['vpn'] = 1
    # 规则14：相同market gps 三天内不同手机号申请数
    gps_3d_thresh = thresh_dict['gps_3d'].split(',')
    gps_mobile_count_3d = gps_mobile_3d['customer_id'].nunique()
    if gps_mobile_count_3d > int(gps_3d_thresh[0]):
        result_dict['gps_3d'] = 1
    # 规则15：相同market gps 一天内不同手机号申请数
    gps_1d_thresh = thresh_dict['gps_1d'].split(',')
    gps_mobile_1d = gps_mobile_3d[gps_mobile_3d['create_time'] > date_1]
    gps_mobile_count_1d = gps_mobile_1d['customer_id'].nunique()
    if gps_mobile_count_1d > int(gps_1d_thresh[0]):
        result_dict['gps_1d'] = 1
    # 规则16：紧急联系人在黑名单中的数量
    emergency_black_list_thresh = thresh_dict['emergency_black_list'].split(',')
    emergency_black_count = emergency_black_df['black_count'].item()
    if emergency_black_count > int(emergency_black_list_thresh[0]):
        result_dict['emergency_black_list'] = 1
    # 规则17：紧急联系人逾期未还的数量
    emergency_overdue_thresh = thresh_dict['emergency_overdue'].split(',')
    emergency_overdue_count = emergency_overdue['customer_id'].nunique()
    if emergency_overdue_count > int(emergency_overdue_thresh[0]):
        result_dict['emergency_overdue'] = 1
    # 规则18：紧急联系人被当作其它用户紧急联系人的次数
    emergency_customer_thresh = thresh_dict['emergency_customer'].split(',')
    emergency_customer_count = emergency_customer['customer_count'].item()
    if emergency_customer_count >= int(emergency_customer_thresh[0]):
        result_dict['emergency_customer'] = 1
    # 规则19：advance人脸相似度
    face_sim_thresh = thresh_dict['face'].split(',')
    if params['face_sim'] < float(face_sim_thresh[0]):
        result_dict['face'] = 1
    # 规则20：紧急联系人逾期>=x且没结清的数量>x
    emergency_overdue_days_thresh = thresh_dict['emergency_overdue_days'].split(',')
    emergency_overdue_max_days = emergency_overdue['over_due_days'].max()
    if emergency_overdue_max_days >= int(emergency_overdue_days_thresh[0]) and emergency_overdue_count > int(emergency_overdue_days_thresh[1]):
        result_dict['emergency_overdue_days'] = 1
    # 规则21：是否是其它坏客户（在本平台借款逾期大于10天）的紧急联系人
    emergency_customer_overdue_count = emergency_customer_overdue['overdue_count'].item()
    if emergency_customer_overdue_count > 0:
        result_dict['emergency_customer_overdue_10d'] = 1
    # 规则22：相同证件号，并且在本马甲包有过订单通过记录且未被取消的客户数量>x
    # 规则23：相同证件号，在其他马甲包中有未结清订单的客户数量>x
    id_same_app_thresh = thresh_dict['id_same_app'].split(',')
    id_diff_app_thresh = thresh_dict['id_diff_app_overdue'].split(',')
    id_borrow_same_app = id_borrow[id_borrow['market_id'] == params['market_id']]
    id_borrow_diff_app_overdue = id_borrow[(id_borrow['market_id'] != params['market_id']) & (id_borrow['borrow_status'] == 70)]
    id_same_app_count = id_borrow_same_app['customer_id'].nunique()
    id_diff_app_overdue_count = id_borrow_diff_app_overdue['customer_id'].count()
    if id_same_app_count > int(id_same_app_thresh[0]):
        result_dict['id_same_app'] = 1
    if id_diff_app_overdue_count > int(id_diff_app_thresh[0]):
        result_dict['id_diff_app_overdue'] = 1
    # 规则24：相同手机号、sim卡，并且在本马甲包有过订单通过记录且未被取消的客户数量>x
    # 规则25：相同手机号、sim卡，在其他马甲包中有未结清订单的客户数量>x
    mobile_sim_same_app_thresh = thresh_dict['mobile_sim_same_app'].split(',')
    mobile_sim_diff_app_thresh = thresh_dict['mobile_sim_diff_app_overdue'].split(',')
    mobile_sim_borrow_same_app = mobile_sim_borrow[mobile_sim_borrow['market_id'] == params['market_id']]
    mobile_sim_borrow_diff_app_overdue = mobile_sim_borrow[(mobile_sim_borrow['market_id'] != params['market_id']) & (mobile_sim_borrow['borrow_status'] == 70)]
    mobile_sim_same_app_count = mobile_sim_borrow_same_app['customer_id'].nunique()
    mobile_sim_diff_app_overdue_count = mobile_sim_borrow_diff_app_overdue['customer_id'].count()
    if mobile_sim_same_app_count > int(mobile_sim_same_app_thresh[0]):
        result_dict['mobile_sim_same_app'] = 1
    if mobile_sim_diff_app_overdue_count > int(mobile_sim_diff_app_thresh[0]):
        result_dict['mobile_sim_diff_app_overdue'] = 1
    # 规则26：相同设备号，并且在本马甲包有过订单通过记录且未被取消的客户数量>x
    # 规则27：相同设备号，在其他马甲包中有未结清订单的客户数量>x
    device_same_app_thresh = thresh_dict['device_same_app'].split(',')
    device_diff_app_thresh = thresh_dict['device_diff_app_overdue'].split(',')
    device_borrow_same_app = device_borrow[(device_borrow['market_id'] == params['market_id']) & (device_borrow['borrow_status'].isin([10,20,30,40,50,60,70]))]
    device_borrow_diff_app_overdue = device_borrow[(device_borrow['market_id'] != params['market_id']) & (device_borrow['borrow_status'] == 70)]
    device_same_app_count = device_borrow_same_app['customer_id'].nunique()
    device_diff_app_overdue_count = device_borrow_diff_app_overdue['customer_id'].count()
    if device_same_app_count > int(device_same_app_thresh[0]):
        result_dict['device_same_app'] = 1
    if device_diff_app_overdue_count > int(device_diff_app_thresh[0]):
        result_dict['device_diff_app_overdue'] = 1
    # 规则28：相同银行卡号，并且在本马甲包有过订单通过记录且未被取消的客户数量>x
    # 规则29：相同银行卡号，在其他马甲包中有未结清订单
    bank_same_app_thresh = thresh_dict['bank_same_app'].split(',')
    bank_diff_app_thresh = thresh_dict['bank_diff_app_overdue'].split(',')
    bank_borrow_same_app = bank_borrow[bank_borrow['market_id'] == params['market_id']]
    bank_borrow_diff_app_overdue = bank_borrow[(bank_borrow['market_id'] != params['market_id']) & (bank_borrow['borrow_status'] == 70)]
    bank_same_app_count = bank_borrow_same_app['customer_id'].nunique()
    bank_diff_app_overdue_count = bank_borrow_diff_app_overdue['customer_id'].count()
    if bank_same_app_count > int(bank_same_app_thresh[0]):
        result_dict['bank_same_app'] = 1
    if bank_diff_app_overdue_count > int(bank_diff_app_thresh[0]):
        result_dict['bank_diff_app_overdue'] = 1
    # 规则30：相同名字、出生日期，并且在本马甲包中有过订单通过记录且未被取消的客户数量>x
    # 规则31：相同名字、出生日期，并且在其他马甲包中有未结清订单
    name_birth_same_app_thresh = thresh_dict['name_birth_same_app'].split(',')
    name_birth_diff_app_thresh = thresh_dict['name_birth_diff_app_overdue'].split(',')
    name_birth_borrow_same_app = name_birth_borrow[name_birth_borrow['market_id'] == params['market_id']]
    name_birth_borrow_diff_app_overdue = name_birth_borrow[(name_birth_borrow['market_id'] != params['market_id']) & (name_birth_borrow['borrow_status'] == 70)]
    name_birth_same_app_count = name_birth_borrow_same_app['customer_id'].nunique()
    name_birth_diff_app_overdue_count = name_birth_borrow_diff_app_overdue['customer_id'].count()
    if name_birth_same_app_count > int(name_birth_same_app_thresh[0]):
        result_dict['name_birth_same_app'] = 1
    if name_birth_diff_app_overdue_count > int(name_birth_diff_app_thresh[0]):
        result_dict['name_birth_diff_app_overdue'] = 1
    # 规则32：用户使用过的设备上，在本马甲包有订单通过记录且未被取消的客户数量>x
    # 规则33：用户使用过的设备上，在其他马甲包中有未结清订单
    other_device_same_app_thresh = thresh_dict['other_device_same_app'].split(',')
    other_device_diff_app_thresh = thresh_dict['other_device_diff_app_overdue'].split(',')
    other_device_borrow_same_app = other_device_borrow[other_device_borrow['market_id'] == params['market_id']]
    other_device_borrow_diff_app_overdue = other_device_borrow[(other_device_borrow['market_id'] != params['market_id']) & (other_device_borrow['borrow_status'] == 70)]
    other_device_same_app_count = other_device_borrow_same_app['customer_id'].nunique()
    other_device_diff_app_overdue_count = other_device_borrow_diff_app_overdue['customer_id'].count()
    if other_device_same_app_count > int(other_device_same_app_thresh[0]):
        result_dict['other_device_same_app'] = 1
    if other_device_diff_app_overdue_count > int(other_device_diff_app_thresh[0]):
        result_dict['other_device_diff_app_overdue'] = 1
    # 规则34：通讯录无重复并且通讯录个数>x
    contact_list = params['contact_list']
    contact_num_thresh = thresh_dict['contact_num'].split(',')
    if len(set(contact_list)) == len(contact_list) and  len(contact_list) > int(contact_num_thresh[0]):
        result_dict['contact_num'] = 1
    # 规则35：通讯录联系人有借贷的人数>x
    # 规则36：通讯录中联系人逾期数量>x
    contact_borrow_thresh = thresh_dict['contact_borrow'].split(',')
    contact_overdue_thresh = thresh_dict['contact_overdue'].split(',')
    contact_borrow_count = contact_borrow['customer_mobile'].nunique()
    contact_overdue = contact_borrow[contact_borrow['borrow_status'] == 70]
    contact_overdue_count = contact_overdue['customer_mobile'].count()
    if contact_borrow_count > int(contact_borrow_thresh[0]):
        result_dict['contact_borrow'] = 1
    if contact_overdue_count > int(contact_overdue_thresh[0]):
        result_dict['contact_overdue'] = 1
    # 规则37：通讯录联系人黑名单数量>x
    contact_black_list_thresh = thresh_dict['contact_black_list'].split(',')
    contact_black_count = contact_black_df['black_count'].item()
    if contact_black_count > int(contact_black_list_thresh[0]):
        result_dict['contact_black_list'] = 1
    # 规则38：相同设备识别号，有申请记录的电话号码>x
    device_borrow_thresh = thresh_dict['device_borrow'].split(',')
    device_borrow_temp = device_borrow[device_borrow['market_id'] == params['market_id']]
    device_borrow_count = device_borrow_temp['customer_id'].nunique()
    if device_borrow_count > int(device_borrow_thresh[0]):
        result_dict['device_borrow'] = 1
    # 规则39：相同设备识别号，注册手机号>x
    device_mobile_thresh = thresh_dict['device_mobile'].split(',')
    device_mobile_count = device_mobile['mobile_count'].item()
    if device_mobile_count > int(device_mobile_thresh[0]):
        result_dict['device_mobile'] = 1
    # 规则40：通讯录中正确电话号码数量占总数量比例<=x
    # 规则41：通讯录中正确电话号码数量<=x
    valid_phone_ratio_thresh = thresh_dict['valid_phone_ratio'].split(',')
    valid_phone_count_thresh = thresh_dict['valid_phone_count'].split(',')
    valid_phone_count, valid_phone_ratio = uc.get_valid_phone(params['contact_list'])
    if len(params['contact_list']) > 0 and valid_phone_ratio <= float(valid_phone_ratio_thresh[0]):
        result_dict['valid_phone_ratio'] = 1
    if len(params['contact_list']) > 0 and valid_phone_count <= int(valid_phone_count_thresh[0]):
        result_dict['valid_phone_count'] = 1
    # 规则42：手机号是否在白名单中，白名单不过其它规则
    if is_white:
        result_dict['mobile_white_list'] = 1
    # 规则43：设备内存<x
    total_storage_thresh = thresh_dict['total_storage'].split(',')
    if params['total_storage'] < int(total_storage_thresh[0]):
        result_dict['total_storage'] = 1
    # 规则44：iphone x 型号以下
    model_name_thresh = thresh_dict['model_name'].split(',')
    temp = re.findall(r"iphone \d+", params['model_name'].lower())
    if len(temp) > 0:
        iphone_num = int(temp[0].split(' ')[1])
        if iphone_num < int(model_name_thresh[0]):
            result_dict['model_name'] = 1
    # 规则45：手机除去系统应用使用的硬盘<x
    user_memory_use_thresh = thresh_dict['model_name'].split(',')
    user_card_size = feature.get('userCardSizeUse')
    if user_card_size is not None and user_card_size < int(user_memory_use_thresh[0]):
        result_dict['user_memory_use'] = 1
        
    # 判断风控状态: 白名单-拒绝-转审核-直接放款
    rule_message_list = [] # model_result_reason
    status = StatusType.REVIEW # 新客默认转审核
    for k,v in result_dict.items():
        if v == 1:
            rule_message_list.append(k)
            if status_dict[k] > status:
                status = status_dict[k]
    '''
    # advance common_phone_detection
    if status != StatusType.REJECT and (result_dict['mobile_white_list'] == 0 or status_dict['mobile_white_list'] == 0) and status_dict['common_phone_detection'] != 0:
        advance_result, phone = uc.advance_request(uc.config['url'], uc.config['key'], params['customer_mobile'])
        if isinstance(advance_result, dict):
            if isinstance(advance_result['data'], dict):
                if advance_result['data']['status'] == 0:
                    result_dict['common_phone_detection'] = 1
                    status = status_dict['common_phone_detection']
                    rule_message_list.append('common_phone_detection')
            # 记录advance调用结果
            advance_result['borrow_id'] = params['borrow_id']
            advance_result['customer_mobile'] = phone
            advance_result['record_time'] = record_time.strftime('%Y-%m-%d %H:%M:%S')
            uc.set_advance_common_phone(advance_result)
    '''
    # 记录规则命中情况
    result_dict['borrow_id'] = params['borrow_id']
    result_dict['record_time'] = record_time
    result_dict['result'] = status
    result_dict['score'] = score
    uc.set_risk_control_result(result_dict)
    # 成功返回
    res['message'] = ','.join(rule_message_list)
    res['result'] = status
    res['score'] = score
    return json.dumps(res)


'''
params:
    {
     'borrow_id': 123456,
     'customer_id': 123456,
     'id_card_no': '12344556',
     'customer_mobile': '12345567',
     'bank_account': '1234456',
     'market_id': 123456,
     'device_sno': 'abcd',
     'customer_name':'abcd',
     'customer_birthday':'1988-10-13',
     'sim':['123456','654321'],
     'device_id': 123456,
     'emergency': [],
     'device_platform': 'ios'
     }
'''
@app.route("/ios_old_customer", methods=["POST"])
def ios_old_customer():
    res = {'status': 200}
    params = json.loads(request.get_data(as_text=True))
    logger.info(f'parameter: {params}')
    uc = app.extensions['uc']
    if not uc.check_params(keys=list(params.keys()), customer_type=CustomerType.IOS_OLD):
        res['status'] = 1001
        res['message'] = 'parameter key error!'
        return json.dumps(res)
    # 紧急联系人不能为空
    if len(params['emergency']) == 0:
        res['status'] = 1003
        res['message'] = 'emergency is empty!'
        return json.dumps(res)
    
    model = app.extensions['old_model'] if params['device_platform'] == 'android' else None
    # 跑模型
    feature_params = {
                     'customer_id': params['customer_id'],
                     'market_id': params['market_id'],
                     'device_id': params['device_id'],
                     'borrow_id': params['borrow_id'],
                     'loan_type': CustomerType.IOS_OLD,
                     'emergency': params['emergency'],
                     'system_version': ''
                     }
    score = -1
    try:
        feature = calc_feature(feature_params, uc)
        if model is not None:
            feature_df = uc.get_model_feature(model.feature_name(), feature)
            if feature_df.empty:
                logger.error('feature is empty!')
            else:
                score = uc.model_score(model, feature_df)
        else:
            score = 800
    except Exception as e:
        msg = traceback.format_exc()
        logger.error('run model error:' + str(e) + "\n" + msg)
    
    try:
        # 读数据库
        white_df = uc.get_sys_white_list(params['customer_mobile'])
        is_white = white_df['white_count'].item() > 0
        thresh_df = uc.get_threshold(customer_type=CustomerType.IOS_OLD, is_white=is_white)
        # 10: 身份证号
        id_black_df = uc.get_sys_black_list(10, params['market_id'], CustomerType.IOS_OLD, params['id_card_no'])
        # 20：手机号
        mobile_list = params['sim'].copy()
        mobile_list.append(params['customer_mobile'])
        mobile_black_df = uc.get_sys_black_list(20, params['market_id'], CustomerType.IOS_OLD, mobile_list)
        # 30：银行卡号
        bank_black_df = uc.get_sys_black_list(30, params['market_id'], CustomerType.IOS_OLD, params['bank_account'])
        # 60：设备黑名单
        device_black_df = uc.get_sys_black_list(60, params['market_id'], CustomerType.IOS_OLD, params['device_sno'])
        customer_device = uc.get_customer_device(params['customer_id'],params['borrow_id'])
        id_borrow = uc.get_id_borrow_count(params['id_card_no'],params['market_id'],params['borrow_id'])
        mobile_sim_borrow = uc.get_mobile_sim_borrow_count(params['customer_mobile'],params['sim'],params['market_id'],params['borrow_id'])
        device_borrow = uc.get_device_borrow_count(params['device_sno'],params['market_id'],params['borrow_id'])
        bank_borrow = uc.get_bank_borrow_count(params['bank_account'],params['market_id'],params['borrow_id'])
        name_birth_borrow = uc.get_name_birth_borrow_count(params['customer_name'],params['customer_birthday'],params['market_id'],params['borrow_id'])
        other_device_borrow = uc.get_other_device_borrow_count(params['device_sno'],params['customer_mobile'],params['market_id'],params['borrow_id'])
        borrow_df = uc.get_last_overdue(params['customer_id'])
    except Exception as e:
        logger.error('read data error:' + str(e))
        res['status'] = 1002
        res['message'] = 'read data error!'
        return json.dumps(res)
    
    record_time = pd.to_datetime(datetime.datetime.now())
    # 阈值
    thresh_dict = dict(zip(thresh_df['rule_name'],thresh_df['thresh_value']))
    status_dict = dict(zip(thresh_df['rule_name'],thresh_df['status']))
    result_dict = {'mobile_white_list':0,'id_black_list':0,'mobile_black_list':0,'bank_black_list':0,'device_black_list':0,'customer_overdue':0,'customer_device':0,
                   'id_same_app':0,'mobile_sim_same_app':0,'device_same_app':0,'bank_same_app':0,'name_birth_same_app':0,'other_device_same_app':0}

    # 规则1：手机号是否在白名单，白名单不过其他规则
    if is_white:
        result_dict['mobile_white_list'] = 1
    # 规则2：身份证命中黑名单
    if id_black_df['black_count'].item() > 0:
        result_dict['id_black_list'] = 1
    # 规则3：手机号、sim卡命中黑名单
    if mobile_black_df['black_count'].item() > 0:
        result_dict['mobile_black_list'] = 1
    # 规则4：银行卡命中黑名单
    if bank_black_df['black_count'].item() > 0:
        result_dict['bank_black_list'] = 1
    # 规则5：设备黑名单
    if device_black_df['black_count'].item() > 0:
        result_dict['device_black_list'] = 1
    # 规则6：上一次逾期天数>x
    last_overdue_days = 0
    if not borrow_df.empty:
        last_overdue_days = borrow_df['over_due_days'].iloc[0]
    customer_overdue_thresh = thresh_dict['customer_overdue'].split(',')
    if last_overdue_days > int(customer_overdue_thresh[0]):
        result_dict['customer_overdue'] = 1
    # 规则7：本次申请是否变更了设备识别号
    last_device_no = params['device_sno']
    if not customer_device.empty:
        last_device_no = customer_device['device_sno'].iloc[0]
    if last_device_no != params['device_sno']:
        result_dict['customer_device'] = 1
    # 规则8：相同证件号，并且在本马甲包有过订单通过记录且未被取消的客户数量>x
    id_same_app_thresh = thresh_dict['id_same_app'].split(',')
    id_same_app_count = id_borrow['customer_count'].item()
    if id_same_app_count > int(id_same_app_thresh[0]):
        result_dict['id_same_app'] = 1
    # 规则9：相同手机号、sim卡，并且在本马甲包有过订单通过记录且未被取消的客户数量>x
    mobile_sim_same_app_thresh = thresh_dict['mobile_sim_same_app'].split(',')
    mobile_sim_same_app_count = mobile_sim_borrow['customer_count'].item()
    if mobile_sim_same_app_count > int(mobile_sim_same_app_thresh[0]):
        result_dict['mobile_sim_same_app'] = 1
    # 规则10：相同设备号，并且在本马甲包有过订单通过记录且未被取消的客户数量>x
    device_same_app_thresh = thresh_dict['device_same_app'].split(',')
    device_same_app_count = device_borrow['customer_count'].item()
    if device_same_app_count > int(device_same_app_thresh[0]):
        result_dict['device_same_app'] = 1
    # 规则11：相同银行卡号，并且在本马甲包有过订单通过记录且未被取消的客户数量>x
    bank_same_app_thresh = thresh_dict['bank_same_app'].split(',')
    bank_same_app_count = bank_borrow['customer_count'].item()
    if bank_same_app_count > int(bank_same_app_thresh[0]):
        result_dict['bank_same_app'] = 1
    # 规则12：相同名字、出生日期，并且在本马甲包中有过订单通过记录且未被取消的客户数量>x
    name_birth_same_app_thresh = thresh_dict['name_birth_same_app'].split(',')
    name_birth_same_app_count = name_birth_borrow['customer_count'].item()
    if name_birth_same_app_count > int(name_birth_same_app_thresh[0]):
        result_dict['name_birth_same_app'] = 1
    # 规则13：用户使用过的设备上，在本马甲包有订单通过记录且未被取消的客户数量>x
    other_device_same_app_thresh = thresh_dict['other_device_same_app'].split(',')
    other_device_same_app_count = other_device_borrow['customer_count'].item()
    if other_device_same_app_count > int(other_device_same_app_thresh[0]):
        result_dict['other_device_same_app'] = 1

    # 判断风控状态: 白名单-拒绝-转审核-直接放款
    rule_message_list = [] # model_result_reason
    status = StatusType.RELEASE # 复贷默认直接放款
    for k,v in result_dict.items():
        if v == 1:
            rule_message_list.append(k)
            if status_dict[k] > status:
                status = status_dict[k]
    
    # 记录规则命中情况
    result_dict['borrow_id'] = params['borrow_id']
    result_dict['record_time'] = record_time
    result_dict['result'] = status
    result_dict['score'] = score
    uc.set_risk_control_result(result_dict)
    # 成功返回
    res['message'] = ','.join(rule_message_list)
    res['result'] = status
    res['score'] = score
    return json.dumps(res)


'''
params:
    {
     'customer_id': 123456,
     'market_id': 123456,
     'device_id': 123456,
     'borrow_id': 12345,
     'loan_type': 10,
     'emergency': [],
     }
'''
def calc_feature(params, uc):
    feature = {}
    record_time = pd.to_datetime(datetime.datetime.now())
    start_date = record_time - datetime.timedelta(days=365)
    try:
        # 读数据库
        contact_divider = 10000
        app_divider = 100000
        basic_df = uc.get_basic_info(params['customer_id'])
        emergency_df = uc.get_emergency_list(params['customer_id'],params['market_id'],params['emergency'],start_date.strftime('%Y-%m-%d'))
        last_emergency = uc.get_last_borrow_emergency(params['customer_id'])
        contact_df = uc.get_contact_list(params['device_id'], contact_divider)
        contact_borrow = pd.DataFrame([], columns=['customer_mobile','borrow_status'])
        app_df = uc.get_app_list(params['device_id'], app_divider)
        app_df['app_id'] = app_df['package_name'].apply(lambda x:zlib.crc32(x.encode()))
        install_app_df = uc.get_install_app()
        content_df = uc.get_device_raw(params['device_id'])
        google_play_df = pd.DataFrame([], columns=['genre_id', 'app_id'])
        if len(app_df) > 0:
            google_play_df = uc.get_google_play(app_df['app_id'].tolist())
        if len(contact_df) > 0:
            contact_borrow = uc.get_contact_borrow(contact_df['phone'].unique().tolist(), params['market_id'])
        operate_df = uc.get_operate_track(params['customer_id'], record_time)
        borrow_df = uc.get_borrow_feature(params['customer_id'])
    except Exception as e:
        logger.error('calc_feature: read data error: ' + str(e))
        return feature
    
    if basic_df.empty:
        logger.error('calc_feature: basic info empty!')
        return feature
    basic_df['create_time'] = pd.to_datetime(basic_df['create_time'])
    basic_df['customer_birthday'] = pd.to_datetime(basic_df['customer_birthday'])
    try:
        contact_df['in_time'] = pd.to_datetime(contact_df['in_time'])
    except Exception as e:
        logger.error('calc_feature: in_time error: ' + str(e))
        contact_df['in_time'] = pd.to_datetime(np.nan)
    app_df['in_time'] = pd.to_datetime(app_df['in_time'])
    operate_df['operation_time'] = pd.to_datetime(operate_df['operation_time'])
    borrow_df['create_time'] = pd.to_datetime(borrow_df['create_time'])
    # 复贷特征
    if params['loan_type'] == CustomerType.IOS_OLD:
        monthly_income = uc.get_monthly_income(basic_df['monthly_income'].iloc[0])
        borrow_df['loan_amount'] = borrow_df['principal_amount'] + borrow_df['interest_amount']
        old_borrow = borrow_df[borrow_df['borrow_status'].isin([1000,1010])].copy()
        old_borrow = old_borrow.reset_index(drop=True)
        new_borrow = borrow_df[borrow_df['id'] == params['borrow_id']]
        if old_borrow.empty or new_borrow.empty:
            logger.error('calc_feature: borrow data error!')
            return feature
        # account_amount_num 取用户所有的放款成功的单子，取borrow表中多少个不同的principal_amount
        feature['account_amount_num'] = old_borrow['principal_amount'].nunique()
        # loan_num 用户所有的放款成功的单子数
        feature['loan_num'] = len(old_borrow)
        # max_loan 用户所有的放款成功的单子中，borrow表中principal_amount+interest_amount的最大值
        feature['max_loan'] = old_borrow['loan_amount'].max()
        # min_loan 用户所有的放款成功的单子中，borrow表中principal_amount+interest_amount的最小值
        feature['min_loan'] = old_borrow['loan_amount'].min()
        # avg_loan 用户所有的放款成功的单子中，borrow表中principal_amount+interest_amount的平均值
        feature['avg_loan'] = old_borrow['loan_amount'].mean()
        # overdue1_num 用户所有的放款成功的单子中，over_due_days>=1的数量
        temp = old_borrow[old_borrow['over_due_days'] >= 1]
        feature['overdue1_num'] = len(temp)
        # overdue1_loan_rate 用户在本公司逾期1天以上还款次数和贷款次数的比值
        feature['overdue1_loan_rate'] = feature['overdue1_num'] / feature['loan_num']
        # first_overdue_num 用户所有的放款成功的单子中首次出现overdue_days>=1是第几次放款
        feature['first_overdue_num'] = temp.index[0]+1 if len(temp) > 0 else np.nan
        # first_overdue_amount 首次出现overdue_days>=1的订单（principal_amount+interest_amount）
        feature['first_overdue_amount'] = temp['principal_amount'].iloc[0] + temp['interest_amount'].iloc[0] if len(temp) > 0 else np.nan
        # first_overdue_income_rate 用户所有的放款成功的单子中首次出现overdue_days>=1时的customer表中monthly_income /（principal_amount+interest_amount）
        feature['first_overdue_income_rate'] = monthly_income / feature['first_overdue_amount']
        # first_overdue_loan_rate 本次借款订单的（principal_amount+interest_amount），除以用户所有的放款成功的单子中首次出现overdue_days>=1时的principal_amount+interest_amount）
        feature['first_overdue_loan_rate'] = (new_borrow['principal_amount'].iloc[0] + new_borrow['interest_amount'].iloc[0]) / feature['first_overdue_amount']
        # overdue3_num 用户所有的放款成功的单子中，over_due_days>=3的数量
        temp = old_borrow[old_borrow['over_due_days'] >= 3]
        feature['overdue3_num'] = len(temp)
        # overdue3_loan_rate 用户在本公司逾期3天以上还款次数和贷款次数的比值
        feature['overdue3_loan_rate'] = feature['overdue3_num'] / feature['loan_num']
        # first_overdue3_num 用户所有的放款成功的单子中首次出现overdue_days>=3是第几次放款
        feature['first_overdue3_num'] = temp.index[0]+1 if len(temp) > 0 else np.nan
        # first_overdue3_amount 首次出现overdue_days>=3的订单（principal_amount+interest_amount）
        feature['first_overdue3_amount'] = temp['principal_amount'].iloc[0] + temp['interest_amount'].iloc[0] if len(temp) > 0 else np.nan
        # first_overdue3_income_rate 用户所有的放款成功的单子中首次出现overdue_days>=3时的customer表中monthly_income /（principal_amount+interest_amount）
        feature['first_overdue3_income_rate'] = monthly_income / feature['first_overdue3_amount']
        # first_overdue3_loan_rate 本次借款订单的（principal_amount+interest_amount），除以用户所有的放款成功的单子中首次出现overdue_days>=3时的principal_amount+interest_amount）
        feature['first_overdue3_loan_rate'] = (new_borrow['principal_amount'].iloc[0] + new_borrow['interest_amount'].iloc[0]) / feature['first_overdue3_amount']
        # overdue5_num 用户所有的放款成功的单子中，over_due_days>=5的数量
        temp = old_borrow[old_borrow['over_due_days'] >= 5]
        feature['overdue5_num'] = len(temp)
        # overdue5_loan_rate 用户在本公司逾期5天以上还款次数和贷款次数的比值
        feature['overdue5_loan_rate'] = feature['overdue5_num'] / feature['loan_num']
        # first_overdue5_num 用户所有的放款成功的单子中首次出现overdue_days>=5是第几次放款
        feature['first_overdue5_num'] = temp.index[0]+1 if len(temp) > 0 else np.nan
        # first_overdue5_amount 首次出现overdue_days>=5的订单（principal_amount+interest_amount）
        feature['first_overdue5_amount'] = temp['principal_amount'].iloc[0] + temp['interest_amount'].iloc[0] if len(temp) > 0 else np.nan
        # first_overdue5_income_rate 用户所有的放款成功的单子中首次出现overdue_days>=5时的customer表中monthly_income /（principal_amount+interest_amount）
        feature['first_overdue5_income_rate'] = monthly_income / feature['first_overdue5_amount']
        # first_overdue5_loan_rate 本次借款订单的（principal_amount+interest_amount），除以用户所有的放款成功的单子中首次出现overdue_days>=5时的principal_amount+interest_amount）
        feature['first_overdue5_loan_rate'] = (new_borrow['principal_amount'].iloc[0] + new_borrow['interest_amount'].iloc[0]) / feature['first_overdue5_amount']
        # ad_loan_num 用户所有的放款成功的单子中，over_due_days<0的数量
        temp = old_borrow[old_borrow['over_due_days'] < 0]
        feature['ad_loan_num'] = len(temp)
        # ad_loan_ratio 用户提前还款次数和贷款次数的比值
        feature['ad_loan_ratio'] = feature['ad_loan_num'] / feature['loan_num']
        # first_ad_num 用户所有的放款成功的单子中首次出现overdue_days<0是第几次放款
        feature['first_ad_num'] = temp.index[0]+1 if len(temp) > 0 else np.nan
        # nor_loan_num 用户在本公司正常还款次数
        feature['nor_loan_num'] = feature['loan_num'] - feature['overdue1_num']
        # nor_loan_num_ratio1 用户在本公司正常还款次数与贷款次数的比值
        feature['nor_loan_num_ratio1'] = feature['nor_loan_num'] / feature['loan_num']
        # min_account_amount_num 用户所有的放款成功的单子中,principal_amount+interest_amount=min_loan的次数
        temp = old_borrow[old_borrow['loan_amount'] == feature['min_loan']]
        feature['min_account_amount_num'] = len(temp)
        # loan_amount_max_ratio 本次订单中，(principal_amount+interest_amount)/max_loan
        feature['loan_amount_max_ratio'] = (new_borrow['principal_amount'].iloc[0] + new_borrow['interest_amount'].iloc[0]) / feature['max_loan']
        # time_interval_loan 本次申请时间减去用户所有的放款成功的单子中时间的最小值
        feature['time_interval_loan'] = (record_time - old_borrow['create_time'].min()).days
        # avg_time_interval_loan 用户在本公司平均每次申请贷款的时间间隔
        feature['avg_time_interval_loan'] = feature['time_interval_loan'] / feature['loan_num']
        # max_overdue_days 用户所有的放款成功的单子中over_due_days的最大值
        feature['max_overdue_days'] = old_borrow['over_due_days'].max()
        # min_overdue_days 用户所有的放款成功的单子中over_due_days的最小值
        feature['min_overdue_days'] = old_borrow['over_due_days'].min()
        # last_overdue_days 用户所有的放款成功的单子中create_time的最大值订单中的overdue_days
        feature['last_overdue_days'] = old_borrow['over_due_days'].iloc[-1]
        # last_max_overdue_days_ratio 上一次还款逾期天数和最大逾期天数的比值
        feature['last_max_overdue_days_ratio'] = feature['last_overdue_days'] / feature['max_overdue_days'] if feature['max_overdue_days'] > 0 else np.nan
        
    
    # new_be_urgent_count 1年内申请的新客；统计次数大于0的紧急联系人个数
    # new_be_urgent_time_count 1年内申请的新客；统计全部紧急联系人对应的总次数
    # new_be_urgent_time_count_max 1年内申请的新客；统计次数的最大值
    # new_be_urgent_time_count_avg 1年内申请的新客；统计次数的平均值
    new_df = emergency_df[emergency_df['loan_type'] == 10]
    temp = pd.concat([new_df['contact1_mobile'],new_df['contact2_mobile'],new_df['contact3_mobile'],new_df['contact4_mobile'],new_df['contact5_mobile']])
    temp = temp.reset_index().rename(columns={0:'contact_mobile'})
    temp = temp[temp['contact_mobile'].isin(params['emergency'])]
    temp = temp.groupby('contact_mobile')['index'].count().reset_index()
    feature['new_be_urgent_count'] = len(temp)
    feature['new_be_urgent_time_count'] = temp['index'].sum()
    feature['new_be_urgent_time_count_max'] = temp['index'].max()
    feature['new_be_urgent_time_count_avg'] = temp['index'].mean()
    # newold_be_urgent_count 1年内申请的新客和老客: 统计次数大于0的紧急联系人个数
    # newold_be_urgent_time_count 1年内申请的新客和老客: 统计全部紧急联系人对应的总次数
    # newold_be_urgent_time_count_max 1年内申请的新客和老客: 统计次数的最大值
    temp = pd.concat([emergency_df['contact1_mobile'],emergency_df['contact2_mobile'],emergency_df['contact3_mobile'],emergency_df['contact4_mobile'],emergency_df['contact5_mobile']])
    temp = temp.reset_index().rename(columns={0:'contact_mobile'})
    temp = temp[temp['contact_mobile'].isin(params['emergency'])]
    temp = temp.groupby('contact_mobile')['index'].count().reset_index()
    feature['newold_be_urgent_count'] = len(temp)
    feature['newold_be_urgent_time_count'] = temp['index'].sum()
    feature['newold_be_urgent_time_count_max'] = temp['index'].max()
    # newold_be_urgent_time_count90 90天内申请的新客和老客: 统计全部紧急联系人对应的总次数
    # newold_be_urgent_time_count_max90 90天内申请的新客和老客: 统计次数的最大值
    last_90 = record_time - datetime.timedelta(days=90)
    df_90 = emergency_df[emergency_df['create_time'] > last_90]
    temp = pd.concat([df_90['contact1_mobile'],df_90['contact2_mobile'],df_90['contact3_mobile'],df_90['contact4_mobile'],df_90['contact5_mobile']])
    temp = temp.reset_index().rename(columns={0:'contact_mobile'})
    temp = temp[temp['contact_mobile'].isin(params['emergency'])]
    temp = temp.groupby('contact_mobile')['index'].count().reset_index()
    feature['newold_be_urgent_time_count90'] = temp['index'].sum()
    feature['newold_be_urgent_time_count_max90'] = temp['index'].max()
    # contact_change_count 取老客成功放款的订单对应的紧急联系人，通过customer_id关联出上一笔成功放款的订单，以及该订单对应的紧急联系人；对比两次的紧急联系人不同的个数
    # contact_change_count_inner_24 取老客成功放款的订单对应的紧急联系人，通过customer_id关联出上一笔成功放款的订单，以及该订单对应的紧急联系人；取出两次紧急联系人不同的telephone_no，查询出本次申请时，这些不同的紧急联系人创建的时间，距本次申请时间24小时内的个数
    # contact_create_count_inner_24 关联申请订单的紧急联系人与这些联系人在通讯录中的创建时间（排除新增紧急联系人），统计这些紧急联系人在24小时内创建的个数
    # contact_create_avg_hour 关联申请订单的紧急联系人与这些联系人在通讯录中的创建时间（排除新增紧急联系人），统计这些紧急联系人创建时间与申请时间的小时差值，再取平均
    last_24 = record_time - datetime.timedelta(hours=24)
    if len(last_emergency) > 0:
        last_emergency_list = list(last_emergency.iloc[0])
        diff_emergency_list = list(set(params['emergency']) - set(last_emergency_list))
        feature['contact_change_count'] = len(diff_emergency_list)
        temp = contact_df[contact_df['phone'].isin(diff_emergency_list)]
        temp = temp[temp['in_time'] > last_24]
        feature['contact_change_count_inner_24'] = len(temp)
    temp = contact_df[contact_df['phone'].isin(params['emergency'])]
    diff_hours = (record_time - temp['in_time']).dt.total_seconds() / 3600
    feature['contact_create_avg_hour'] = diff_hours.mean()
    temp = temp[temp['in_time'] > last_24]
    feature['contact_create_count_inner_24'] = len(temp)
    
    # contact_amount 通讯录所有号码数量
    feature['contact_amount'] = len(contact_df)
    if feature['contact_amount'] > 0:
        # earliest_modifi_day 取通讯录最早日期，与本次申请日期做差（天数）
        feature['earliest_modifi_day'] = (record_time - contact_df['in_time'].min()).days 
        # earliest_last_moifi_day 取通讯录最早日期与最晚日期，相减得天数差
        feature['earliest_last_moifi_day'] = (contact_df['in_time'].max() - contact_df['in_time'].min()).days
        # mail_create_percent_inner_24 关联申请订单的全部通讯录，统计这些通讯录创建时间距申请时间24小时内的个数，统计其在全部通讯录中的占比（不需要去重）
        temp = contact_df[contact_df['in_time'] > last_24]
        feature['mail_create_percent_inner_24'] = len(temp) / feature['contact_amount']
        # invalid_contact_amount 无效联系人数量
        # imobile_contact_amount 通讯录中以639开头，并且总位数为12位的号码数量
        # distinct_contact_amount 通讯录所有号码去重后的个数
        # contact_area_code_cnt 取出手机号中的3位数区号，统计通讯录中区号去重个数
        # contact_area_code_2016_percent 取出手机号中3位数区号，统计2016年之前的去重手机号个数占全部去重手机号个数的比值
        # contact_area_code_phone_avg 取出手机号中3位数区号，计算平均每个区号对应多少个手机号（全部去重手机号数/全部去重区号数）
        # fixed_line_num  通讯录中固定号码数量
        # invalid_number_num2  通讯录中无效号码数量 通讯录号码数量-移动号码数量-固定号码数量-服务号码数量
        # service_num  通讯录中服务类号码数量
        # telsmart_num  通讯录中SMART号码数量
        # telglobe_num  通讯录中GLOBE号码数量
        # telsun_num  通讯录中SUN CELLULAR号码数量
        # telexpress_num  通讯录中EXPRESS号码数量 
        # telunknown_num  通讯录中其他类型号码数量 
        # telmobile_num_rate 通讯录中移动号码数量占比
        # telglobe_num_rate 通讯录中GLOBE号码数量占比
        # telsun_num_rate通讯录中SUN CELLULAR号码数量占比
        # telunknown_num_rate通讯录中其他类型号码数量 占比
        # familyname_num  通讯录中常见的名字数量
        # appname_num  通讯录中借贷app名字的数量
        # familyname_num_rate  通讯录中常见的名字数量占比
        # appname_num_rate  通讯录中借贷app名字的数量占比
        uc.deal_contact_num(contact_df[['phone', 'contact_name']], feature)
        # loaned_contact_amount 通讯录中的手机号在本马甲包有申请记录的手机号数量
        # loan_count_avg 通讯录手机号去重后，去重后通讯录对应的全部贷款次数的和(loaned_contact_amount)/总去重通讯录个数
        feature['loaned_contact_amount'] = contact_borrow['customer_mobile'].nunique()
        feature['loan_count_avg'] = feature['loaned_contact_amount'] / feature['distinct_contact_amount']
    
    # app_amount 用户当前申请抓取的app总数量
    feature['app_amount'] = len(app_df)
    if feature['app_amount'] > 0:
        app_df = app_df.merge(install_app_df, how='left', on='app_name')
        # loan_app_count app名字中，包含财务类app列表中的借贷类app数量
        temp = app_df[app_df['loan'] == 1]
        feature['loan_app_count'] = len(temp)
        # not_loan_app_count app名字中，包含财务类app列表中的非借贷类app数量
        temp = app_df[app_df['loan'] == 0]
        feature['not_loan_app_count'] = len(temp)
        # app_info_loan_low_eng5_cnt app名字中，包含财务类app列表中的借贷类app并且overdue_diff<-5的数量
        temp = app_df[(app_df['loan'] == 1) & (app_df['overdue_diff'] < -5)]
        feature['app_info_loan_low_eng5_cnt'] = len(temp)
        # app_info_loan_up_10_cnt app名字中，包含财务类app列表中的借贷类app并且overdue_diff>10的数量
        temp = app_df[(app_df['loan'] == 1) & (app_df['overdue_diff'] > 10)]
        feature['app_info_loan_up_10_cnt'] = len(temp)
        # app_info_loan_low_eng510_cnt app名字中，包含财务类app列表中的借贷类app并且overdue_diff在5-10之间的数量
        temp = app_df[(app_df['loan'] == 1) & (app_df['overdue_diff'] <= 10) & (app_df['overdue_diff'] >= 5)]
        feature['app_info_loan_low_eng510_cnt'] = len(temp)
        # app_info_loan_low_eng5_day060_cnt app名字中，包含财务类app列表中的借贷类app并且overdue_diff<-5的数量，安装时间在申请日期前0-60
        last_60 = record_time - datetime.timedelta(days=60)
        temp = app_df[(app_df['loan'] == 1) & (app_df['overdue_diff'] < -5) & (app_df['in_time'] >= last_60)]
        feature['app_info_loan_low_eng5_day060_cnt'] = len(temp)
        # app_info_loan_up_10_day060_cnt 贷款类app 安装逾期差+10%以上的app的个数 安装时间在申请日期前0-60
        temp = app_df[(app_df['loan'] == 1) & (app_df['overdue_diff'] > 10) & (app_df['in_time'] >= last_60)]
        feature['app_info_loan_up_10_day060_cnt'] = len(temp)
        # app_info_loan_low_day060_eng510_cnt 贷款类app 安装逾期差5-10%的app的个数 安装时间在申请日期前0-60
        temp = app_df[(app_df['loan'] == 1) & (app_df['overdue_diff'] <= 10) & (app_df['overdue_diff'] >= 5) & (app_df['in_time'] >= last_60)]
        feature['app_info_loan_low_day060_eng510_cnt'] = len(temp)
        # app_info_loan_low_eng5_up60_cnt 贷款类app 安装逾期差-5%以下的app的个数 安装时间在申请日期前60
        temp = app_df[(app_df['loan'] == 1) & (app_df['overdue_diff'] < -5) & (app_df['in_time'] < last_60)]
        feature['app_info_loan_low_eng5_up60_cnt'] = len(temp)
        # app_info_loan_up_10_up60_cnt 贷款类app 安装逾期差+10%以上的app的个数 安装时间在申请日期前60
        temp = app_df[(app_df['loan'] == 1) & (app_df['overdue_diff'] > 10) & (app_df['in_time'] < last_60)]
        feature['app_info_loan_up_10_up60_cnt'] = len(temp)
        # app_info_loan_low_up60_eng510_cnt 贷款类app 安装逾期差5-10%的app的个数 安装时间在申请日期前60
        temp = app_df[(app_df['loan'] == 1) & (app_df['overdue_diff'] <= 10) & (app_df['overdue_diff'] >= 5) & (app_df['in_time'] < last_60)]
        feature['app_info_loan_low_up60_eng510_cnt'] = len(temp)
        # app_info_not_loan_low_eng5_cnt app名字中，包含财务类app列表中的非借贷类app并且overdue_diff<-5的数量
        temp = app_df[(app_df['loan'] == 0) & (app_df['overdue_diff'] < -5)]
        feature['app_info_not_loan_low_eng5_cnt'] = len(temp)
        # app_info_not_loan_up_5_cnt app名字中，包含财务类app列表中的非借贷类app并且overdue_diff>5的数量
        temp = app_df[(app_df['loan'] == 0) & (app_df['overdue_diff'] > 5)]
        feature['app_info_not_loan_up_5_cnt'] = len(temp)
        # app type
        app_df = app_df.merge(google_play_df, how='left', on='app_id')
        feature['tool_app_count'] = uc.get_app_type_feature(app_df, key='tool_list')
        feature['education_app_count'] = uc.get_app_type_feature(app_df, key='education_list')
        feature['entertainment_app_count'] = uc.get_app_type_feature(app_df, key='entertainment_list')
        feature['photography_app_count'] = uc.get_app_type_feature(app_df, key='photography_list')
        feature['lifestyle_app_count'] = uc.get_app_type_feature(app_df, key='lifestyle_list')
        feature['musicaudio_app_count'] = uc.get_app_type_feature(app_df, key='musicaudio_list')
        feature['finance_app_count'] = uc.get_app_type_feature(app_df, key='finance_list')
        feature['health_app_count'] = uc.get_app_type_feature(app_df, key='health_list')
        feature['social_app_count'] = uc.get_app_type_feature(app_df, key='social_list')
        feature['travel_app_count'] = uc.get_app_type_feature(app_df, key='travel_list')
        feature['personalization_app_count'] = uc.get_app_type_feature(app_df, key='personalization_list')
    
    # memoryCardSize:存储卡总容量(B)
    # ramTotalSize:内存总容量(B)
    # ramUsableSize:内存可用容量(B)
    # memoryCardSizeUse:存储卡可用容量(B)
    # cpuType
    if len(content_df) > 0:
        content = json.loads(content_df['content'].item())
        if 'storage' in content.keys():
            storage = content['storage']
            memoryCardSize = storage.get('memoryCardSize')
            ramTotalSize = storage.get('ramTotalSize')
            ramUsableSize = storage.get('ramUsableSize')
            memoryCardSizeUse = storage.get('memoryCardSizeUse')
            feature['memoryCardSize'] = memoryCardSize if type(memoryCardSize) == int else (int(memoryCardSize) if type(memoryCardSize) == str and memoryCardSize.isdigit() else np.nan)
            feature['ramTotalSize'] = ramTotalSize if type(ramTotalSize) == int else (int(ramTotalSize) if type(ramTotalSize) == str and ramTotalSize.isdigit() else np.nan)
            feature['ramUsableSize'] = ramUsableSize if type(ramUsableSize) == int else (int(ramUsableSize) if type(ramUsableSize) == str and ramUsableSize.isdigit() else np.nan)
            feature['memoryCardSizeUse'] = memoryCardSizeUse if type(memoryCardSizeUse) == int else (int(memoryCardSizeUse) if type(memoryCardSizeUse) == str and memoryCardSizeUse.isdigit() else np.nan)
            # 手机除去系统应用使用了多少硬盘
            system_size = uc.get_system_size(params['system_version'])
            if system_size is not None and not np.isnan(feature['memoryCardSize']) and not np.isnan(feature['memoryCardSizeUse']):
                feature['userCardSizeUse'] = (feature['memoryCardSize'] / (1024*1024*1024)) - (feature['memoryCardSizeUse'] / (1024*1024*1024)) - system_size
        if 'hardware' in content.keys():
            hardware = content['hardware']
            feature['cpuType'] = hardware.get('cpuType')
    
    # 基本信息
    feature['monthly_income'] = basic_df['monthly_income'].iloc[0]
    feature['id_type'] = basic_df['id_type'].iloc[0]
    feature['customer_sex'] = basic_df['customer_sex'].iloc[0]
    feature['marital_status'] = basic_df['marital_status'].iloc[0]
    feature['edu_status'] = basic_df['edu_status'].iloc[0]
    feature['profession_type'] = basic_df['profession_type'].iloc[0]
    feature['child_count'] = basic_df['child_count'].iloc[0]
    feature['age'] = record_time.year - basic_df['customer_birthday'].iloc[0].year
    feature['register_application_days'] = (record_time - basic_df['create_time'].iloc[0]).days
    
    # 埋点特征
    if not operate_df.empty:
        operate_df.sort_values(by='operation_time', ascending=False, inplace=True)
        session_id = operate_df['session_id'].iloc[0]
        # 客户在本次提交申请之前，全部的点击行为触发的不同session_id的总和
        feature['total_fill_information_count'] = operate_df['session_id'].nunique()
        last_1 = record_time - datetime.timedelta(days=1)
        last_3 = record_time - datetime.timedelta(days=3)
        last_7 = record_time - datetime.timedelta(days=7)
        # 客户在本次提交申请的前3天至前1天之间，app_start出现次数，日期范围是闭区间
        temp = operate_df[(operate_df['operation_time'] >= last_3) & (operate_df['operation_time'] <= last_1) & (operate_df['action_type'] == 10)]
        feature['app_start_3days_ago_count'] = len(temp)
        # 客户在本次提交申请的前7天至前1天之间，app_start出现次数，日期范围是闭区间
        temp = operate_df[(operate_df['operation_time'] >= last_7) & (operate_df['operation_time'] <= last_1) & (operate_df['action_type'] == 10)]
        feature['app_start_7days_ago_count'] = len(temp)
        # 本次申请前APP页面总停留时间(min)
        # 本次申请前APP页面平均停留时间(min)
        # 本次申请前APP页面最小停留时间(min)
        temp = operate_df.groupby('session_id')['duration_seconds'].sum()
        temp = temp / 60
        feature['total_wait_time'] = temp.sum()
        feature['avg_wait_time'] = temp.mean()
        feature['min_wait_time'] = temp.min()
        # 客户在本次申请中，触发过的不同页面总和，本次session_id对应的用户本次不同page_code的总和
        temp = operate_df[operate_df['session_id'] == session_id]
        feature['unique_page_count'] = temp['page_code'].nunique()
        # 客户在本次申请中，触发过的不同action_type总和，本次session_id对应的用户本次不同action_type的总和
        feature['unique_action_type_count'] = temp['action_type'].nunique()
        # 客户本次申请中，总计停留时间。本次session_id对应的App_start到end的时间总和，转换为分钟数
        feature['apply_wait_time'] = (temp['operation_time'].max() - temp['operation_time'].min()).total_seconds() / 60
        # 本次申请page_code的duration_seconds相加的最大值
        temp = temp.groupby('page_code')['duration_seconds'].sum()
        feature['max_page_duration_seconds'] = temp.max()
        # 客户在本次提交申请之前，统计每个早先的申请订单的page_code=home的停留时间，加和，再转为分钟数
        # 客户在本次提交申请之前，统计每个早先的申请订单的page_code=home停留时间，取最小值，再转为分钟数
        # 客户在本次提交申请之前，统计每个早先的申请订单的page_code=home停留时间，取最大值，再转为分钟数
        # 客户在本次提交申请之前，统计每个早先的申请订单的page_code=home停留时间，取平均，再转为分钟数
        temp = operate_df[operate_df['page_code'] == 'HOME']
        temp = temp.groupby('session_id')['duration_seconds'].sum()
        temp = temp / 60
        feature['home_total_wait_time'] = temp.sum()
        feature['home_min_wait_time'] = temp.min()
        feature['home_max_wait_time'] = temp.max()
        feature['home_avg_wait_time'] = temp.mean()
        # 客户在本次提交申请之前，统计每个早先的申请订单的page_code=ID_INFO的停留时间，加和，再转为分钟数
        # 客户在本次提交申请之前，统计每个早先的申请订单的page_code=ID_INFO停留时间，取最小值，再转为分钟数
        # 客户在本次提交申请之前，统计每个早先的申请订单的page_code=ID_INFO停留时间，取最大值，再转为分钟数
        # 客户在本次提交申请之前，统计每个早先的申请订单的page_code=ID_INFO停留时间，取平均，再转为分钟数
        temp = operate_df[operate_df['page_code'] == 'ID_INFO']
        temp = temp.groupby('session_id')['duration_seconds'].sum()
        temp = temp / 60
        feature['id_info_total_wait_time'] = temp.sum()
        feature['id_info_min_wait_time'] = temp.min()
        feature['id_info_max_wait_time'] = temp.max()
        feature['id_info_avg_wait_time'] = temp.mean()
        # 客户在本次提交申请之前，统计每个早先的申请订单的page_code=CONFIRM的停留时间，加和，再转为分钟数
        # 客户在本次提交申请之前，统计每个早先的申请订单的page_code=CONFIRM停留时间，取最小值，再转为分钟数
        # 客户在本次提交申请之前，统计每个早先的申请订单的page_code=CONFIRM停留时间，取最大值，再转为分钟数
        # 客户在本次提交申请之前，统计每个早先的申请订单的page_code=CONFIRM停留时间，取平均，再转为分钟数
        temp = operate_df[operate_df['page_code'] == 'CONFIRM']
        temp = temp.groupby('session_id')['duration_seconds'].sum()
        temp = temp / 60
        feature['confirm_total_wait_time'] = temp.sum()
        feature['confirm_min_wait_time'] = temp.min()
        feature['confirm_max_wait_time'] = temp.max()
        feature['confirm_avg_wait_time'] = temp.mean()
        # 客户在本次提交申请之前，统计每个早先的申请订单的page_code=MINE的停留时间，加和，再转为分钟数
        # 客户在本次提交申请之前，统计每个早先的申请订单的page_code=MINE停留时间，取最小值，再转为分钟数
        # 客户在本次提交申请之前，统计每个早先的申请订单的page_code=MINE停留时间，取最大值，再转为分钟数
        # 客户在本次提交申请之前，统计每个早先的申请订单的page_code=MINE停留时间，取平均，再转为分钟数
        temp = operate_df[operate_df['page_code'] == 'MINE']
        temp = temp.groupby('session_id')['duration_seconds'].sum()
        temp = temp / 60
        feature['mime_total_wait_time'] = temp.sum()
        feature['mime_min_wait_time'] = temp.min()
        feature['mime_max_wait_time'] = temp.max()
        feature['mime_avg_wait_time'] = temp.mean()
        # 客户在本次提交申请之前，统计每个早先的申请订单的page_code=PAY的停留时间，加和，再转为分钟数
        # 客户在本次提交申请之前，统计每个早先的申请订单的page_code=PAY停留时间，取最小值，再转为分钟数
        # 客户在本次提交申请之前，统计每个早先的申请订单的page_code=PAY停留时间，取最大值，再转为分钟数
        # 客户在本次提交申请之前，统计每个早先的申请订单的page_code=PAY停留时间，取平均，再转为分钟数
        temp = operate_df[operate_df['page_code'] == 'PAY']
        temp = temp.groupby('session_id')['duration_seconds'].sum()
        temp = temp / 60
        feature['pay_total_wait_time'] = temp.sum()
        feature['pay_min_wait_time'] = temp.min()
        feature['pay_max_wait_time'] = temp.max()
        feature['pay_avg_wait_time'] = temp.mean()
        # 客户在本次提交申请之前，统计每个早先的申请订单的page_code=RESULT的停留时间，加和，再转为分钟数
        # 客户在本次提交申请之前，统计每个早先的申请订单的page_code=RESULT停留时间，取最小值，再转为分钟数
        # 客户在本次提交申请之前，统计每个早先的申请订单的page_code=RESULT停留时间，取最大值，再转为分钟数
        # 客户在本次提交申请之前，统计每个早先的申请订单的page_code=RESULT停留时间，取平均，再转为分钟数
        temp = operate_df[operate_df['page_code'] == 'RESULT']
        temp = temp.groupby('session_id')['duration_seconds'].sum()
        temp = temp / 60
        feature['result_total_wait_time'] = temp.sum()
        feature['result_min_wait_time'] = temp.min()
        feature['result_max_wait_time'] = temp.max()
        feature['result_avg_wait_time'] = temp.mean()
        # 客户在本次提交申请之前，统计每个早先的申请订单的page_code=SELECT_CONTACT的停留时间，加和，再转为分钟数
        # 客户在本次提交申请之前，统计每个早先的申请订单的page_code=SELECT_CONTACT停留时间，取最小值，再转为分钟数
        # 客户在本次提交申请之前，统计每个早先的申请订单的page_code=SELECT_CONTACT停留时间，取最大值，再转为分钟数
        # 客户在本次提交申请之前，统计每个早先的申请订单的page_code=SELECT_CONTACT停留时间，取平均，再转为分钟数
        temp = operate_df[operate_df['page_code'] == 'SELECT_CONTACT']
        temp = temp.groupby('session_id')['duration_seconds'].sum()
        temp = temp / 60
        feature['select_contact_total_wait_time'] = temp.sum()
        feature['select_contact_min_wait_time'] = temp.min()
        feature['select_contact_max_wait_time'] = temp.max()
        feature['select_contact_avg_wait_time'] = temp.mean()
        # 客户在本次提交申请之前，统计每个早先的申请订单的page_code=BASIC_INFO的停留时间，加和，再转为分钟数
        # 客户在本次提交申请之前，统计每个早先的申请订单的page_code=BASIC_INFO停留时间，取最小值，再转为分钟数
        # 客户在本次提交申请之前，统计每个早先的申请订单的page_code=BASIC_INFO停留时间，取最大值，再转为分钟数
        # 客户在本次提交申请之前，统计每个早先的申请订单的page_code=BASIC_INFO停留时间，取平均，再转为分钟数
        temp = operate_df[operate_df['page_code'] == 'BASIC_INFO']
        temp = temp.groupby('session_id')['duration_seconds'].sum()
        temp = temp / 60
        feature['basic_info_total_wait_time'] = temp.sum()
        feature['basic_info_min_wait_time'] = temp.min()
        feature['basic_info_max_wait_time'] = temp.max()
        feature['basic_info_avg_wait_time'] = temp.mean()
        # 客户在本次提交申请之前，统计每个早先的申请订单的page_code=PAY_AGREMENT的停留时间，加和，再转为分钟数
        # 客户在本次提交申请之前，统计每个早先的申请订单的page_code=PAY_AGREMENT停留时间，取最小值，再转为分钟数
        # 客户在本次提交申请之前，统计每个早先的申请订单的page_code=PAY_AGREMENT停留时间，取最大值，再转为分钟数
        # 客户在本次提交申请之前，统计每个早先的申请订单的page_code=PAY_AGREMENT停留时间，取平均，再转为分钟数
        temp = operate_df[operate_df['page_code'] == 'PAY_AGREMENT']
        temp = temp.groupby('session_id')['duration_seconds'].sum()
        temp = temp / 60
        feature['pay_agreement_total_wait_time'] = temp.sum()
        feature['pay_agreement_min_wait_time'] = temp.min()
        feature['pay_agreement_max_wait_time'] = temp.max()
        feature['pay_agreement_avg_wait_time'] = temp.mean()
    
    
    
    
    feature['borrow_id'] = params['borrow_id']
    feature['customer_id'] = params['customer_id']
    feature['device_id'] = params['device_id']
    feature['create_time'] = record_time
    uc.set_feature_result(feature)
    return feature

    







