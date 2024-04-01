#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Apr  2 09:33:50 2019

@author: lilnfang
"""

import pandas as pd
import lightgbm as lgb
from lightgbm import LGBMClassifier
from sklearn.feature_selection import RFE
import datetime
from hyperopt import fmin, tpe, hp, partial
import warnings
warnings.filterwarnings("ignore")


def feature_selection(data, params):
    X = data.drop(labels=['target','id_type','customer_sex','marital_status','edu_status','profession_type'], axis=1)
    y = data['target']
    estimator = LGBMClassifier(objective='binary', metric='auc')
    estimator.fit(X, y)
    importance_df = pd.DataFrame(estimator.feature_name_, columns=['feature_name'])
    importance_df['importance'] = estimator.feature_importances_
    valid_feature = importance_df[importance_df['importance'] > 0]['feature_name'].tolist()
    X = X[valid_feature]
    del estimator
    estimator = LGBMClassifier(objective='binary', metric='auc')
    selector = RFE(estimator, n_features_to_select=1, step=1)
    selector = selector.fit(X, y)
    rank_df = pd.DataFrame(selector.feature_names_in_, columns=['feature_name'])
    rank_df['rank'] = selector.ranking_
    rank_df.sort_values(by='rank', ascending=True, inplace=True)
    rank_df.to_csv('data/feature_rank.csv', index=False)
    drop_num = 0
    best_auc = 0.0
    best_feature_num = len(rank_df)
    categorical_feature_list = ['id_type','customer_sex','marital_status','edu_status','profession_type']
    while len(rank_df) - drop_num > 5:
        feature_num = len(rank_df) - drop_num
        temp = rank_df.iloc[0:feature_num]
        feature_list = categorical_feature_list + temp['feature_name'].tolist()
        auc = CV(data, feature_list, params)
        if auc < best_auc:
            best_auc = auc
            best_feature_num = feature_num
            best_feature_list = feature_list
        drop_num += 5
    return best_auc, best_feature_num, best_feature_list


def CV(data, feature_list, params):
    X = data[feature_list]
    y = data['target']
    train_data = lgb.Dataset(X, label=y)
    result = lgb.cv(params, train_data, metrics='auc', categorical_feature=['id_type','customer_sex','marital_status','edu_status','profession_type'], early_stopping_rounds=100)
    auc = result['auc-mean'][-1]
    return -auc


def parameter_tune(data, feature_list):
    
    space = {"max_depth": hp.randint("max_depth", 15),
             "num_iterations": hp.randint("num_iterations", 100, 1000),
             'learning_rate': hp.uniform('learning_rate', 1e-3, 5e-1),
             "bagging_fraction": hp.uniform("bagging_fraction", 0.1, 1),
             "num_leaves": hp.randint("num_leaves", 15, 127),
             "lambda_l2":hp.uniform('lambda_l2',0, 10),
             "lambda_l1":hp.uniform('lambda_l1',0, 10),
             "min_data_in_leaf": hp.randint("min_data_in_leaf", 100),
             "max_cat_threshold": hp.randint("max_cat_threshold", 8, 64),
             "cat_l2": hp.randint("cat_l2", 20),
             "cat_smooth": hp.randint("cat_smooth", 20),
             "max_cat_to_onehot": hp.randint("max_cat_to_onehot", 4, 16),
             "feature_fraction": hp.uniform("feature_fraction", 0.1, 1)}
    
    def lgb_tune(argsDict, data=data, feature_list=feature_list):
        params = {
        'task': 'train',
        'objective': 'binary',
        'boosting': 'gbdt',
        'num_threads': 4,
        'metric': 'auc',
        'verbose': -1,
        'seed': 666,
        'num_iterations': argsDict['num_iterations'],
        'learning_rate': argsDict['learning_rate'],
        'num_leaves': argsDict['num_leaves'],
        'max_depth': argsDict['max_depth'],
        'min_data_in_leaf': argsDict['min_data_in_leaf'],
        'bagging_fraction': argsDict['bagging_fraction'],
        'bagging_freq': 1,
        'feature_fraction': argsDict['feature_fraction'],
        'lambda_l1': argsDict['lambda_l1'],
        'lambda_l2': argsDict['lambda_l2'],
        'max_cat_threshold': argsDict['max_cat_threshold'],
        'cat_l2': argsDict['cat_l2'],
        'cat_smooth': argsDict['cat_smooth'],
        'max_cat_to_onehot': argsDict['max_cat_to_onehot'],
        'early_stopping_round': 100
        }
        return CV(data, feature_list, params)
    
    algo = partial(tpe.suggest, n_startup_jobs=1)
    best = fmin(lgb_tune, space, algo=algo, max_evals=300, pass_expr_memo_ctrl=None)
    return best
    

def model_train(data, feature_list, params):
    date = pd.to_datetime(datetime.datetime.now()).normalize().strftime('%Y%m%d')
    X = data[feature_list]
    y = data['target']
    train_data = lgb.Dataset(X, label=y)
    model = lgb.train(params, train_data, valid_sets=[train_data], valid_names=['train'], 
                      categorical_feature=['id_type','customer_sex','marital_status','edu_status','profession_type'])
    model.save_model(f'model/ios_risk_control_{date}.model')


def deal_low_var(data, thresh):
    test = data.isna().mean().reset_index()        
    test = test[test[0] < thresh]
    column_list = test['index'].tolist()
    test = data.var().reset_index()
    test = test[test[0] == 0]
    delete_list = test['index'].tolist()
    for column in delete_list:
        if column in column_list:
            column_list.remove(column)
    return data[column_list]
    
    
    
    
    
        
        


if __name__ == '__main__':
    
    params = {
    'task': 'train',
    'objective': 'binary',
    'boosting': 'gbdt',
    'num_threads': 4,
    'metric': 'auc',
    'verbose': -1,
    'seed': 666,
    'num_iterations': 100,
    'learning_rate': 0.1,
    'num_leaves': 31,
    'max_depth': -1,
    'min_data_in_leaf': 20,
    'bagging_fraction': 1.0,
    'bagging_freq': 1,
    'feature_fraction': 1.0,
    'lambda_l1': 0.0,
    'lambda_l2': 0.0,
    'max_cat_threshold': 32,
    'cat_l2': 10,
    'cat_smooth': 10,
    'max_cat_to_onehot': 4,
    'early_stopping_round': 100
    }
    
    data = pd.read_csv('data/ios_feature.csv')
    data = deal_low_var(data, 0.6)
    '''
    best_auc, best_feature_num, best_feature_list = feature_selection(data, params)
    print(f'best auc: {best_auc} best_feature_num: {best_feature_num}')
    best_params = parameter_tune(data, best_feature_list)
    for k,v in best_params.items():
        params[k] = v
    print(params)
    model_train(data, best_feature_list, params)
    '''
    