# coding: utf-8

# # Saylent Programming Exercise
# 
# Authored by Chris Cotton, 08/14/2017
# 
# chris.j.cotton@me.com


import os
import sys
import re
import numpy as np
import pandas as pd
from copy import deepcopy
from fuzzywuzzy import fuzz
from fuzzywuzzy import process




def create_category_dictionary(df_merchant_map, category):
    category_dict = {}
    merchants = df_merchant_map["MerchantName"][df_merchant_map["MerchantTypeName"] == category].tolist()

    category_dict[merchants[0]] = [merchants[0]]

    n = 0

    for merchant in merchants:
        for key in category_dict.keys():
            if fuzz.ratio(key, merchant) > 75:
                category_dict[key].append(merchant)
            else:
                category_dict[merchant] = [merchant]
        n += 1
        if n % 100 == 0:
            print str(n) + " merchants complete."

    return category_dict



def merge_keys(category_dict):

    n = 0

    for key1 in category_dict.keys():
        for key2 in category_dict.keys():
            if key1 != key2:
                try:
                    if fuzz.ratio(key1, key2) > 75:
                        category_dict[key1].extend(category_dict[key2])
                        del category_dict[key2]
                    else:
                        pass
                except:
                    pass
        n += 1
        if n % 100 == 0:
            print str(n) + " merchants complete."

    return category_dict



def recursively_merge_a_category(df_merchant_map, category):
    print "Beginning initial dictionary creation."
    category_dict = create_category_dictionary(df_merchant_map, category)
    num_keys_current = len(category_dict)
    print "Dictionay creation complete."

    print "Beginning initial key merge."
    category_dict = merge_keys(category_dict)
    num_keys_new = len(category_dict)
    print "Initial key merge complete."

    # i = 0

    # while float(num_keys_new) / float(num_keys_current) < 0.75:
        # i += 1
        # print "Beginning key merge #" + str(i)
        # category_dict = merge_keys(category_dict)
        # print "Key merge #" + str(i) + " complete."

    category_dict = {v:k for (k,vs) in category_dict.items() for v in vs}

    return category_dict




def execute_master_merge_procedure(df_merchant_map, categories):
    master_dict = {}

    for category in categories:
        print "Beginning category: " + category
        category_dict = recursively_merge_a_category(df_merchant_map, category)
        master_dict.update(category_dict)
        print "Category " + category + " complete."

    return master_dict




def read_and_apply_regex(path_to_file):
    df = pd.read_csv(path_to_file)
    df["MerchantName"] = df["MerchantName"].map(lambda x: x.lstrip().rstrip())
    df["MerchantName"] = df["MerchantName"].map(lambda x: re.sub(r'[^\w\s]', "", x))
    df["MerchantName"] = df["MerchantName"].map(lambda x: re.sub(r'[\s\d+\s]', "", x))

    return df



def prep_for_procedure(path_to_file):
    df = read_and_apply_regex(path_to_file)
    merchant_type_transactions = df.groupby(by = "MerchantTypeName").count().sort_values(by = "MerchantName", ascending = False)
    categories = merchant_type_transactions.index.tolist()
    df_merchant_map = df.groupby(by = ["MerchantName","MerchantTypeName"]).count().reset_index()[["MerchantName","MerchantTypeName"]]

    return df_merchant_map, categories




def main(path_to_file):
    df_merchant_map, categories = prep_for_procedure(path_to_file)
    master_dict = execute_master_merge_procedure(df_merchant_map, categories)
    print "Master merge procedure complete."
    return master_dict




def show_me_the_money(input_path):
    master_dict = main(input_path)
    df_fuzzy = read_and_apply_regex(input_path)
    df_fuzzy["MerchantName"] = df_fuzzy["MerchantName"].map(lambda x: master_dict[x])
    print df_fuzzy.groupby(by = "MerchantName").count().sort_values(
    by = "AmountCompleted", ascending = False).head(10)
    print df_fuzzy.groupby(by = "MerchantName").sum().sort_values(by = "AmountCompleted", ascending = False).head(10)



show_me_the_money(sys.argv[1])



