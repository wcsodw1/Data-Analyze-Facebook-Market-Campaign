# python facebook_campaign_metric.py

from collections import Counter
import datetime
import csv
import json
from re import L
import requests
import numpy as np
import pandas as pd
from datetime import datetime
import adgeek_permission as permission
import facebook_datacollector as collector
import facebook_business.adobjects.campaign as facebook_business_campaign


from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adset import AdSet
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.customaudience import CustomAudience


def function_list(account_id, campaign_id):
    permission.FacebookPermission(account_id).init_api()
    # print("hi")
    '''A.Create Custom Audience'''  # (move to function "custom_audience")
    # create_new_custom_audience(campaign_id)

    '''B.Add Custom Audience into FB List'''  # (move to function "custom_audience")
    # type_of_custom_audience = 'Insert'
    # add_custom_audience(23850533721340227, 23850610141000227, "Insert")
    # add_custom_audience(23850641169630227, 23851951373890227, "Replace")

    '''C.Lookalike'''
    # fan_page_lookalike(campaign_id)
    '''D.Campaign Metric'''
    # campaign_metric_adset_API(campaign_id, 3)
    campaign_metric_day_buildup(campaign_id, "2022-08-30", "2022-09-02")

    '''E.result'''
    # adset_InterestedAudienceID_Popularity(campaign_id, day_count)

# =================================== #


'''
def campaign_metric_hour_buildup(campaign_id):

    adset_ID_list = campaign_metric_table_buildup(campaign_id)
    print("adset_ID_list : ", adset_ID_list)

    account_id = get_account_id_by_campaign(campaign_id)
    my_access_token = permission.FacebookPermission(account_id).get_token()
    day_List = []
    for i in adset_ID_list:
        url_insight = permission.FACEBOOK_API_VERSION_URL + \
            str(i) + '/insights'
        # print("url_insight : ", url_insight)
        time_since = "2022-08-22"
        time_until = "2022-08-23"
        params = {
            "fields": "ctr, clicks, impressions, spend, actions",
            "breakdowns": "hourly_stats_aggregated_by_advertiser_time_zone",
            'time_range[since]': time_since,
            'time_range[until]': time_until,
        }
        headers = {
            'Authorization': "Bearer {}".format(my_access_token)}
        response = requests.request(
            "GET", url_insight, headers=headers, params=params)
        # print(response)
        html = json.loads(response.text)
        # print("html : ", html)

        if len(html) <= 1:
            print("This Adset's not running during [",
                  time_since, "] to [", time_until, "]")
            blank_dict = {}
            day_List.append(blank_dict)

        else:
            day_clicks = html['data'][0]['clicks']
            day_ctr = html['data'][0]['ctr']
            # day_reach = html['data'][0]['reach']
            day_spend = html['data'][0]['spend']
            day_actions = html['data'][1]['actions'][0]['value']
            day_impression = html['data'][0]['impressions']

            # print("day_spend : ", day_spend)
            # print("day_clicks : ", day_clicks)
            # print("day_ctr : ", day_ctr)
            # # print("day_reach : ", day_reach)
            # print("day_actions : ", day_actions)
            # print("day_impression : ", day_impression)

            day_dict = {'day_spend': day_spend, 'day_clicks': day_clicks, 'day_ctr': day_ctr,
                        'day_actions': day_actions, 'day_impression': day_impression}
            day_dict_copy = day_dict.copy()
            # day_dict_copy.update(zip__)
            day_List.append(day_dict_copy)
            # print("day_List :", day_List)
    # print("len :", len(adset_ID_list))
    # print("len(day_List) : ", len(day_List))
    # print("day_List : ", day_List)
    df_perday = pd.DataFrame(day_List)
    print("df_perday : ", df_perday)

    # df_perday.to_csv('0_per_hour_Dict.csv')
'''
'''興趣受眾資料建立program'''
'''
    # set the container
    Total_IT_List = []
    # all_list = list(range(0, 30))
    ID_List_Parameters = ["IT_ID1", "IT_ID2", "IT_ID3", "IT_ID4", "IT_ID5"]

        try:
            interests_audience = html['data'][i]['targeting']['flexible_spec'][0]['interests']
            intersted_num = len(interests_audience)

            ID_List_Values = []
            for j in range(intersted_num):
                interests_audience = html['data'][i]['targeting']['flexible_spec'][0]['interests']
                ID = interests_audience[j]['id']
                ID_List_Values.append(ID)
            # print("ID_List_Values : ", ID_List_Values)
            zip__ = dict(zip(ID_List_Parameters, ID_List_Values))
            zip__.copy().update(zip__)
            Total_IT_List.append(zip__)

        except:
            continue
    # print("Total_IT_List : ", Total_IT_List)
    '''
# =================================== #


def count_day_and_create_date_list(time_since, time_until):
    Start = datetime.strptime(time_since, '%Y-%m-%d')
    End = datetime.strptime(time_until, '%Y-%m-%d')
    day_count_ = str(End-Start)
    day_count = int(day_count_[:2])+1
    # print("Total Day :", day_count)

    day_list = []
    # 這裡 start 和 end 都有包括進去
    datelist = pd.date_range(start=time_since, end=time_until)
    by_day = pd.to_datetime(datelist)
    for i in by_day:
        pd2str = i.strftime('%Y-%m-%d %X')
        each_day = pd2str[:10]
        # print("each_day: ", each_day)
        day_list.append(each_day)

    # print("day_list : ", day_list)

    return day_count, day_list


def campaign_metric_day_buildup(campaign_id, time_since, time_until):
    account_id = get_account_id_by_campaign(campaign_id)
    my_access_token = permission.FacebookPermission(account_id).get_token()

    # count all day and create date list
    day_count, day_list = count_day_and_create_date_list(
        time_since, time_until)

    df_Adset_API_list, Adset_API_data, Adset_ID_list, List_IT_Aud_ID = campaign_metric_adset_API(
        campaign_id, day_count)

    df_IT_Aud_API_Loop_data = Intereste_Audience_Size(
        campaign_id, day_count, Adset_API_data, List_IT_Aud_ID)
    # print("df_IT_Aud_API_Loop_data : \n", df_IT_Aud_API_Loop_data)

    per_day_list = []
    day_new_ID_List = []

    # create adsetID_date parameter for "key" on merge
    for i in range(len(Adset_ID_list)):
        for j in range(len(day_list)):
            new_ID = Adset_ID_list[i]+"_"+day_list[j]
            day_dict = {'adsetID_date': new_ID}
            day_dict_copy = day_dict.copy()
            day_new_ID_List.append(day_dict_copy)
    df_day_new_ID_List = pd.DataFrame(day_new_ID_List)  # turn dataframe

    # Grab "perday" data from Insights API
    for i in Adset_ID_list:
        url_insight = permission.FACEBOOK_API_VERSION_URL + \
            str(i) + '/insights'
        params = {
            "fields": "ctr, clicks, impressions, spend, actions, reach",
            'time_range[since]': time_since,
            'time_range[until]': time_until,
            'time_increment': 1,
        }
        headers = {
            'Authorization': "Bearer {}".format(my_access_token)}
        response = requests.request(
            "GET", url_insight, headers=headers, params=params)
        Adset_info = json.loads(response.text)

        # Built per-day data(list)
        # grab (day_clicks, day_ctr, day_reach, day_spend, day_actions, day_impression, date_start, adsetID_date)
        for j in range(len(day_list)):
            try:
                day_clicks = Adset_info['data'][j]['clicks']
                day_ctr = Adset_info['data'][j]['ctr']
                day_reach = Adset_info['data'][j]['reach']
                day_spend = Adset_info['data'][j]['spend']
                day_actions = Adset_info['data'][j]['actions'][0]['value']
                day_impression = Adset_info['data'][j]['impressions']
                date_start = Adset_info['data'][j]['date_start']
                adsetID_date = i+"_"+date_start

                day_dict = {'adsetID_date': adsetID_date, 'date': date_start, 'day_ctr': day_ctr,
                            'day_clicks': day_clicks, 'day_reach': day_reach, 'day_spend': day_spend,
                            'day_actions': day_actions, 'day_impression': day_impression}
                day_dict_copy = day_dict.copy()
                per_day_list.append(day_dict_copy)

            except:
                continue

    # 1.Turn to dataframe  2.concat dataframe  3.merge 4.Save to (.csv)
    df_per_day_list = pd.DataFrame(per_day_list)
    df_concat_newID_and_AdsetAPI_Info = pd.concat(
        [df_day_new_ID_List, df_Adset_API_list], axis=1)
    # df_concat_newID_and_AdsetAPI_Info.to_csv(
    #     '0_df_concat_newID_and_AdsetAPI_Info.csv')
    df = pd.merge(df_concat_newID_and_AdsetAPI_Info,
                  df_per_day_list, on="adsetID_date", how="outer")

    df_Final = pd.concat(
        [df, df_IT_Aud_API_Loop_data], axis=1)
    print("df_Final\n", df_Final)

    df_Final.to_csv(
        '0_df_Final.csv')
    # df_final =  pd.merge(df_concat_newID_and_AdsetAPI_Info,
    #               df_per_day_list, on="adsetID_date", how="outer")

    print("Campaign-Metric Table built up Complete!")


def campaign_metric_adset_API(campaign_id, day_count):

    # API Setting :
    account_id = get_account_id_by_campaign(campaign_id)
    my_access_token = permission.FacebookPermission(account_id).get_token()
    url = permission.FACEBOOK_API_VERSION_URL + \
        str(campaign_id) + '/adsets'
    params = {
        "fields": "id,name,targeting,insights{ctr,clicks,impressions,reach,spend,actions,adset_id}"}
    headers = {
        'Authorization': "Bearer {}".format(my_access_token)}
    response = requests.request(
        "GET", url, headers=headers, params=params)
    html = json.loads(response.text)

    # set the container
    dict_blank = {}
    IT_Aud_list_5 = {}
    Adset_ID_list = []
    adset_len_list = []
    List_IT_Aud_ID = []
    Adset_API_Loop_data = []
    ID_List_Parameters = ["IT_ID1",
                          "IT_ID2", "IT_ID3"]  # "IT_ID4", "IT_ID5"
    for i in range(30):
        try:
            Adset_id = html['data'][i]['id']
            adset_len_list.append(Adset_id)
        except:
            continue
    adset_len = len(adset_len_list)

    # 當天資料沒有insight,因此迴圈從1開始 :
    for i in range(1, adset_len):

        # Adset_id, Name, impressions, Spend, interests_audience
        Adset_id = html['data'][i]['id']
        Adset_ID_list.append(Adset_id)
        Name = html['data'][i]['name']
        print("Adset Name[", i, "] :", Name)

        # 其他資訊 : 需要時可取用
        # ctr = html['data'][i]['insights']['data'][0]['ctr']
        # clicks = html['data'][i]['insights']['data'][0]['clicks']
        # reach = html['data'][i]['insights']['data'][0]['reach']
        # actions = html['data'][i]['insights']['data'][0]['actions'][0]['value']
        # CPM = (int(Spend)/int(impressions))*1000
        # impressions = html['data'][i]['insights']['data'][0]['impressions']
        # Spend = html['data'][i]['insights']['data'][0]['spend']

        # Interested_Audience Information :
        IT_Aud = html['data'][i]['targeting']['flexible_spec'][0]['interests']
        ID_List_Values = []
        for j in range(len(IT_Aud)):
            interests_audience = html['data'][i]['targeting']['flexible_spec'][0]['interests']
            ID = interests_audience[j]['id']
            ID_List_Values.append(ID)
            IT_Aud_list_5 = ID_List_Values[:5]

        zip_IT_Aud_list_with_Parameter = dict(
            zip(ID_List_Parameters, IT_Aud_list_5))
        dict_blank_copy = dict_blank.copy()
        dict_blank_copy.update(zip_IT_Aud_list_with_Parameter)
        List_IT_Aud_ID.append(dict_blank_copy)
        df_IT_Aud_ID = pd.DataFrame(List_IT_Aud_ID)
        df_IT_Aud_ID.to_csv('0_df_IT_Aud_ID.csv')  # Save to csv

        Final_Dictionary = {'Campaign_ID': campaign_id,
                            'Adset_ID': Adset_id, 'Adset_Name': Name}
        # Final_Dictionary = {'Campaign_ID': campaign_id, 'Adset_ID': Adset_id, 'Adset_Name': Name, 'ctr': ctr, 'clicks': clicks, 'impressions': impressions, 'reach': reach, 'Spend': Spend, 'actions': actions,'CPM': CPM}
        Final_Dictionary_copy = Final_Dictionary.copy()
        Final_Dictionary_copy.update(zip_IT_Aud_list_with_Parameter)
        print("Final_Dictionary_copy\n", Final_Dictionary_copy)

        for x in range(day_count):
            Adset_API_Loop_data.append(Final_Dictionary_copy)
        continue
    print("Adset_API_Loop_data", Adset_API_Loop_data)
    print("len Adset_API_Loop_data : ", len(Adset_API_Loop_data))

    print("=========================================")

    df_Adset_API_Loop_data = pd.DataFrame(Adset_API_Loop_data)
    # print(Adset_API_data)
    df_Adset_API_Loop_data.to_csv(
        'df_Adset_API_Loop_data.csv')
    return df_Adset_API_Loop_data, Adset_API_Loop_data, Adset_ID_list, List_IT_Aud_ID


def Intereste_Audience_Size(campaign_id, day_count, Adset_API_data, List_IT_Aud_ID):
    account_id = get_account_id_by_campaign(campaign_id)
    my_access_token = permission.FacebookPermission(account_id).get_token()

    df_Adset_API_data = pd.DataFrame(Adset_API_data)
    print("df_Adset_API_data \n", df_Adset_API_data)

    IT_Aud_parameter = ["IT_Aud_ID1_up_bound",
                        "IT_Aud_ID2_up_bound", "IT_Aud_ID3_up_bound"]  # "IT_Aud_ID4_up_bound", "IT_Aud_ID5_up_bound"
    List_parameter_Upbound = []
    IT_Aud_API_Loop_data = []

    for x in range(len(List_IT_Aud_ID)):
        # IT_all = List_IT_Aud_ID[x]
        IT_ID1 = List_IT_Aud_ID[x]['IT_ID1']
        IT_ID2 = List_IT_Aud_ID[x]['IT_ID2']
        IT_ID3 = List_IT_Aud_ID[x]['IT_ID3']
        ID_List = [IT_ID1, IT_ID2, IT_ID3]
        # print("ID List:", ID_List)
        # try:
        #     IT_ID4 = List_IT_Aud_ID[x]['IT_ID4']
        #     IT_ID5 = List_IT_Aud_ID[x]['IT_ID5']
        #     # print("IT_ID4 : ", IT_ID4)
        #     # print("IT_ID5 : ", IT_ID5)
        #     ID_List.append(IT_ID4)
        #     ID_List.append(IT_ID5)

        # except:
        #     continue
        # print("ID List > 3:", ID_List)
        List_Up_bound = []
        for i in range(len(ID_List)):
            url_interested_audience = permission.FACEBOOK_API_VERSION_URL + \
                str(ID_List[i]) + '/'
            params = {
                "fields": "name,audience_size_upper_bound",
            }
            headers = {
                'Authorization': "Bearer {}".format(my_access_token)}
            response = requests.request(
                "GET", url_interested_audience, headers=headers, params=params)
            IT_Aud_info = json.loads(response.text)
            name = IT_Aud_info['name']
            # print("name : ", name)
            audience_size_upper_bound = IT_Aud_info['audience_size_upper_bound']

            List_Up_bound.append(audience_size_upper_bound)
        zip_parameter_with_Up_bound = dict(
            zip(IT_Aud_parameter, List_Up_bound))
        zip_parameter_with_Up_bound_copy = zip_parameter_with_Up_bound.copy()
        zip_parameter_with_Up_bound_copy.update(zip_parameter_with_Up_bound)
        List_parameter_Upbound.append(zip_parameter_with_Up_bound_copy)

        for x in range(day_count):
            IT_Aud_API_Loop_data.append(zip_parameter_with_Up_bound_copy)
            continue
    df_IT_Aud_API_Loop_data = pd.DataFrame(IT_Aud_API_Loop_data)

    return df_IT_Aud_API_Loop_data

#=======================================================================================#


def add_custom_audience(target_adset_id, src_custom_audience, type_of_custom_audience):
    # target_adset_id : The adset(id) we gonna insert new  "custom_audience"(自訂受眾)
    # src_custom_audience : The "custom_audience"(自訂受眾) that we gonna add to our adset (ex:23851951373890227)
    ''' Test Source(src) Custom Audience '''
    # FB Fan Page Like : 23851951373890227
    # 類似廣告受眾 (1%) - 上線名單_3萬以上 : 23850605554510227
    # 類似廣告受眾 (3% to 5%) - 上線名單_3萬以上 : 23850610141000227
    # Testing FanPage Like : 23851960814320227

    # 1.get response by "target_adset_id"
    fb_target_adset_id = AdSet(target_adset_id)
    response = fb_target_adset_id.api_get(['campaign_id', 'targeting'])
    campaign_id = response['campaign_id']
    print("campaign_id :", campaign_id)
    targeting = response['targeting']
    print("Before targeting", targeting)
    update_list = [{'id': str(src_custom_audience)}]

    # Decide whether replace or insert :
    # type_of_custom_audience = "Insert"

    # 2.Replace/Insert Customer Audience :
    if type_of_custom_audience == "Replace":
        # 2.A DELETE all the custom_audiences from the list, and new the custom_audience
        print("targeting['custom_audiences'] : ",
              targeting['custom_audiences'])
        del targeting['custom_audiences']
        targeting['custom_audiences'] = []
        targeting['custom_audiences'].extend(update_list)
        fb_target_adset_id.api_update(
            params={'targeting : ': targeting})
        # print("fb_target_adset_id :", fb_target_adset_id)  # Check "custom_audiences" insert or not
        print("Replaced the Customer_Audience Updated")

    elif type_of_custom_audience == "Insert":
        # 2.B extend new custom_audiences to the list
        # Decision custom_audiences in targeting or not
        if "custom_audiences" not in targeting:
            # print("targeting['custom_audiences']", targeting['custom_audiences'])
            targeting['custom_audiences'] = []
            targeting['custom_audiences'].extend(update_list)
            # print("target : ", targeting['custom_audiences'])
            fb_target_adset_id.api_update(
                params={'targeting : ': targeting['custom_audiences']})
            print("fb_target_adset_id :", fb_target_adset_id)
            print("First Customer_Audience Updated")
        else:
            targeting['custom_audiences'].extend(update_list)
            fb_target_adset_id.api_update(
                params={'targeting : ': targeting['custom_audiences']})
            print("fb_target_adset_id :", fb_target_adset_id)
            print("More Customer_Audience Updated")
    else:
        print("Type_of_custom_audience :", type_of_custom_audience)


def create_new_custom_audience(campaign_id):
    print("======== create new Custom Audience ========")
    account_id = get_account_id_by_campaign(campaign_id)
    print("account_id :", account_id)
    print("campaign_id :", campaign_id)

    my_access_token = permission.FacebookPermission(account_id).get_token()

    Total_account_id = "act_" + account_id
    FacebookAdsApi.init(access_token=my_access_token)
    fields = []

    """ 1.粉專互動(Fan-Page) """
    # 1.粉專互動(Fan-Page) :
    # - a.page_engaged : CustomAudience id: "23851972615700227"
    # - b.page_visited : CustomAudience id: "23851972620900227"
    # - c.page_liked : CustomAudience id: "23851972631460227"
    # - d.page_messaged : CustomAudience id: "23851972633690227"
    # - e.page_cta_clicked :　CustomAudience id: "23851972634730227"
    # - f.page_or_post_save　: CustomAudience id: "23851972640930227"
    # - g.page_post_interaction : CustomAudience id: "23851972651150227"

    # params = {
    #     'name': '8/17 粉專互動(Fan-Page) - page_post_interaction --DavidLin',
    #     'rule': {'inclusions': {'operator': 'or', 'rules': [{'event_sources': [{'id': '103546861252232', 'type': 'page'}], 'retention_seconds': 0,
    #                                                          'filter': {'operator': 'and', 'filters': [{'field': 'event', 'operator': 'eq', 'value': 'page_post_interaction'}]}}]}},
    #     'prefill': '1',
    # }

    """ 2.名單型廣告(Stay-list) """
    # 2.名單型廣告 :
    # - a.lead_generation_submitted : CustomAudience id: "23851973106610227"
    # - b.lead_generation_dropoff : CustomAudience id: "23851973114630227"
    # - c.lead_generation_opened : CustomAudience id: "23851973149880227"

    # params = {
    #     'name': '8/17 名單型廣告(Stay-list) - lead_generation_opened --DavidLin',
    #     'rule': {'inclusions': {'operator': 'or', 'rules': [{'event_sources': [{'id': '349621876662601', 'type': 'lead'}], 'retention_seconds': 0,
    #                                                          'filter': {'operator': 'and', 'filters': [{'field': 'event', 'operator': 'eq', 'value': 'lead_generation_opened'}]}}]}},
    #     'prefill': '1',
    # }

    """ 3.Instagram 商業簡介(IG互動) """
    # 3.Instagram 商業簡介 :
    # - a.ig_business_profile_all : CustomAudience id: "23851972947450227"
    # - b.ig_business_profile_engaged : CustomAudience id: "23851972966120227"
    # - c.ig_user_messaged_business : CustomAudience id: "23851972981740227"
    # - d.ig_business_profile_visit :  CustomAudience id: "23851972987150227"
    # - e.ig_business_profile_ad_saved : CustomAudience id:""

    # params = {
    #     'name': '8/17 Instagram 商業簡介(IG互動)-ig_business_profile_ad_saved  --DavidLin',
    #     'rule': {'inclusions': {'operator': 'or', 'rules': [{'event_sources': [{'id': '17841448130959023', 'type': 'shopping_ig'}], 'retention_seconds': 31536000,
    #                                                          'filter': {'operator': 'and', 'filters': [{'field': 'event', 'operator': 'eq', 'value': 'ig_business_profile_ad_saved'}]}}]}},
    #     'prefill': '1',
    # }

    # print(AdAccount(Total_account_id).create_custom_audience(
    #     fields=fields,
    #     params=params,
    # ))
    # print("Params of New Custome Audience :", params)

# In[4]:


def fan_page_lookalike(campaign_id):
    print("======== create fan_page_lookalike ========")
    account_id = get_account_id_by_campaign(campaign_id)
    Total_account_id = "act_" + account_id
    my_access_token = permission.FacebookPermission(account_id).get_token()
    page_id = 103546861252232
    print("account_id : ", account_id)
    print("my_access_token :", my_access_token)
    print("Total_account_id : ", Total_account_id)
    print("campaign_id : ", campaign_id)  # 23851882559020227
    # print("lookalike : ", lookalike)

    """
    'page_id': '103546861252232',
    'origin_ids': '23851882559020227',
    'conversion_type': 'page_like',
    """

    '''Adding new lookalike audience'''
    lookalike = CustomAudience(parent_id=Total_account_id)
    print("lookalike : ", lookalike)
    lookalike.update({
        CustomAudience.Field.name: '8/18 Test page_like location_spec',
        CustomAudience.Field.subtype: CustomAudience.Subtype.lookalike,
        CustomAudience.Field.lookalike_spec: {
            'ratio': 0.04,
            'country': 'TW',
            'page_id': '103546861252232',
            'conversion_type': 'page_like',
            'allow_international_seeds': True
        },
    })

    '''Build new lookalike audience'''
    lookalike = CustomAudience(parent_id=Total_account_id)
    lookalike.update({
        CustomAudience.Field.name: '8/18 Test Build Lookalike Customer Audience - page_like ',
        CustomAudience.Field.subtype: CustomAudience.Subtype.lookalike,
        CustomAudience.Field.origin_audience_id: '23851972631460227',
        CustomAudience.Field.lookalike_spec: {
            'ratio': 0.19,
            'country': 'TW',
            'type': 'similarity',
            'origin_ids': '23851882559020227',
            'allow_international_seeds': True,
        },
    })
    lookalike.remote_create()
    print("lookalike : ", lookalike)


def get_account_id_by_campaign(campaign_id):
    this_campaign = facebook_business_campaign.Campaign(
        campaign_id).api_get(fields=["account_id"])
    account_id = this_campaign.get('account_id')
    return account_id


def main():
    # account_id = "1033361086872688"
    # campaign_id = "23851882559020227"
    function_list(1033361086872688, 23851882559020227)


if __name__ == "__main__":
    main()
