import requests
import time
import json
import urllib
import urllib2
import sys
import datetime
import re
from collections import defaultdict
import pyodbc
#from bingpy import WebSearch
import tweepy
import gspread
import gspread
import logging
import timeit
import sys
start = timeit.default_timer()
time_start = datetime.datetime.now()
str_logging_time = str(datetime.datetime.now())
from oauth2client.service_account import ServiceAccountCredentials

#GOOGLE SHEET REFERENCE

scope = ['https://spreadsheets.google.com/feeds']
credentials = ServiceAccountCredentials.from_json_keyfile_name('ENTER PROJECT NAME HERE', scope)
gc = gspread.authorize(credentials)
wks = gc.open("BRPMEN_Bingboard").sheet1
wks.update_acell('A2', 'Running...')
wks.update_acell('B3', 'Will populate when process is finished')
wks.update_acell('B4', 'Will populate when process is finished')
wks.update_acell('B5', 'Will populate when process is finished')
wks.update_acell('B6', 'Will populate when process is finished')
wks.update_acell('B7', 'Will populate when process is finished')
wks.update_acell('B8', 'Will populate when process is finished')

wks.update_acell('D2', '')
wks.update_acell('E2', '')

#LISTS

twitter_ids_database = []
facebook_ids_database = []
instagram_ids_database = []

web = WebSearch("ENTER BING SEARCH TOKEN")
counter = "2000" #twitter max 2000

### GET TOTAL NUMBER OF FACEBOOK ON THE SYSTEM BEFORE THE PROCESS BEGAN

database_details = 'DRIVER={SQL Server};SERVER=ENTER SERVER NAME;DATABASE=ENTER DATABASE NAME;UID=ENTER USER ID;PWD=ENTER PASSWORD'
table_name = 'BRPMEN_AddressBookSocialProfiles'
cnxn = pyodbc.connect(database_details)
cursor = cnxn.cursor()
cursor.execute("SELECT matchkey FROM "+table_name+ " ORDER BY matchkey DESC")
rows = cursor.fetchall()
cnxn.close()

num_of_handles_before_running_script = 0

for item in rows:
    if "None" in str(item):
        pass
    else:
        num_of_handles_before_running_script = num_of_handles_before_running_script + 1

str_num_of_handles_before_running_script = str(num_of_handles_before_running_script)

#ANTI-DUPLICATOR
                        
cnxn = pyodbc.connect(database_details)
cursor = cnxn.cursor()
cursor.execute("select platform, matchkey from "+table_name+" WHERE matchkey IS NOT NULL")
address_book_locations_fetch = cursor.fetchall()

for address_book_locations in address_book_locations_fetch:
    platform = address_book_locations[0]
    address_book_location = address_book_locations[1]

    fb = "fb"
    tw = "tw"
    ig = "ig"
    
    if tw in str(platform):
        twitter_ids_database.append(address_book_location + " ")
    if fb in str(platform):
        facebook_ids_database.append(address_book_location + " ")
    if ig in str(platform):
        instagram_ids_database.append(address_book_location + " ")

#WIKIPEDIA
num_of_new_handles = 0
scope = ['https://spreadsheets.google.com/feeds']
credentials = ServiceAccountCredentials.from_json_keyfile_name('ENTER PROJECT NAME HERE', scope)
gc = gspread.authorize(credentials)
wks = gc.open("locations").sheet1
cell_list = wks.range('A1:A282')

for item in cell_list:
    data_loc = str(item)
    start_location = data_loc.find("{")
    end_location = data_loc.find("}")
    result_location_dirty = data_loc[start_location:end_location]
    city = result_location_dirty.replace("{","")
    start_country = data_loc.find("(")
    end_country = data_loc.find(")")
    country_dirty = data_loc[start_country:end_country]
    country = country_dirty.replace("(","")
    start_url = "https://en.wikipedia.org/w/api.php?action=query&titles="+city
    end_url = "&prop=coordinates&imlimit=20&format=json"
    wiki_space_remove = start_url.replace("\n","")
    wiki = wiki_space_remove + end_url
    opener_wiki = urllib.urlopen(wiki)
    
    for data_latlong in opener_wiki:
        json_latlong = json.loads(data_latlong)
        pageid_find = json_latlong["query"]["pages"]
        latitude_string = str(pageid_find)
        start_lat = str(latitude_string).find("u'")
        end_lat = str(latitude_string).find("':")
        latitude_finder_dirty = latitude_string[start_lat:end_lat]
        pageid = latitude_finder_dirty.replace("u'","")
        
        if 'coordinates' in json_latlong["query"]["pages"][pageid]:
            try:
                latitude = json_latlong["query"]["pages"][pageid]["coordinates"][0]["lat"]
            except KeyError:
                print KeyError
            try:
                longitude = json_latlong["query"]["pages"][pageid]["coordinates"][0]["lon"]
            except KeyError:
                print KeyError
                
            comma = ","

#COLLECTS ALL THE COORDINATES

            thirteen_longlat = str(latitude)+ comma + str(longitude)
            
            twomiles_vertical_up = 0.02802878
            twomiles_horizontal_right = 0.041546 
            
            north_longlat = latitude + twomiles_vertical_up
            eight_longlat = str(north_longlat) + comma + str(longitude)
            
            south_longlat = latitude - twomiles_vertical_up
            eighteen_longlat = str(south_longlat) + comma + str(longitude)
            
            east_longlat = longitude + twomiles_horizontal_right
            fourteen_longlat = str(latitude) + comma + str(east_longlat)

            northeast_longlat = latitude + twomiles_vertical_up
            nine_longlat = str(northeast_longlat) + comma + str(east_longlat)

            southeast_longlat = latitude - twomiles_vertical_up
            nineteen_longlat = str(southeast_longlat) + comma + str(east_longlat)

            west_longlat = longitude - twomiles_horizontal_right
            twelve_longlat = str(latitude) + comma + str(west_longlat)

            northwest_longlat = latitude + twomiles_vertical_up
            seven_longlat = str(northwest_longlat) + comma + str(west_longlat)

            southwest_longlat = latitude - twomiles_vertical_up
            seventeen_longlat = str(southwest_longlat) + comma + str(west_longlat)

            two_make_longlat = northwest_longlat + twomiles_vertical_up
            two_longlat = str(two_make_longlat) + comma + str(west_longlat)
            
            westwest_longlat = west_longlat - twomiles_horizontal_right
            one_longlat = str(two_make_longlat) + comma + str(westwest_longlat)

            three_make_longlat = north_longlat + twomiles_vertical_up
            three_longlat = str(three_make_longlat) + comma + str(longitude)

            four_make_longlat = northeast_longlat + twomiles_vertical_up
            four_longlot = str(four_make_longlat) + comma + str(east_longlat)

            easteast_longlat = east_longlat + twomiles_horizontal_right
            five_longlot = str(four_make_longlat) + comma + str(easteast_longlat)

            six_make_longlat = west_longlat - twomiles_horizontal_right
            six_longlat = str(northwest_longlat) + comma + str(six_make_longlat)

            eleven_make_longlat = six_make_longlat
            eleven_longlat = str(latitude) + comma + str(eleven_make_longlat)

            sixteen_make_longlat = latitude - twomiles_vertical_up
            sixteen_longlat = str(sixteen_make_longlat) + comma + str(six_make_longlat)

            twentyone_make_longlat = sixteen_make_longlat - twomiles_vertical_up
            twentyone_longlat = str(twentyone_make_longlat) + comma + str(six_make_longlat)

            twentytwo_make_longlat = six_make_longlat + twomiles_horizontal_right
            twentytwo_longlat = str(twentyone_make_longlat) + comma + str(twentytwo_make_longlat)

            twentytthree_make_longlat = twentytwo_make_longlat + twomiles_horizontal_right
            twentythree_longlat = str(twentyone_make_longlat) + comma + str(twentytthree_make_longlat)

            twentyfour_make_longlat = twentytthree_make_longlat + twomiles_horizontal_right
            twentyfour_longlat = str(twentyone_make_longlat) + comma + str(twentyfour_make_longlat)

            twentyfive_make_longlat = twentyfour_make_longlat + twomiles_horizontal_right
            twentyfive_longlat = str(twentyone_make_longlat) + comma + str(twentyfive_make_longlat)

            ten_make_longlat = four_make_longlat - twomiles_vertical_up
            ten_longlat = str(ten_make_longlat) + comma + str(easteast_longlat)

            fifteen_make_longlat = ten_make_longlat - twomiles_vertical_up
            fifteen_longlat = str(fifteen_make_longlat) + comma + str(easteast_longlat)

            twenty_make_longlat = fifteen_make_longlat - twomiles_vertical_up
            twenty_longlat = str(twenty_make_longlat) + comma + str(easteast_longlat)
            
            all_locations = [one_longlat,two_longlat,three_longlat,four_longlot,five_longlot,six_longlat,seven_longlat,eight_longlat,nine_longlat,ten_longlat,eleven_longlat,twelve_longlat,thirteen_longlat,fourteen_longlat,fifteen_longlat,sixteen_longlat,seventeen_longlat,eighteen_longlat,nineteen_longlat,twenty_longlat,twentyone_longlat,twentytwo_longlat,twentythree_longlat,twentyfour_longlat,twentyfive_longlat]
            original_date = datetime.datetime.now()
            
            limit = limit_num 
            client_id = "ENTER FOURSQUARE CLIENT ID"
            client_secret = "ENTER FOURSQUARE CLIENT SECRET"
            
            for location_execute in all_locations: #this should loop 25 times because there are 25 coordinates per city

                url = "https://api.foursquare.com/v2/venues/explore?ll="+location_execute+"&client_id=" + client_id + "&client_secret="+client_secret+"&v=20151212&section=drinks&food&limit=" + limit
                time.sleep(1)
                f = urllib.urlopen(url)
                
                for item in f:
                    try:
                        parsed_json = json.loads(item)
                        values = parsed_json["response"]["groups"][0]['items'][0]['venue']

                        name = values['name']
                        contact = values['contact']

#TWITTER              
                        if 'twitter' in contact:
                            time.sleep(1)
                            twitter = contact['twitter']
                        
                            if str(twitter) in str(twitter_ids_database):
                                print twitter, " ", "tw - we have it"
                                pass
                            if str(twitter) not in str(twitter_ids_database):
                                twitter_ids_database.append(twitter + " ") #this is here because the coordinates sometimes pull duplicate bars/restauarants
                                platform_t = "tw"
                                print "twitter", name, twitter, city, country, platform_t

                                cnxn = pyodbc.connect(database_details)
                                cursor = cnxn.cursor()
                                cursor.execute("insert into "+table_name+" (platform, venue_name, location, country, matchkey) values (?,?,?,?,?)",platform_t,name,city,country,twitter)
                                cnxn.commit()
                                cnxn.close()
                                num_of_new_handles = num_of_new_handles + 1
                                
                        if 'twitter' not in contact:
                            pass


#FACEBOOK
                        
                        if 'facebook' in contact:
                            time.sleep(1)
                            facebook = contact['facebook']
                            if str(facebook) in str(facebook_ids_database):
                                print facebook, " ", "fb - we have it"
                                pass
                            if str(facebook) not in str(facebook_ids_database):
                                facebook_ids_database.append(facebook + " ") #this is here because the coordinates sometimes pull duplicate bars/restauarants
                                platform_f = "fb"
                                print "facebook", name,facebook, city, country, platform_f

                                cnxn = pyodbc.connect(database_details)
                                cursor = cnxn.cursor()
                                cursor.execute("insert into "+table_name+" (platform, venue_name, location, country, matchkey) values (?,?,?,?,?)",platform_f,name,city,country,facebook)
                                cnxn.commit()
                                cnxn.close()
                                num_of_new_handles = num_of_new_handles + 1
                                
                        if 'facebook' not in contact:
                            pass
                    except Exception, e:
                        with open ('discovery_log.txt', 'w') as error_catch:
                            error_catch.write("Script finished but there was an error: " + str(e))
                            str_error = str(e)
                            wks.update_acell('D2', 'Process finished but there was an error')
                            wks.update_acell('E2', str_error)
                            
                            print "There was an error. Refer to discovery_log.txt for error message."
                        error_catch.close()
                        #sys.exit()

#INSTAGRAM & BING SEARCH
##
##            full_item_bing = name + " " + city + " " + '"instagram"'
##            try:
##                pages = web.search(full_item_bing, 20)
##
##                for page in pages:
##                    url_bing = page.url
##                    https_url_bing = url_bing.replace("http","https")
##                    httpss = https_url_bing.replace("httpss","https")
##
##                    if "https://www.instagram.com/" in httpss:
##                        pass 
##                        
##                        if httpss is "https://www.instagram.com/":
##                            pass
##                        else:
##                            time.sleep(1)
##                            insta_user = httpss.replace("https://www.instagram.com/","")
##                            instagram = insta_user.replace("/","")
##                            
##                            if str(instagram) in str(instagram_ids_database):
##                                print instagram, " ", "ig - we have it"
##                            if str(instagram) not in str(instagram_ids_database):
##                                instagram_ids_database.append(instagram + " ") #doing this because the search sometimes pull duplicate bars/restauarants
##                                platform_i = "ig"
##                                print "instagram", name,instagram, city, country, platform_i
##
##                                cnxn = pyodbc.connect(database_details)
##                                cursor = cnxn.cursor()
##                                cursor.execute("insert into "+table_name+" (platform, venue_name, location, country, matchkey) values (?,?,?,?,?)",platform_i,name,city,country,instagram)
##                                cnxn.commit()
##                                cnxn.close()                            
##                              
##                    if "https://www.instagram.com/" not in httpss:
##                        pass
##            except KeyError:
##                pass

### TIMER STOPS
stop = timeit.default_timer()
run_time = stop - start
m, s = divmod(run_time, 60)
h, m = divmod(m, 60)
hms_run_time = "%dh %02dm %02ds" % (h, m, s)

total_num_of_handles = num_of_handles_before_running_script + num_of_new_handles
str_total_num_of_handles = str(total_num_of_handles)
str_num_of_new_handles = str(num_of_new_handles)

str_time_end = str(datetime.datetime.now())

with open('discovery_log.txt', 'w') as success_message:
    success_message.write("Script finished successfully!" + "\n")
    success_message.write("Script started at " + str_logging_time + "\n")
    success_message.write("Script stopped at " + str_time_end + "\n")
    success_message.write("This script took " + hms_run_time + " to run" + "\n")
    success_message.write("The number of posts on the system before the script ran was " + str_num_of_handles_before_running_script + "\n")
    success_message.write("The number of posts on the system after the script has ran is " + str_total_num_of_handles + "\n")
    success_message.write(str_num_of_new_handles + " new posts were added")
    print "Script finished. Refer to discovery_log.txt for top line stats"
success_message.close()

scope = ['https://spreadsheets.google.com/feeds']
wks.update_acell('A2', 'Script finished!')
wks.update_acell('B3', str_logging_time)
wks.update_acell('B4', str_time_end)
wks.update_acell('B5', hms_run_time)
wks.update_acell('B6', str_num_of_handles_before_running_script)
wks.update_acell('B7', str_total_num_of_handles)
wks.update_acell('B8', str_num_of_new_handles)
