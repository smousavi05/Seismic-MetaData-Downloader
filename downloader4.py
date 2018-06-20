#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun May 20 14:43:38 2018

@author: mostafamousavi
"""

import urllib
import urllib.request as request
import re, os
import shutil
import requests
import numpy as np
from glob import glob
import pickle
import collections
import obspy
import sqlite3 
from sqlite3 import Error
import pandas as pd
from math import sin, cos, sqrt, atan2, radians
import math
import collections

class NCEDC_PHASE():
     ''' This class finds out the availabel phase catalogs of North California Earthquake Data Center (NCEDC) and aoutomatically 
     downloads the catalogs '''
     
     def __init__(self, start_year, end_year, output_dir):
         self.root_url = "http://ncedc.org/ftp/pub/catalogs/ncss/hypoinverse/phase2k"
         self.start_year = start_year
         self.end_year = end_year
         self.output_dir = output_dir
         self.get_years()
         self.download()
     
     def get_years(self):
         html = urllib.request.urlopen(self.root_url).read().decode()
         pattern = re.compile("<a href=\"\d\d\d\d/\">", re.S)
         tmp_years = re.findall(pattern, html)
         years = [re.findall("\d\d\d\d", yr)[0] for yr in tmp_years]
         self.years = [yr for yr in years if int(yr) >= self.start_year and int(yr) <= self.end_year ]
         self.years_url = [self.root_url+"/"+yr for yr in years]
        
     def download(self):
         for year in self.years:
             data_dir = os.path.join(self.output_dir, year)
             if os.path.exists(data_dir): # delete the previous downloades 
                 shutil.rmtree(data_dir)
             if not os.path.exists(data_dir): # creates new directory 
                 os.makedirs(data_dir)
             for mon in range(12):
                 url = (self.root_url + "/%s/%s.%.2d.phase.Z" % (year, year, mon+1))
                 print("Downloading: "+url)
                 r = requests.get(url, allow_redirects=True)
                 open(("%s.%.2d.phase.Z" % (year, mon+1), 'wb').write(r.content)
 
 
class Merge_Catalog():
     def __init__(self, root_dir):
         self.get_files(root_dir)
         self.merge_files()
         
     def get_files(self, root_dir):
         years_dir = sorted(glob(root_dir + "*/"))
         files_dict = {}
         for yr in years_dir:
             files_dict[yr] = sorted(glob(yr + "*phase"))
         self.years = years_dir
         self.files = files_dict
     
     def merge_files(self):
         catlog = []
         for yr in tqdm(self.years):
             for fl in self.files[yr]:
                 with open(fl, 'r') as fp:
                     lines = fp.readlines()
                     catlog += lines
         self.catlog = catlog
     
     def write(self, fname):
         with open(fname, 'w') as fp:
             for line in self.catlog:
                 fp.write(line)
                 

# northern california's stations
NC_st_list ={}
fhand = urllib.request.urlopen("ftp://ehzftp.wr.usgs.gov/klein/hypfiles/calsta2000.loc")
for line in fhand:
     net = line.decode()[5:7]
     station = line.decode()[0:5]
     latd = line.decode()[30:32]
     latm = line.decode()[33:41]
     latdeci = float(latm)/60
     stlat = int(latd)+round(latdeci,5) 
     
     lond = line.decode()[41:45]
     lonm = line.decode()[45:53]
     londeci = float(lonm)/60
     stlon = (int(lond)+round(londeci,5))*-1
     
     stel = line.decode()[54:59]
     
     k = str(station.replace(" ", ""))+"."+str(net)
     print(k, [stlat, stlon, stel])
     if k not in NC_st_list.keys():
         NC_st_list[k] = [stlat, stlon, stel]
 
# file = open('./NC_st_list', 'wb')
# pickle.dump(NC_st_list, file)
# file.close() 
# =============================================================================



class Catalog_to_SQL():
     ''' This class reads the downloaded phase catalog of North California Data 
     Center, extract the key parameters such as event Id, event date, magnitude,
     magnitude type, station name, network name, channel name, first motion, 
     p picking weight, p arrival time, and s arrival time and build a SQL table
     (phaseCatSQL)of these parameters. '''
     
     #loading station info
     
     def __init__(self, fname):
         self.read_and_convert(fname)
         
     def read_and_convert(self, fname):
         
         ## CLEANING previous tabels
         try:
             os.remove("phaseCatSQL")
         except Exception as e:
             pass
                 
         conn = sqlite3.connect("phaseCatSQL")
         cur = conn.cursor()
                  
         try:
             cur.execute('''CREATE TABLE phaseCatSQL (event_ID INTEGER, 
                                                      orgin_time TEXT, 
                                                      magnitude NUMERIC, 
                                                      station TEXT, 
                                                      network TEXT, 
                                                      channel TEXT,
                                                      first_motion TEXT, 
                                                      P_arrival TEXT, 
                                                      P_weight INTEGER,
                                                      P_marker TEXT,
                                                      S_arrival TEXT,
                                                      S_weigth INTEGER,
                                                      S_marker TEXT,
                                                      event_lat NUMERIC,
                                                      event_lon NUMERIC,
                                                      event_depth NUMERIC,
                                                      station_lat NUMERIC, 
                                                      station_lon NUMERIC,
                                                      station_elv NUMERIC, 
                                                      azimuth NUMERIC,
                                                      back_azimuth NUMERIC,
                                                      distance_M NUMERIC,
                                                      distance_D NUMERIC)''')
         except Exception as e:
             pass
            
         def to_float(string):
             if string.strip() == '':
                 return 0
             else:
                 return float(string)
 
         def to_int(string):
             if string.strip() == '':
                 return 0
             else:
                 return int(string)
         
         with open(fname, "r") as F:
             fp = open('station_list_full', 'rb');
             sta = pickle.load(fp)
             lines = F.readlines()
             printcounter = 0             
             for i, l in enumerate(lines):               
                 # finds the header line containing event info
                 #print(len(l))
                 if len(l) >= 160:
                     cash = {}
                     printcounter += 1
                     #print("evnLine:  ", len(l))
                     evId = (l[130:146].strip())
                     #print("ID:    ",evId)
                     
                     # this part extract event info, if more accurate info was available on the web
                     # it will replace them by the online ones
                     orgT = l[0:4]+"-"+l[4:6]+"-"+l[6:8]+"T"+\
                     l[8:10]+":"+l[10:12]+":"+l[12:14]+"."+l[14:16];
                     magD = to_float((l[147:150]).strip())/100.0; 
                     evlat = l[16:18]+"."+l[19:23].replace(" ", "0");
                     evlon = "-"+l[23:26]+"."+l[27:31].replace(" ", "0");
                     evDpth = to_float(l[31:37].strip())/100.0;
                     #print("evlat: ", evlat," evlon: ", evlon, "magD: ", magD, "evDpth: ", evDpth)
                     if (printcounter == 1000):
                         print(orgT)
                         printcounter = 0

                 # this part extract picks info        
                 if len(l) <= 130 and len(l) >= 75:
                     #print("phase line", len(l))
                     ll = l.strip();
                     stName = re.findall('\w+', ll[0:5]); stName = str(stName[0]);
                     network = ll[5:7];
                     key = str(stName.replace(" ", ""))+"."+str(network);
                     channel = ll[9:13];
                     Pmarker = ll[13:15].replace(" ", "0");
                     first_motion = ll[15:16].replace(" ", "0");
                     Pweight = ll[16:17];
                     distance = to_float((ll[74:78]).strip())/10.0       
                     
                     if Pweight == "4": 
                         tp = -666;
                         Pmarker = 0;
                         
                     else:
                         hrsP = to_int(ll[25:27].replace(" ", "0"));
                         minP = to_int(ll[27:29].replace(" ", "0"));
                         secP = to_int(ll[30:32].replace(" ", "0"));
                                 
                         secP = int(secP);
                         minP = int(minP);
                         hrsP = int(hrsP);
                         if secP >= 60:
                             ds = secP//60;
                             rs = secP%60;
                             minP = minP + ds;
                             secP = rs;
                                     
                         if minP >= 60:
                             dm = minP//60;
                             rm = minP%60;
                             hrsP = hrsP + dm;
                             minP = rm;
                                                  
                         tp = (ll[17:21]+"-"+ll[21:23]+"-"+ll[23:25]+"T"+str(hrsP)+":"+
                                               str(minP)+":"+str(secP)+'.'+ll[32:34]).replace(" ", "0");
                                                    
                         try:
                             tp = obspy.UTCDateTime(tp)
                         except:
                             print("tp time error")
                             tp = 0;

                     
                     S = ll[47:48];                     
                     if S =='S':
                         Smarker = ll[46:48].replace(" ", "0");
                         Sweight = ll[49:50];                                
                         hrsS = to_int(ll[25:27].replace(" ", "0"));
                         minS = to_int(ll[27:29].replace(" ", "0"));
                         secS = to_int(ll[42:44].replace(" ", "0"));
                                 
                         secS = int(secS);
                         minS = int(minS);
                         hrsS = int(hrsS);
                         if secS >= 60:
                             ds = secS//60;
                             rs = secS%60;
                             minS = minS + ds;
                             secS = rs;
                                     
                         if minS >= 60:
                             dm = minS//60;
                             rm = minS%60;
                             hrsS = hrsS + dm;
                             minS = rm;
                                     
                         ts = (ll[17:21]+"-"+ll[21:23]+"-"+ll[23:25]+"T"+str(hrsS)+":"+
                                       str(minS)+":"+str(secS)+'.'+ll[44:46]).replace(" ", "0");
                                 # print("###ts:", ts)
                         try:
                             ts = obspy.UTCDateTime(ts);
                         except:
                             return
                         
                     else:
                         ts = 0;
                         Smarker = "_"
                         Sweight = 0;                                         
                           
# getting station info                      
                     if key in sta.keys():
                         stlat, stlon ,stelv = sta[key]
                             #print(stlat, stlon ,stelv)                          
                     else:
                         stlat = 0;
                         stlon = 0;
                         stelv = 0;
                         print("station is not in the list")
                         #print(stName, "tp: ", tp, "ts: ", ts)
                     if stlat > 0:
                         try:
                             distanceKm, azimuth, back_azimuth = obspy.geodetics.base.gps2dist_azimuth(
                                                                       float(evlat), 
                                                                       float(evlon),
                                                                       float(stlat), 
                                                                       float(stlon), 
                                                                       a=6378137.0, 
                                                                       f=0.0033528106647474805)
                             distanceKm = (distanceKm/1000);
                             distanceD = (distanceKm / 111);
                         except Exception:
                             R = 6373.0 # approximate radius of earth in km
                             lat1 = radians(float(stlat))
                             lon1 = radians(float(stlon))
                             lat2 = radians(float(evlat))
                             lon2 = radians(float(evlon))
                             dlon = lon2 - lon1
                             dlat = lat2 - lat1
                             a = math.sin(dlat / 2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2)**2
                             c = 2 * atan2(sqrt(a), sqrt(1 - a))
                             distanceKm = R * c; 
                             distanceD = (distanceKm / 111 );                                
                             x = math.sin(dlon)*math.cos(lat2)
                             y = math.cos(lat1)*math.sin(lat2)-(math.sin(lat1)
                                         * math.cos(lat2)*math.cos(dlon))
                             init_bearing = math.atan2(x, y)
                             initial_bearing = math.degrees(init_bearing)
                             back_azimuth = (initial_bearing + 360) % 360
                             azimuth = 0;
                     else:
                         distanceKm = 0;
                         distanceD = 0;
                         back_azimuth = 0;
                         azimuth = 0;
                         
                     cash[str(stName.replace(" ", "")+"."+str(channel))]=[int(evId),
                                                            str(orgT),
                                                            float(magD),
                                                            str(stName),
                                                            str(network),
                                                            str(channel),
                                                            str(first_motion),
                                                            str(tp),
                                                            int(Pweight),
                                                            str(Pmarker), 
                                                            str(ts),
                                                            int(Sweight),
                                                            str(Smarker),
                                                            float(evlat),
                                                            float(evlon),
                                                            float(evDpth),
                                                            float(stlat),
                                                            float(stlon),
                                                            float(stelv),
                                                            float(azimuth),
                                                            float(back_azimuth),
                                                            float(distanceKm),
                                                            float(distanceD)];
                     #print(cash[str(stName.replace(" ", ""))])        
                 if len(l) <= 75: # this part takes care of those records with tp and ts in different lines for the same station
                     #print(cash)
                     cash2 = {}
                     for k, v in cash.items():
                         k2 = k.split(".")
                         #print("new", v[7])
                         if k2[0] in cash2.keys():
                             #print(k2[0]+" is already there")
                             #print("listed_tp", cash2[k2[0]][7])
                             #print("listed_ts", cash2[k2[0]][10])
                             if cash2[k2[0]][7] == "-666":
                                 #print("replace tp")
                                 cash2[k2[0]][7] = v[7];
                                 cash2[k2[0]][8] = v[8];
                                 cash2[k2[0]][9] = v[9];
                             else:
                                 #print("replace ts")
                                 cash2[k2[0]][10] = v[10];
                                 cash2[k2[0]][11] = v[11];
                                 cash2[k2[0]][12] = v[12]; 
                             #print("new_listed_tp", cash2[k2[0]][7])
                             #print("new_listed_ts", cash2[k2[0]][10])
                         else:
                             #print("adding to the list")
                             cash2[k2[0]]=v
                     for ky, val in cash2.items(): 
                         cur.execute('''INSERT INTO phaseCatSQL VALUES 
                                      (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''', val);
                         conn.commit()                                                     
        
Catalog_to_SQL("NCEDC.txt") 

# =============================================================================
# 
# 
# conn = sqlite3.connect("phaseCatSQL")
# tbl = pd.read_sql_query("SELECT * FROM phaseCatSQL\
#                         WHERE evId == 55189" , conn);
#                         
# tpstr = pd.Series.to_string(tbl["tp"])
# print((tpstr))
# 
# tpCat = obspy.UTCDateTime(tpstr[5:]);
# itp = int((tpCat - trc_starttime)/(trc_endtime - trc_starttime) * tnpts);
# print((tpCat))
# 
# firstmostionCat = pd.Series.to_string(tbl["first_motion"]);
# firstmostionCat = firstmostionCat[5:];
# 
# pweightCat = pd.Series.to_string(tbl["pweight"]); 
# pweightCat = (pweightCat[5:]);
# tsstr = pd.Series.to_string(tbl["ts"])
# if tsstr[5:] != '0':
#     tsCat = obspy.UTCDateTime(tsstr[5:]);
#     itp = int((tsCat - trc_starttime)/(trc_endtime - trc_starttime) * tnpts);
# print(tpCat)
# 
# 
# print(tbl.loc[:, : ])
# 
# for ii in range(17119495):
#     if tbl.loc[ii, "tp"] != 0:
#         print(obspy.UTCDateTime(tbl.loc[ii, "tp"]))
#         
#         print((tbl.loc[ii, "tp"]))
#         
# =============================================================================


 