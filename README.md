# seismic metadata preparation (with application to North California Earthquake Data Center)

This code search and download metadata for siesmic phases and associated station information. 
Then it decode the hypoinverse format and write it as a SQL database. 
The database will next be used to match with waveform data. 
The database can easily be converted to Panda dataframe for exploring the data. 

# See the notebook for instructions
![Downloading phase arrival time and event information](F1.png)
![Downloading station information](F2.png)
![Decoding the data and building a SQL database](F3.png)
![Resulted table](F4.png)
![Statistics of metadata](F5.png)
![Statistics of metadata](F6.png)
