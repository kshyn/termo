import pandas as pd
import os
import numpy as np
print('current working directory', os.getcwd())
print('Enter the new working directory where the files are stored.')
# working files include flow, points, log. After execution, the script creates the results file in the folder
os.chdir(input())
print('enter name of log file')
#x = '10 sensor.csv'
x = input()
table = pd.read_csv(x, header=19)
#change type
table['Value'] = table['Value'].astype(float)
table['Unnamed: 3'] = table['Unnamed: 3'].astype(float)
table['Unnamed: 3'] = table['Unnamed: 3'].apply(lambda x: x*0.001)#multiply a column by a number to make a fraction.
table['Temp'] = table[['Value', 'Unnamed: 3']].sum(axis=1)#column addition
#further delete extra columns
table = table.drop(columns=['Unit', 'Value', 'Unnamed: 3'])
# based on the file name, calculate the sensor number
number_sensor = [int(s) for s in x.split() if s.isdigit()]
table['Sensor'] = number_sensor[0]
table['Date/Time'] = pd.to_datetime(table['Date/Time'], format='%d.%m.%y %H:%M:%S')
table['Weight'] = 1
#reading information about methane measurements from the file flow.csv and points.csv
points = pd.read_csv('points.csv', header=0)
points['Date/Time'] = pd.to_datetime(points['Date/Time'], format='%d.%m.%y')
flow = pd.read_csv('flow.csv', header=0)
flow['Start'] = pd.to_datetime(flow['Start'], format='%d.%m.%y %H:%M:%S')
flow['End'] = pd.to_datetime(flow['End'], format='%d.%m.%y %H:%M:%S')
#add empty columns to data frame
flow['Average temperature'] = np.nan
pd.to_numeric(flow['Average temperature'])
flow['Sensor'] = np.nan
pd.to_numeric(flow['Sensor'])
# declare variables, these lists will be used to store data
temp = []
depth = []
sens = []
# iterrows is not the best, but it's a simple solution. (to be replaced with a vector operation in the future)
for index, row in flow.iterrows():
     start_date = row['Start']
     end_date = row['End']
     #create a time mask for each sequence of measurements ("flow")
     mask = (table['Date/Time'] >= start_date) & (table['Date/Time'] <= end_date)
     a = table.loc[mask]
     # find adjacent temperature measurements not included in the mask
     before = numpy.argmax(mask) - 1
     after = (len(mask) - np.argmax(mask[::-1]))
     before = table.loc[[before]]
     after = table.loc[[after]]
     # assign them a weight and attach them to the data frame
     before['Weight'] = ((start_date - before.iloc[0]['Date/Time']).total_seconds())/180
     after['Weight'] = ((after.iloc[0]['Date/Time'] - end_date).total_seconds())/180
     a = pd.concat([before, a, after])
     # calculate the average temperature, taking into account the weight
     b = (round(np.average(a['Temp'], weights=a['Weight']), 3))
     # save to list
     temp.append(b)
    # keep a list of sensors in parallel
     sens.append(number_sensor[0])
#the cycle is over, so we save it from the list into a data frame
flow['Average temperature'] = temp
flow['Sensor'] = sens
# further similar loop to save depth from points.csv
for index, row in flow.iterrows():
     start_date = flow.at[(index), ('Start')]
     current_sensor = flow.at[(index), ('Sensor')]
     current_point = flow.at[(index), ('Point')]
     for index1, row1 in points.iterrows():
         if (current_sensor == points.at[(index1), ('Sensor')]) & (current_point == points.at[(index1), ('Point')]):
             c = points.at[(index1), ('Depth')]
             depth.append(c)
flow['Depth'] = depth
print(flow['Average temperature'])
flow.to_csv('result.csv')