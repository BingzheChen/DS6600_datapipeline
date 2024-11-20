import urllib.request
import csv
import codecs
import sys

# Specify the URL of the data
url = "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/United%20State/2023-10-01/2024-10-01?unitGroup=metric&include=days&key=KUCGGQQ58KYENK3BTD9GMXND7&contentType=csv"

# Specify the file name where you want to save the data
file_name = "weather_data.csv"

try: 
    # Download data
    ResultBytes = urllib.request.urlopen(url)
    
    # Parse the results as CSV
    CSVText = csv.reader(codecs.iterdecode(ResultBytes, 'utf-8'))
    
    # Write data to a CSV file
    with open(file_name, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        
        # Write each row to the CSV file
        for row in CSVText:
            writer.writerow(row)
    
    print(f"Data downloaded and saved to {file_name}")

except urllib.error.HTTPError as e:
    ErrorInfo = e.read().decode() 
    print('HTTPError code:', e.code, ErrorInfo)
    sys.exit()
except urllib.error.URLError as e:
    ErrorInfo = e.reason
    print('URLError:', ErrorInfo)
    sys.exit()
