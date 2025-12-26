import requests
import pandas as pd
from datetime import datetime, timedelta
import time
import json

def fetch_wam_data(start_date, end_date):

    base_url = "https://markets.newyorkfed.org/api/soma/tsy/wam/all/asof/{}.json"
    
    
    start = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')
    
    all_data = []
    current_date = start
    
    total_days = (end - start).days + 1
    processed = 0
    
    print(f"Fetching data from {start_date} to {end_date}")
    print(f"Total days to process: {total_days}\n")
    
    while current_date <= end:
        date_str = current_date.strftime('%Y-%m-%d')
        url = base_url.format(date_str)
        
        try:
            response = requests.get(url, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                
                
                if 'soma' in data:
                    soma = data['soma']
                    if 'wam' in soma and 'asOfDate' in soma:
                        record = {
                            'asOfDate': soma['asOfDate'],
                            'wam': soma['wam']
                        }
                        all_data.append(record)
                        print(f" {date_str}: WAM = {soma['wam']}")
                    else:
                        print(f" {date_str}: Missing wam or asOfDate")
                else:
                    print(f" {date_str}: No soma data")
            
            elif response.status_code == 404:
                print(f" {date_str}: No data (404)")
            
            else:
                print(f" {date_str}: Error {response.status_code}")
        
        except requests.exceptions.Timeout:
            print(f" {date_str}: Timeout")
        except Exception as e:
            print(f" {date_str}: {str(e)}")
        
        processed += 1
        if processed % 100 == 0:
            print(f"\nProgress: {processed}/{total_days} days ({processed/total_days*100:.1f}%)\n")
        
        
        current_date += timedelta(days=1)
        
        
        time.sleep(0.1)
    
    
    if all_data:
        df = pd.DataFrame(all_data)
        print(f"Data collection complete!")
        print(f"Total records collected: {len(df)}")
        print(f"Date range: {df['asOfDate'].min()} to {df['asOfDate'].max()}")
        return df
    else:
        print("\nNo data was collected.")
        return pd.DataFrame()



if __name__ == "__main__":

    
    start_date = "2003-07-09"
    end_date = "2025-12-18"
    
    
    df = fetch_wam_data(start_date, end_date)
    
    
    if not df.empty:
        output_file = "ny_fed_wam_data_2003_2025.csv"
        df.to_csv(output_file, index=False)
        print(f"Data saved to: {output_file}")
        
        
        print("\nDataset Info:")
        print(f"Columns: {list(df.columns)}")
        print(f"\nFirst few rows:")
        print(df.head())
        
        
        unique_dates = df['asOfDate'].nunique()
        print(f"\nUnique dates with data: {unique_dates}")
    else:
        print("No data to save.")