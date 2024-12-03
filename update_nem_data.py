import os
import pandas as pd
from datetime import datetime, timedelta
import requests
import time
from pymongo import MongoClient
from pymongo.errors import BulkWriteError

def get_latest_date_from_mongodb(collection):
    """Get the most recent date from MongoDB"""
    latest_record = collection.find_one(
        filter={},
        sort=[("SETTLEMENTDATE", -1)]
    )
    if latest_record:
        return datetime.strptime(latest_record['SETTLEMENTDATE'], '%Y-%m-%d %H:%M:%S')
    return None

def process_aemo_data(url, region, yearmonth, headers):
    """Process AEMO data directly from URL"""
    try:
        filename = f'PRICE_AND_DEMAND_{yearmonth}_{region}.csv'
        url = f'{url}/{filename}'
        
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        df = pd.read_csv(
            pd.io.common.StringIO(response.content.decode('utf-8')),
            dtype={
                'SETTLEMENTDATE': str,
                'REGION': str,
                'RRP': float,
                'TOTALDEMAND': float,
                'PERIODTYPE': str
            }
        )
        
        df['SETTLEMENTDATE'] = pd.to_datetime(df['SETTLEMENTDATE'], format='%Y/%m/%d %H:%M:%S')
        return df.drop_duplicates()
    
    except Exception as e:
        print(f'Error processing data for {region} {yearmonth}: {str(e)}')
        return None

def update_nem_prices(client, start_date=None, regions=None):
    """Update NEM prices database with only new data"""
    if regions is None:
        regions = ['NSW1', 'QLD1', 'VIC1', 'SA1']
    
    nem_db = client.NEM_Prices
    price_demand = nem_db.price_demand
    
    try:
        price_demand.drop_index("SETTLEMENTDATE_1_REGION_1")
    except Exception:
        pass
    
    price_demand.create_index(
        [("SETTLEMENTDATE", 1), ("REGION", 1)], 
        unique=True,
        name="SETTLEMENTDATE_1_REGION_1"
    )
    
    if start_date is None:
        latest_date = get_latest_date_from_mongodb(price_demand)
        if latest_date:
            start_date = latest_date.strftime('%Y-%m')
        else:
            start_date = '2022-01'
    
    end_date = datetime.now().strftime('%Y-%m')
    print(f"Updating data from {start_date} to {end_date}")
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
    }
    base_url = 'https://aemo.com.au/aemo/data/nem/priceanddemand'
    
    stats = {'processed': 0, 'skipped': 0, 'failed': 0}
    
    start = datetime.strptime(start_date, '%Y-%m')
    end = datetime.strptime(end_date, '%Y-%m')
    
    for region in regions:
        print(f"\nProcessing region: {region}")
        current = start
        while current <= end:
            yearmonth = current.strftime('%Y%m')
            
            existing_count = price_demand.count_documents({
                "REGION": region,
                "SETTLEMENTDATE": {
                    "$regex": f"^{yearmonth[:4]}-{yearmonth[4:6]}"
                }
            })
            
            if existing_count > 1400 and current.month != datetime.now().month:
                print(f"Skipping {yearmonth} {region} - complete data exists")
                stats['skipped'] += 1
            else:
                df = process_aemo_data(base_url, region, yearmonth, headers)
                
                if df is not None:
                    records = df.to_dict('records')
                    for record in records:
                        record['SETTLEMENTDATE'] = record['SETTLEMENTDATE'].strftime('%Y-%m-%d %H:%M:%S')
                    
                    try:
                        result = price_demand.insert_many(records, ordered=False)
                        stats['processed'] += len(result.inserted_ids)
                        print(f'Added {len(result.inserted_ids)} records for {region} {yearmonth}')
                    except BulkWriteError as bwe:
                        successful_inserts = bwe.details['nInserted']
                        stats['processed'] += successful_inserts
                        print(f'Partially added {successful_inserts} records for {region} {yearmonth}')
                    except Exception as e:
                        print(f'Error inserting data for {region} {yearmonth}: {str(e)}')
                        stats['failed'] += 1
                else:
                    stats['failed'] += 1
                
                time.sleep(1)
            
            current = current + timedelta(days=32)
            current = current.replace(day=1)
    
    return stats

if __name__ == "__main__":
    # Get MongoDB URI from environment variable
    mongodb_uri = os.getenv('MONGODB_URI')
    if not mongodb_uri:
        raise ValueError("MONGODB_URI environment variable not set")
        
    client = MongoClient(mongodb_uri)
    
    try:
        stats = update_nem_prices(client)
        
        print("\nUpdate Statistics:")
        print(f"Records processed: {stats['processed']}")
        print(f"Months skipped: {stats['skipped']}")
        print(f"Operations failed: {stats['failed']}")
        
        nem_db = client.NEM_Prices
        total_records = nem_db.price_demand.count_documents({})
        print(f"\nTotal documents in collection: {total_records:,}")
        
        earliest = nem_db.price_demand.find_one(sort=[("SETTLEMENTDATE", 1)])
        latest = nem_db.price_demand.find_one(sort=[("SETTLEMENTDATE", -1)])
        if earliest and latest:
            print(f"Date range: {earliest['SETTLEMENTDATE']} to {latest['SETTLEMENTDATE']}")
            
    except Exception as e:
        print(f"Error: {str(e)}")
        raise
    finally:
        client.close()