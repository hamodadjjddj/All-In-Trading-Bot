import json
import investpy
from datetime import datetime, timedelta
import pandas as pd

class EconomicCalendarLayer:
    def __init__(self):
        pass
    
    def fetch_calendar(self, from_date, to_date):
        try:
            print(f"Fetching from Investing.com: {from_date} to {to_date}")
            
            df = investpy.economic_calendar(
                countries=['united states'],
                importances=['high', 'medium'],
                from_date=from_date,
                to_date=to_date
            )
            
            if not df.empty:
                print(f" Fetched {len(df)} events from Investing.com")
                return df
            else:
                print(" No data returned")
                return None
                
        except Exception as e:
            print(f" Error: {str(e)}")
            return None
    
    def has_numeric_data(self, actual, forecast, previous):
        for val in [actual, forecast, previous]:
            if val and str(val).strip() and str(val).strip() != 'nan':
                try:
                    float(str(val).replace('%', '').replace('K', '').replace('M', '').replace('B', '').strip())
                    return True
                except:
                    continue
        return False
    
    def run(self):
        print("="*60)
        print("Economic Calendar - USD High/Medium Impact with Data")
        print("="*60)
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)
        
        from_str = start_date.strftime('%d/%m/%Y')
        to_str = end_date.strftime('%d/%m/%Y')
        
        print(f"\nDate Range: {from_str} to {to_str}")
        
        df = self.fetch_calendar(from_str, to_str)
        
        if df is None or df.empty:
            print("\n Failed to fetch calendar data")
            return None
        
        print(f"\nFound {len(df)} USD High/Medium impact events")
        
        events_list = []
        skipped = 0
        
        for _, row in df.iterrows():
            actual = row.get('actual', '')
            forecast = row.get('forecast', '')
            previous = row.get('previous', '')
            
            if not self.has_numeric_data(actual, forecast, previous):
                skipped += 1
                continue
            
            event_data = {
                'date': str(row.get('date', '')),
                'time': str(row.get('time', '')),
                'currency': str(row.get('currency', '')),
                'event': str(row.get('event', '')),
                'actual': str(actual) if actual and str(actual) != 'nan' else '',
                'forecast': str(forecast) if forecast and str(forecast) != 'nan' else '',
                'previous': str(previous) if previous and str(previous) != 'nan' else ''
            }
            
            events_list.append(event_data)
        
        print(f"Filtered to {len(events_list)} events with numeric data (skipped {skipped} without data)")
        
        output = {
            'events': events_list
        }
        
        out_path = "Fetchers/jsons/economic_calendar.json"
        with open(out_path, 'w', encoding='utf-8') as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
        
        print(f"\n{'='*60}")
        print(f"Saved to {out_path}")
        print(f"Total events: {len(events_list)}")
        print("="*60)
        
        return output

if __name__ == "__main__":
    print("\n Using investpy - No API Key Required!")
    print("=" * 60)
    
    layer = EconomicCalendarLayer()
    result = layer.run()
    
    if result:
        print(f"\n Total events with data: {len(result['events'])}")
    else:
        print("\n Failed to retrieve data")