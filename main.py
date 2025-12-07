#!/usr/bin/env python3
import subprocess
import sys
from datetime import datetime
from pathlib import Path

SCRIPTS = {
    "INFLATION": "Fetchers/inflation.py",
    "ECO CALENDER": "Fetchers/eco calender.py",
    "MARKET DATA": "Fetchers/market data.py",
    "NEWS": "Fetchers/news.py",
    "REDDIT": "Fetchers/reddit.py"
}

TIMEOUT_SECONDS = 300

def run_script(name, script_path):
    print(f"Running {name}...", end=" ", flush=True)
    
    if not Path(script_path).exists():
        print(f"ERROR: Script not found")
        return False
    
    try:
        result = subprocess.run(
            [sys.executable, script_path],
            capture_output=True,
            text=True,
            timeout=TIMEOUT_SECONDS
        )
        
        if result.returncode != 0:
            print(f"FAILED")
            if result.stderr:
                print(f"  Error: {result.stderr}")
            return False
        
        print("DONE")
        return True
            
    except subprocess.TimeoutExpired:
        print(f"TIMEOUT")
        return False
    except Exception as e:
        print(f"ERROR: {str(e)}")
        return False

def main():
    print("\n" + "="*60)
    print("DATA COLLECTION BOT RUNNER")
    print("="*60)
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60 + "\n")
    
    results = {}
    
    for name, script_path in SCRIPTS.items():
        success = run_script(name, script_path)
        results[name] = success
    
    print("\n" + "="*60)
    successful = sum(1 for v in results.values() if v)
    failed = sum(1 for v in results.values() if not v)
    
    print(f"Completed: {successful}/{len(SCRIPTS)} successful")
    
    if failed > 0:
        print(f"Failed: {failed}/{len(SCRIPTS)}")
        for name, success in results.items():
            if not success:
                print(f"  - {name}")
    
    print("="*60)
    print("FINISHED\n")
    
    sys.exit(0 if failed == 0 else 1)

if __name__ == "__main__":
    main()