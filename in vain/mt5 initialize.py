import MetaTrader5 as mt5

print("🔧 Trying to connect to running MT5 terminal...")

if not mt5.initialize():
    print("❌ MT5 initialize failed:", mt5.last_error())
    quit()

print("✅ Connected to MT5 (IPC working)")

ACCOUNT = 191658031         # <-- your account number
PASSWORD = "iiG(W~1&"      # <-- your password
SERVER = "Dukascopy-demo-mt5-1"  # <-- from MT5 Login Window

print("🔐 Trying to login...")

if mt5.login(login=ACCOUNT, password=PASSWORD, server=SERVER):
    print("✅ Login success!")
else:
    print("❌ Login failed:", mt5.last_error())