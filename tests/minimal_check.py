
print("Hello World", flush=True)
import sys
import os
print(f"Python: {sys.version}", flush=True)
try:
    import config
    print("Config imported", flush=True)
except Exception as e:
    print(f"Config import failed: {e}", flush=True)
