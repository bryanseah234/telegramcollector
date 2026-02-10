
import os
import py_compile
import sys

def check_syntax(directory):
    print(f"Checking syntax for python files in {directory}...")
    has_errors = False
    
    for root, dirs, files in os.walk(directory):
        if 'venv' in root or '.git' in root or '__pycache__' in root:
            continue
            
        for file in files:
            if file.endswith(".py"):
                path = os.path.join(root, file)
                try:
                    py_compile.compile(path, doraise=True)
                    # print(f"OK: {path}")
                except py_compile.PyCompileError as e:
                    print(f"ERROR in {path}:")
                    print(e)
                    has_errors = True
                except Exception as e:
                    print(f"ERROR in {path}: {e}")
                    has_errors = True
                    
    if has_errors:
        print("\n❌ Syntax errors found!")
        sys.exit(1)
    else:
        print("\n✅ No syntax errors found.")
        sys.exit(0)

if __name__ == "__main__":
    check_syntax(".")
