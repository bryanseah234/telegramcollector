
import os
import ast
import sys

# Mapping of PyPI package name to import name
PACKAGE_TO_IMPORT = {
    'python-dotenv': 'dotenv',
    'Pillow': 'PIL',
    'opencv-python-headless': 'cv2',
    'opencv-python': 'cv2',
    'psycopg[binary,pool]': 'psycopg',
    'psycopg-pool': 'psycopg_pool',
    'python-json-logger': 'pythonjsonlogger',
    'prometheus-client': 'prometheus_client',
    'scikit-learn': 'sklearn',
    'google-api-python-client': 'googleapiclient',
    'beautifulsoup4': 'bs4',
    'pyyaml': 'yaml',
    'pydantic-settings': 'pydantic_settings',
    'telethon': 'telethon',
    'redis': 'redis',
    'numpy': 'numpy',
    'requests': 'requests',
    'streamlit': 'streamlit',
    'pandas': 'pandas',
    'altair': 'altair',
    'pytest': 'pytest',
    'pytest-asyncio': 'pytest_asyncio',
    'watchdog': 'watchdog',
    'aiofiles': 'aiofiles',
    'av': 'av',
    'insightface': 'insightface',
    'onnxruntime': 'onnxruntime',
    'pgvector': 'pgvector',
}

# Standard library modules (incomplete list, but covers common ones)
STDLIB = {
    'os', 'sys', 'time', 'datetime', 'json', 'logging', 'asyncio', 'typing',
    'dataclasses', 'enum', 'collections', 'pathlib', 'shutil', 'glob', 're',
    'math', 'random', 'uuid', 'hashlib', 'base64', 'io', 'contextlib',
    'unittest', 'signal', 'functools', 'itertools', 'typing_extensions',
    'abc', 'subprocess', 'tempfile', 'threading', 'concurrent', 'inspect',
    'types', 'warnings', 'traceback', 'weakref', 'gc', 'platform', 'socket',
    'urllib', 'http', 'email', 'xml', 'html', 'csv', 'sqlite3', 'copy',
    'pickle', 'struct', 'textwrap', 'argparse', 'optparse', 'getpass',
    'contextvars', 'site', 'builtins'
}

def get_installed_packages(req_file):
    packages = set()
    if not os.path.exists(req_file):
        return packages
    
    with open(req_file, 'r') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            # Handle "package==version" or "package>=version"
            pkg = line.split('==')[0].split('>=')[0].split('<=')[0].split('~=')[0].strip()
            # Handle "psycopg[binary,pool]" -> "psycopg" (simplified)
            # Actually, we map the full string in PACKAGE_TO_IMPORT usually, 
            # but let's also handle clean names
            
            # Check map first
            if pkg in PACKAGE_TO_IMPORT:
                packages.add(PACKAGE_TO_IMPORT[pkg])
            elif pkg.split('[')[0] in PACKAGE_TO_IMPORT:
                 packages.add(PACKAGE_TO_IMPORT[pkg.split('[')[0]])
            else:
                # Default assume import name == package name (normalized)
                packages.add(pkg.lower().replace('-', '_'))
    return packages

def get_imports_from_file(filepath):
    imports = set()
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            tree = ast.parse(f.read(), filename=filepath)
        
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    imports.add(alias.name.split('.')[0])
            elif isinstance(node, ast.ImportFrom):
                if node.module:
                    imports.add(node.module.split('.')[0])
    except Exception as e:
        print(f"Error parsing {filepath}: {e}")
    return imports

def main():
    req_file = 'requirements.txt'
    installed = get_installed_packages(req_file)
    # Add manually known ones that might be implicitly handled or missed
    installed.add('cv2') # handled by map but just in case
    installed.add('dotenv')
    
    all_imports = set()
    valid_files = []
    
    for root, dirs, files in os.walk('.'):
        if '.venv' in root or 'venv' in root or '.git' in root or '__pycache__' in root:
            continue
        for file in files:
            if file.endswith('.py'):
                path = os.path.join(root, file)
                valid_files.append(path)
                file_imports = get_imports_from_file(path)
                all_imports.update(file_imports)

    missing = []
    for imp in all_imports:
        if imp in STDLIB:
            continue
        if imp.startswith('telegramcollector'): # local package
            continue
        
        # Check if it's a local module (file exists in current dir or root)
        if os.path.exists(f"{imp}.py") or os.path.exists(os.path.join("telegramcollector", f"{imp}.py")):
            continue
        
        # Check if it's a known installed package
        if imp in installed:
            continue

        # Heuristic: check if any installed package *contains* this name
        # e.g. "google.cloud" -> google
        if any(imp.startswith(pkg) for pkg in installed):
            continue

        missing.append(imp)

    print(f"Scanned {len(valid_files)} files.")
    print(f"Found imports: {sorted(list(all_imports))}")
    print(f"Installed (mapped): {sorted(list(installed))}")
    
    if missing:
        print("\nPOTENTIALLY MISSING DEPENDENCIES:")
        for m in sorted(missing):
            print(f" - {m}")
    else:
        print("\nNo missing dependencies found!")

if __name__ == "__main__":
    main()
