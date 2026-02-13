import os

def printLinks(dir: str):
    print(f"Searching directory: {dir}")
    for f in os.scandir(dir):
        if f.is_file(): 
            if f.path.split('.')[-1].strip() == "html":
                print(f'<a href="{f.path}">{f.name}</a>')
        else:
            printLinks(f.path)


printLinks('.')