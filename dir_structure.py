import os

base_dir = "app"

# Define the structure
structure = {
    "": ["__init__.py", "main.py", "models.py", "database.py", "schemas.py"],
    "routers": ["__init__.py", "session.py", "verse.py", "memory.py"],
    "services": ["__init__.py", "rag_service.py"],
}

# Create directories and files
for folder, files in structure.items():
    path = os.path.join(base_dir, folder)
    os.makedirs(path, exist_ok=True)
    for file in files:
        file_path = os.path.join(path, file)
        with open(file_path, "w") as f:
            f.write("# " + file + "\n")

print("✅ App directory structure created successfully.")
