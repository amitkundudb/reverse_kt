file_path = "data_unavailable_path.csv"
try:
    with open(file_path, "r") as file:
        data = file.read()
except FileNotFoundError:
    print(f"Error: File '{file_path}' not found")
except PermissionError:
    print(f"Error: Permission denied to access '{file_path}'")
except Exception as e:
    print(f"Error: {e}")
else:
    pass