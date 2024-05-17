try:
    result = 10 / 0
except ZeroDivisionError as e:
    print(f"Error: {e}")
else:
    print(f"Result: {result}")
finally:
    print("This will always be executed")