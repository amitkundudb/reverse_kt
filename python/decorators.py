def uppercase(func):
    def wrapper(arg):
        result = func(arg)
        return result.upper()
    return wrapper
@uppercase
def greet(name):
    return f"Good Morning, {name}"
print(greet("Amit"))  # Output: "GOOD MORNING, AMIT"