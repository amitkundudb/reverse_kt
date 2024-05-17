class Vehicle:
    def __init__(self, brand, model):
        self.brand = brand
        self.model = model
    def start(self):
        print(f"Starting the {self.brand} {self.model}")
class Car(Vehicle):
    def __init__(self, brand, model, num_doors):
        super().__init__(brand, model)
        self.num_doors = num_doors
    def start(self):
        print(f"Starting the {self.num_doors}-door {self.brand} {self.model}")

car = Car("Toyota", "Camry", 4)
car.start()