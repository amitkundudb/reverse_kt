class Company:
    def __init__(self,e_name, designation):
        self.name = e_name
        self.role = designation

    def employee(self):
        print(f"{self.name} is a {self.role}")

co = Company("Amit", "Trainee Engineer")
co.employee()