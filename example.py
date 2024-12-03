from typing import List, Optional
from datetime import datetime
from abc import ABC, abstractmethod

class Vehicle(ABC):
    """
    Abstract base class representing a vehicle.
    
    This class demonstrates inheritance, abstract methods, properties,
    class methods, and proper documentation.
    
    Attributes:
        make (str): The manufacturer of the vehicle
        model (str): The model name of the vehicle
        year (int): The manufacturing year
        _mileage (float): Private attribute for vehicle mileage
    """
    
    # Class variable shared by all instances
    total_vehicles = 0
    
    def __init__(self, make: str, model: str, year: int) -> None:
        """
        Initialize a new Vehicle instance.
        
        Args:
            make: Manufacturer name
            model: Model name
            year: Manufacturing year
        
        Raises:
            ValueError: If year is not within reasonable range
        """
        if not (1900 <= year <= datetime.now().year + 1):
            raise ValueError(f"Year must be between 1900 and {datetime.now().year + 1}")
        
        self.make = make
        self.model = model
        self.year = year
        self._mileage = 0.0  # Protected attribute
        Vehicle.total_vehicles += 1
    
    def __str__(self) -> str:
        """Return string representation of the vehicle."""
        return f"{self.year} {self.make} {self.model}"
    
    @property
    def mileage(self) -> float:
        """Get the current mileage of the vehicle."""
        return self._mileage
    
    @mileage.setter
    def mileage(self, value: float) -> None:
        """
        Set the mileage of the vehicle.
        
        Args:
            value: New mileage value
            
        Raises:
            ValueError: If mileage is negative
        """
        if value < 0:
            raise ValueError("Mileage cannot be negative")
        self._mileage = value
    
    @classmethod
    def getTotalVehicles(cls) -> int:
        """Return the total number of vehicles created."""
        return cls.total_vehicles
    
    @staticmethod
    def validateVin(vin: str) -> bool:
        """
        Validate a Vehicle Identification Number (VIN).
        
        Args:
            vin: The VIN to validate
            
        Returns:
            bool: True if VIN is valid, False otherwise
        """
        return len(vin) == 17 and vin.isalnum()
    
    @abstractmethod
    def calculateFuelEfficiency(self) -> float:
        """Calculate and return the fuel efficiency of the vehicle."""
        pass


class ElectricCar(Vehicle):
    """
    Class representing an electric car.
    
    Demonstrates inheritance, method overriding, and composition.
    
    Attributes:
        battery_capacity (float): Battery capacity in kWh
        _charging_sessions (List): Private list tracking charging history
    """
    
    def __init__(self, make: str, model: str, year: int, battery_capacity: float) -> None:
        """
        Initialize a new ElectricCar instance.
        
        Args:
            make: Manufacturer name
            model: Model name
            year: Manufacturing year
            battery_capacity: Battery capacity in kWh
        """
        super().__init__(make, model, year)
        self.battery_capacity = battery_capacity
        self._charging_sessions = []  # Private attribute
    
    def addChargingSession(self, kwh_charged: float, date: datetime = None) -> None:
        """
        Record a charging session.
        
        Args:
            kwh_charged: Amount of energy charged in kWh
            date: Date of charging, defaults to current time
        """
        if date is None:
            date = datetime.now()
        self._charging_sessions.append({"date": date, "kwh": kwh_charged})
    
    def getChargingHistory(self) -> List[dict]:
        """Return a copy of the charging history."""
        return self._charging_sessions.copy()
    
    def calculateFuelEfficiency(self) -> float:
        """
        Calculate and return the fuel efficiency in miles per kWh.
        
        Overrides the abstract method from Vehicle class.
        """
        if not self._charging_sessions:
            return 0.0
        
        total_kwh = sum(session["kwh"] for session in self._charging_sessions)
        return self.mileage / total_kwh if total_kwh > 0 else 0.0


class CarFleet:
    """
    Class demonstrating composition and collection management.
    
    This class manages a fleet of vehicles, showing how to implement
    a collection class with proper encapsulation.
    """
    
    def __init__(self) -> None:
        """Initialize an empty car fleet."""
        self._vehicles = []  # Private attribute
    
    def addVehicle(self, vehicle: Vehicle) -> None:
        """
        Add a vehicle to the fleet.
        
        Args:
            vehicle: Vehicle instance to add
        """
        self._vehicles.append(vehicle)
    
    def getVehicleByIndex(self, index: int) -> Optional[Vehicle]:
        """
        Get a vehicle by its index in the fleet.
        
        Args:
            index: Index of the vehicle
            
        Returns:
            Vehicle if found, None otherwise
        """
        try:
            return self._vehicles[index]
        except IndexError:
            return None
    
    def getAllVehicles(self) -> List[Vehicle]:
        """Return a copy of the vehicle list."""
        return self._vehicles.copy()
    
    def getTotalMileage(self) -> float:
        """Calculate and return the total fleet mileage."""
        return sum(vehicle.mileage for vehicle in self._vehicles)
    
    def __len__(self) -> int:
        """Return the number of vehicles in the fleet."""
        return len(self._vehicles)