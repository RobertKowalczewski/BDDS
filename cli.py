import uuid
import datetime
import subprocess
import time
import os
from main import CassandraReservationSystem

class ReservationConsole:
    def __init__(self, hosts=None, auto_start_docker=False):
        """Initialize the console with Cassandra connection"""
        self.hosts = hosts or [('127.0.0.1', 9042), ('127.0.0.1', 9043), ('127.0.0.1', 9044)]
        self.system = None
        
        if auto_start_docker:
            self.start_docker_containers()
            
        self.connect_to_cassandra()
        
    def connect_to_cassandra(self):
        """Connect to Cassandra cluster"""
        print("🎬 Initializing Cassandra Reservation System...")
        try:
            self.system = CassandraReservationSystem(self.hosts)
            print("✅ System ready!")
        except Exception as e:
            print(f"❌ Failed to connect to Cassandra: {e}")
            print("💡 Try starting Docker containers first (option 10)")
        
    def display_menu(self):
        """Display the main menu options"""
        print("\n" + "="*50)
        print("🎬 MOVIE RESERVATION SYSTEM")
        print("="*50)
        print("1. Add Movie")
        print("2. List All Movies")
        print("3. Make Reservation")
        print("4. Update Reservation")
        print("5. List All Reservations")
        print("6. Check Cluster Health")
        print("7. Run Stress Tests")
        print("8. Reset Database")
        print("9. Exit")
        print("-" * 50)
        print("👤 USER MANAGEMENT")
        print("-" * 50)
        print("10. Register New User")
        print("11. List All Users")
        print("12. Find User")
        print("-" * 50)
    
    # Add new user management functions
    def register_user(self):
        """Register a new user"""
        print("\n👤 REGISTER NEW USER")
        print("-" * 20)
        
        username = self.get_user_input("Enter username: ")
        if not username:
            print("❌ Username cannot be empty")
            return
            
        # Check if username already exists
        if self.system.username_exists(username):
            print(f"❌ Username '{username}' is already taken")
            return
            
        user_id = self.system.insert_user(username)
        if user_id:
            print(f"✅ User registered successfully!")
            print(f"   Username: {username}")
            print(f"   User ID: {user_id}")
        else:
            print("❌ User registration failed")
    
    def list_users(self):
        """List all registered users"""
        print("\n👥 ALL USERS")
        print("-" * 20)
        
        users = self.system.get_all_users()
        if not users:
            print("No users found.")
            return
            
        for user in users:
            print(f"👤 {user.username} - ID: {user.id} - Created: {user.creation}")
    
    def find_user(self):
        """Find a user by username or ID"""
        print("\n🔍 FIND USER")
        print("-" * 20)
        
        print("Search by:")
        print("1. Username")
        print("2. User ID")
        choice = self.get_user_input("Choose option (1 or 2): ")
        
        if choice == "1":
            username = self.get_user_input("Enter username: ")
            user = self.system.get_user_by_username(username)
            if user:
                print(f"\nUser found:")
                print(f"👤 Username: {user.username}")
                print(f"   ID: {user.id}")
                print(f"   Created: {user.creation}")
                print(f"   Last modified: {user.modification}")
            else:
                print(f"❌ No user found with username '{username}'")
        elif choice == "2":
            user_id_str = self.get_user_input("Enter User ID: ")
            try:
                user_id = uuid.UUID(user_id_str)
                user = self.system.get_user_by_id(user_id)
                if user:
                    print(f"\nUser found:")
                    print(f"👤 Username: {user.username}")
                    print(f"   ID: {user.id}")
                    print(f"   Created: {user.creation}")
                    print(f"   Last modified: {user.modification}")
                else:
                    print(f"❌ No user found with ID '{user_id}'")
            except ValueError:
                print("❌ Invalid UUID format")
        else:
            print("❌ Invalid choice")
            
    # Update existing reservation functions to use the user system
    def make_reservation(self):
        """Make a new reservation"""
        print("\n🎫 MAKE RESERVATION")
        print("-" * 20)
        
        # Show available movies first
        movies = self.system.get_all_movies()
        if not movies:
            print("❌ No movies available. Please add movies first.")
            return
            
        print("Available movies:")
        for movie in movies:
            print(f"  📽️ {movie.name} - {movie.date}")
            
        movie_name = self.get_user_input("Enter movie name: ")
        
        # User selection
        users = self.system.get_all_users()
        if not users:
            print("❌ No users registered. Please register a user first.")
            return
            
        print("\nRegistered users:")
        for user in users:
            print(f"  👤 {user.username} - ID: {user.id}")
            
        print("\nUser selection options:")
        print("1. Select from list")
        print("2. Enter User ID")
        choice = self.get_user_input("Choose option (1 or 2): ")
        
        if choice == "1":
            username = self.get_user_input("Enter username: ")
            user = self.system.get_user_by_username(username)
            if not user:
                print(f"❌ No user found with username '{username}'")
                return
            user_id = user.id
        else:
            user_id_str = self.get_user_input("Enter User ID: ")
            try:
                user_id = uuid.UUID(user_id_str)
                if not self.system.user_exists(user_id):
                    print("❌ User not found")
                    return
            except ValueError:
                print("❌ Invalid UUID format")
                return
                
        seat_name = self.get_user_input("Enter seat (e.g., A1, B5): ").upper()
        
        if self.system.make_reservation(movie_name, user_id, seat_name):
            user = self.system.get_user_by_id(user_id)
            username = user.username if user else "Unknown User"
            print(f"✅ Reservation successful!")
            print(f"   Movie: {movie_name}")
            print(f"   User: {username} ({user_id})")
            print(f"   Seat: {seat_name}")
        else:
            print("❌ Reservation failed")
            
    def update_reservation(self):
        """Update an existing reservation"""
        print("\n🔄 UPDATE RESERVATION")
        print("-" * 20)
        
        # Show current reservations
        reservations = self.system.list_all_reservations()
        if not reservations:
            print("❌ No reservations found.")
            return
            
        movie_name = self.get_user_input("Enter movie name: ")
        current_seat = self.get_user_input("Enter current seat: ").upper()
        new_seat = self.get_user_input("Enter new seat: ").upper()
        
        # User selection
        users = self.system.get_all_users()
        print("\nUser selection options:")
        print("1. Select from list")
        print("2. Enter User ID")
        choice = self.get_user_input("Choose option (1 or 2): ")
        
        if choice == "1":
            print("Registered users:")
            for user in users:
                print(f"  👤 {user.username} - ID: {user.id}")
                
            username = self.get_user_input("Enter username: ")
            user = self.system.get_user_by_username(username)
            if not user:
                print(f"❌ No user found with username '{username}'")
                return
            user_id = user.id
        else:
            user_id_str = self.get_user_input("Enter User ID: ")
            try:
                user_id = uuid.UUID(user_id_str)
                if not self.system.user_exists(user_id):
                    print("❌ User not found")
                    return
            except ValueError:
                print("❌ Invalid UUID format")
                return
            
        if self.system.update_reservation(movie_name, current_seat, new_seat, user_id):
            user = self.system.get_user_by_id(user_id)
            username = user.username if user else "Unknown User"
            print(f"✅ Reservation updated successfully!")
            print(f"   Movie: {movie_name}")
            print(f"   User: {username} ({user_id})")
            print(f"   Changed: {current_seat} → {new_seat}")
        else:
            print("❌ Update failed")
    
    def run(self):
        """Main console loop"""
        print("🎬 Welcome to the Movie Reservation System!")
        
        while True:
            try:
                self.display_menu()
                choice = self.get_user_input("Enter your choice (1-17): ")
                
                if choice == "1":
                    if self.system:
                        self.add_movie()
                    else:
                        print("❌ Not connected to Cassandra. Start Docker containers first.")
                elif choice == "2":
                    if self.system:
                        self.list_movies()
                    else:
                        print("❌ Not connected to Cassandra. Start Docker containers first.")
                elif choice == "3":
                    if self.system:
                        self.make_reservation()
                    else:
                        print("❌ Not connected to Cassandra. Start Docker containers first.")
                elif choice == "4":
                    if self.system:
                        self.update_reservation()
                    else:
                        print("❌ Not connected to Cassandra. Start Docker containers first.")
                elif choice == "5":
                    if self.system:
                        self.list_reservations()
                    else:
                        print("❌ Not connected to Cassandra. Start Docker containers first.")
                elif choice == "6":
                    if self.system:
                        self.check_cluster_health()
                    else:
                        print("❌ Not connected to Cassandra. Start Docker containers first.")
                elif choice == "7":
                    if self.system:
                        self.run_stress_tests()
                    else:
                        print("❌ Not connected to Cassandra. Start Docker containers first.")
                elif choice == "8":
                    if self.system:
                        self.reset_database()
                    else:
                        print("❌ Not connected to Cassandra. Start Docker containers first.")
                elif choice == "9":
                    print("👋 Goodbye!")
                    break
                elif choice == "10":
                    if self.system:
                        self.register_user()
                    else:
                        print("❌ Not connected to Cassandra. Start Docker containers first.")
                elif choice == "11":
                    if self.system:
                        self.list_users()
                    else:
                        print("❌ Not connected to Cassandra. Start Docker containers first.")
                elif choice == "12":
                    if self.system:
                        self.find_user()
                    else:
                        print("❌ Not connected to Cassandra. Start Docker containers first.")
                else:
                    print("❌ Invalid choice. Please enter 1-17.")
                    
                # Pause before showing menu again
                input("\nPress Enter to continue...")
                
            except KeyboardInterrupt:
                print("\n👋 Goodbye!")
                break
            except Exception as e:
                print(f"❌ An error occurred: {e}")
                input("Press Enter to continue...")
            
    def wait_for_cassandra_cluster(self, max_wait=180):
        """Wait for Cassandra cluster to be ready"""
        print("⏳ Waiting for Cassandra cluster to initialize...")
        
        start_time = time.time()
        while time.time() - start_time < max_wait:
            try:
                # Check if all containers are healthy
                healthy_count = 0
                for i in range(1, 4):
                    result = subprocess.run(
                        ["docker", "inspect", "--format={{.State.Health.Status}}", f"cassandra{i}"],
                        capture_output=True,
                        text=True
                    )
                    if result.returncode == 0 and result.stdout.strip() == "healthy":
                        healthy_count += 1
                
                if healthy_count == 3:
                    print("✅ All Cassandra nodes are healthy!")
                    time.sleep(5)  # Extra time for cluster formation
                    return True
                    
                print(f"⏳ {healthy_count}/3 nodes healthy, waiting...")
                time.sleep(10)
                
            except Exception:
                time.sleep(10)
                
        print("⚠️ Cluster initialization timeout, but continuing...")
        return False
        
    def get_user_input(self, prompt, input_type=str):
        """Get user input with type validation"""
        while True:
            try:
                value = input(prompt)
                if input_type == str:
                    return value.strip()
                elif input_type == int:
                    return int(value)
                elif input_type == uuid.UUID:
                    return uuid.UUID(value)
                elif input_type == datetime.datetime:
                    # Expected format: YYYY-MM-DD HH:MM
                    return datetime.datetime.strptime(value, "%Y-%m-%d %H:%M")
            except ValueError as e:
                print(f"❌ Invalid input: {e}")
                if input_type == uuid.UUID:
                    print("Please enter a valid UUID or press Enter to generate a new one.")
                elif input_type == datetime.datetime:
                    print("Please enter date in format: YYYY-MM-DD HH:MM (e.g., 2025-12-25 19:30)")
                
    def add_movie(self):
        """Add a new movie"""
        print("\n📽️ ADD NEW MOVIE")
        print("-" * 20)
        
        name = self.get_user_input("Enter movie name: ")
        if not name:
            print("❌ Movie name cannot be empty")
            return
            
        print("Enter movie date and time (format: YYYY-MM-DD HH:MM)")
        print("Example: 2025-12-25 19:30")
        movie_date = self.get_user_input("Movie date: ", datetime.datetime)
        
        if movie_date:
            result = self.system.insert_movie(name, movie_date)
            if result:
                print(f"✅ Movie '{name}' added successfully!")
            else:
                print(f"❌ Failed to add movie '{name}'")
        
    def list_movies(self):
        """List all movies"""
        print("\n🎬 ALL MOVIES")
        print("-" * 20)
        
        movies = self.system.get_all_movies()
        if not movies:
            print("No movies found.")
            return
            
        for movie in movies:
            print(f"📽️ {movie.name} - {movie.date}")
            
            
    def list_reservations(self):
        """List all reservations"""
        print("\n🎫 ALL RESERVATIONS")
        print("-" * 20)
        self.system.list_all_reservations()
        
    def check_cluster_health(self):
        """Check cluster health"""
        print("\n🏥 CLUSTER HEALTH CHECK")
        print("-" * 25)
        if self.system.check_cluster_health():
            print("✅ Cluster is healthy!")
        else:
            print("❌ Cluster has issues!")
            
    def run_stress_tests(self):
        """Run stress tests menu"""
        print("\n🔥 STRESS TESTS")
        print("-" * 15)
        print("1. Run All Stress Tests")
        print("2. Same Request Rapid Fire")
        print("3. Random Operations")
        print("4. Seat Race Condition")
        print("5. Random Operations (Optimistic)")
        print("6. Back to Main Menu")
        
        choice = self.get_user_input("Choose test (1-6): ")
        
        if choice == "1":
            self.system.run_all_stress_tests()
        elif choice == "2":
            self.run_stress_test_1()
        elif choice == "3":
            self.run_stress_test_2()
        elif choice == "4":
            self.run_stress_test_3()
        elif choice == "5":
            self.run_stress_test_2_optimistic()
        elif choice == "6":
            return
        else:
            print("❌ Invalid choice")
            
    def run_stress_test_1(self):
        """Configure and run stress test 1"""
        print("\n🔥 STRESS TEST 1: Same Request Rapid Fire")
        num_requests = self.get_user_input("Number of requests (default 50): ")
        
        if not num_requests:
            num_requests = 50
        else:
            try:
                num_requests = int(num_requests)
            except ValueError:
                num_requests = 50
                
        print(f"Running test with {num_requests} requests...")
        self.system.stress_test_1_same_request_rapid(num_requests)
        
    def run_stress_test_2(self):
        """Configure and run stress test 2"""
        print("\n🔥 STRESS TEST 2: Random Operations")
            
        num_clients = self.get_user_input("Number of clients (default 5): ")
        operations_per_client = self.get_user_input("Operations per client (default 15): ")
        
        try:
            num_clients = int(num_clients) if num_clients else 5
            operations_per_client = int(operations_per_client) if operations_per_client else 15
        except ValueError:
            num_clients = 5
            operations_per_client = 15
            
        print(f"Running test with {num_clients} clients, {operations_per_client} operations each...")
        self.system.stress_test_2_random_operations(num_clients, operations_per_client)
        
    def run_stress_test_3(self):
        """Configure and run stress test 3"""
        print("\n🔥 STRESS TEST 3: Seat Race Condition")
        num_seats = self.get_user_input("Number of seats to compete for (default 30): ")
        
        try:
            num_seats = int(num_seats) if num_seats else 30
        except ValueError:
            num_seats = 30
            
        print(f"Running test with 2 clients competing for {num_seats} seats...")
        self.system.stress_test_3_seat_race_condition(num_seats, 2)
        
    def run_stress_test_2_optimistic(self):
        """Configure and run optimistic stress test 2"""
        print("\n🔥 STRESS TEST 2: Random Operations (Optimistic)")

        num_clients = self.get_user_input("Number of clients (default 10): ")
        operations_per_client = self.get_user_input("Operations per client (default 20): ")
        
        try:
            num_clients = int(num_clients) if num_clients else 10
            operations_per_client = int(operations_per_client) if operations_per_client else 20
        except ValueError:
            num_clients = 10
            operations_per_client = 20
            
        print(f"Running optimistic test with {num_clients} clients, {operations_per_client} operations each...")
        self.system.stress_test_2_random_operations_optimistic(num_clients, operations_per_client)
        
    def reset_database(self):
        """Reset the database"""
        print("\n⚠️ RESET DATABASE")
        print("-" * 20)
        confirm = self.get_user_input("Are you sure? This will delete ALL data! (yes/no): ").lower()
        
        if confirm in ['yes', 'y']:
            print("Resetting database...")
            self.system.reset_cassandra()
            print("✅ Database reset complete!")
        else:
            print("❌ Reset cancelled")
                
    def close(self):
        """Clean up resources"""
        if hasattr(self, 'system'):
            self.system.close()


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Movie Reservation System CLI')
    parser.add_argument('--auto-start', action='store_true', 
                       help='Automatically start Docker containers on startup')
    parser.add_argument('--hosts', nargs='+', 
                       help='Cassandra host addresses (default: 127.0.0.1:9042,9043,9044)')
    
    args = parser.parse_args()
    
    # Parse hosts if provided
    hosts = None
    if args.hosts:
        hosts = []
        for host in args.hosts:
            if ':' in host:
                ip, port = host.split(':')
                hosts.append((ip, int(port)))
            else:
                hosts.append((host, 9042))
    
    console = None
    try:
        console = ReservationConsole(hosts=hosts, auto_start_docker=args.auto_start)
        console.run()
    except Exception as e:
        print(f"❌ Failed to start system: {e}")
    finally:
        if console and console.system:
            console.system.close()


if __name__ == "__main__":
    main()