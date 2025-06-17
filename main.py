from cassandra.cluster import Cluster, ResultSet
from cassandra.query import SimpleStatement, BatchStatement
from cassandra.policies import RetryPolicy, ExponentialReconnectionPolicy, DCAwareRoundRobinPolicy, TokenAwarePolicy, RoundRobinPolicy
from cassandra import ConsistencyLevel
import uuid
import datetime
import threading
import time
import random
import concurrent.futures
from collections import defaultdict

class CassandraReservationSystem:
    def __init__(self, hosts, executor_threads=4):
        self.cluster = Cluster(
            hosts,
            default_retry_policy=RetryPolicy(),
            reconnection_policy=ExponentialReconnectionPolicy(0.5, 60),
            control_connection_timeout=10,
            connect_timeout=10,
            executor_threads=executor_threads,  # Number of threads for async operations
            max_schema_agreement_wait=30,
            load_balancing_policy=TokenAwarePolicy(
                DCAwareRoundRobinPolicy(local_dc='dc1')
            ),
            # Connection pool settings per host
            port=9042
        )

        self.session = self.cluster.connect()
        self.session.default_timeout = 120
        self.session.default_consistency_level = ConsistencyLevel.QUORUM

        self.reset_cassandra()

        if not self.check_cluster_health():
            print("❌ Cluster health check failed. Please ensure all Cassandra nodes are running.")
            return
        
        self.reservation_insert_stmt = None
        self.reservation_check_stmt = None
        self.reservation_delete_stmt = None
        self.movie_insert_stmt = None
        self.movie_check_stmt = None
        self.user_insert_stmt = None
        self.user_check_by_id_stmt = None
        self.user_check_by_username_stmt = None
        self.get_user_by_id_stmt = None
        self.get_user_by_username_stmt = None

        self._prepare_statements()
        
    def _prepare_statements(self):
        """Prepare all statements once"""
        self.reservation_insert_stmt = self.session.prepare(
            "INSERT INTO reservations (movie_name, user_id, seat, creation, modification) VALUES (?, ?, ?, ?, ?) IF NOT EXISTS"
        )
        self.reservation_check_stmt = self.session.prepare(
            "SELECT user_id, creation FROM reservations WHERE movie_name = ? AND seat = ?"
        )
        self.reservation_delete_stmt = self.session.prepare(
            "DELETE FROM reservations WHERE movie_name = ? AND seat = ? IF user_id = ?"
        )
        self.movie_insert_stmt = self.session.prepare(
            "INSERT INTO movies (name, date, creation, modification) VALUES (?, ?, ?, ?) IF NOT EXISTS"
        )
        self.movie_check_stmt = self.session.prepare(
            "SELECT name FROM movies WHERE name = ?"
        )
        # User-related prepared statements
        self.user_insert_stmt = self.session.prepare(
            "INSERT INTO users (id, username, creation, modification) VALUES (?, ?, ?, ?) IF NOT EXISTS"
        )
        self.user_check_by_id_stmt = self.session.prepare(
            "SELECT id FROM users WHERE id = ?"
        )
        self.user_check_by_username_stmt = self.session.prepare(
            "SELECT id FROM users WHERE username = ?"
        )
        self.get_user_by_id_stmt = self.session.prepare(
            "SELECT id, username, creation, modification FROM users WHERE id = ?"
        )
        self.get_user_by_username_stmt = self.session.prepare(
            "SELECT id, username, creation, modification FROM users WHERE username = ?"
        )
        
    def close(self):
        """Close the cluster connection"""
        self.cluster.shutdown()

    def movie_exists(self, movie_name: str) -> bool:
        """
        Check if a movie exists in the database
        """
        try:
            result = self.session.execute(self.movie_check_stmt, [movie_name])
            return bool(result)
        except Exception as e:
            print(f"Error checking movie existence: {e}")
            return False

    def insert_movie(self, name, movie_date):
        now = datetime.datetime.now()
        try:
            result = self.session.execute(
                self.movie_insert_stmt,
                [name, movie_date, now, now]
            )
            if result.was_applied:
                print(f"Movie '{name}' inserted")
            else:
                print(f"Movie '{name}' already exists")
            return name
        except Exception as e:
            print(f"Error inserting movie: {e}")
            return None
        
    def get_all_movies(self):
        try:
            rows = self.session.execute("SELECT name, date FROM movies")
            return rows
        except Exception as e:
            print(f"Error retrieving movies: {e}")
            return []
            
    # User management functions
    def user_exists(self, user_id: uuid.UUID) -> bool:
        """
        Check if a user exists in the database by ID
        """
        try:
            result = self.session.execute(self.user_check_by_id_stmt, [user_id])
            return bool(result)
        except Exception as e:
            print(f"Error checking user existence: {e}")
            return False
            
    def username_exists(self, username: str) -> bool:
        """
        Check if a username already exists in the database
        """
        try:
            result = self.session.execute(self.user_check_by_username_stmt, [username])
            return bool(result)
        except Exception as e:
            print(f"Error checking username existence: {e}")
            return False
            
    def insert_user(self, username: str) -> uuid.UUID:
        """
        Create a new user with the given username
        Returns the user's UUID if successful, None otherwise
        """
        now = datetime.datetime.now()
        user_id = uuid.uuid4()
        
        try:
            result = self.session.execute(
                self.user_insert_stmt,
                [user_id, username, now, now]
            )
            if result.was_applied:
                print(f"User '{username}' created with ID: {user_id}")
                return user_id
            else:
                print(f"Failed to create user '{username}'")
                return None
        except Exception as e:
            print(f"Error creating user: {e}")
            return None
            
    def get_user_by_id(self, user_id: uuid.UUID):
        """
        Get user details by ID
        """
        try:
            result = self.session.execute(self.get_user_by_id_stmt, [user_id])
            return result.one() if result else None
        except Exception as e:
            print(f"Error retrieving user by ID: {e}")
            return None
            
    def get_user_by_username(self, username: str):
        """
        Get user details by username
        """
        try:
            result = self.session.execute(self.get_user_by_username_stmt, [username])
            return result.one() if result else None
        except Exception as e:
            print(f"Error retrieving user by username: {e}")
            return None
            
    def get_all_users(self):
        """
        Get all users from the database
        """
        try:
            rows = self.session.execute("SELECT id, username, creation FROM users")
            return rows
        except Exception as e:
            print(f"Error retrieving users: {e}")
            return []

    def make_reservation(self, movie_name: str, user_id: uuid.UUID, seat_name: str) -> bool:
        """
        Creates a new reservation for a movie, including the specified seats.
        """
        if not self.movie_exists(movie_name):
            print(f"❌ Movie '{movie_name}' does not exist.")
            return False
            
        if not self.user_exists(user_id):
            print(f"❌ User with ID '{user_id}' does not exist.")
            return False
            
        now = datetime.datetime.now()

        try:
            max_retries = 5
            for attempt in range(max_retries):
                try:
                    result = self.session.execute(
                        self.reservation_insert_stmt, 
                        [movie_name, user_id, seat_name, now, now],
                        timeout=30
                    )

                    if result.was_applied:
                        print(f"✅ Reservation created: movie {movie_name}, user {user_id}, seat {seat_name}")
                        return True
                    else:
                        print(f"❌ Seat {seat_name} already reserved for movie {movie_name}")
                        return False
                        
                except Exception as e:
                    if "CAS operation result is unknown" in str(e) and attempt < max_retries - 1:
                        #print(f"⚠️ CAS timeout on attempt {attempt + 1}, retrying...")
                        time.sleep(0.1 * (attempt + 1))
                        continue
                    else:
                        raise e
                    
            #print(f"❌❌ CAS timeout on all attempts")
            return False

        except Exception as e:
            pass
        #     print(f"❌ Error creating reservation: {e}")
        #     return False
            
    def update_reservation(self, movie_name: str, current_seat_name: str, new_seat_name: str, user_id: uuid.UUID) -> bool:
        """
        Updates an existing movie reservation by changing its seat using atomic operations.
        """
        if not self.movie_exists(movie_name):
            print(f"❌ Movie '{movie_name}' does not exist.")
            return False
            
        if not self.user_exists(user_id):
            print(f"❌ User with ID '{user_id}' does not exist.")
            return False
            
        now = datetime.datetime.now()

        try:
            # First verify the current reservation exists and belongs to this user
            rows = self.session.execute(
                self.reservation_check_stmt,
                [movie_name, current_seat_name]
            )

            if not rows:
                print(f"❌ No reservation found for movie {movie_name} with seat {current_seat_name}")
                return False

            existing_reservation = rows.one()
            if existing_reservation.user_id != user_id:
                print(f"❌ Reservation for seat {current_seat_name} belongs to different user")
                return False

            if new_seat_name == current_seat_name:
                print(f"ℹ️ New seat is the same as current seat. No update needed.")
                return True

            original_creation = existing_reservation.creation

            # Use atomic two-step approach instead of batch (which doesn't support conditional operations properly)
            max_retries = 5
            for attempt in range(max_retries):
                try:
                    # Step 1: Try to claim the new seat
                    new_seat_result = self.session.execute(
                        self.reservation_insert_stmt,
                        [movie_name, user_id, new_seat_name, original_creation, now],
                        timeout=30
                    )

                    if not new_seat_result.was_applied:
                        print(f"❌ New seat {new_seat_name} is already taken")
                        return False

                    # Step 2: Try to release the old seat (with user verification)
                    old_seat_result = self.session.execute(
                        self.reservation_delete_stmt,
                        [movie_name, current_seat_name, user_id],
                        timeout=30
                    )

                    if old_seat_result.was_applied:
                        print(f"✅ Reservation updated: {current_seat_name} → {new_seat_name}")
                        return True
                    else:
                        # Rollback: remove the new seat reservation
                        self.session.execute(
                            self.reservation_delete_stmt,
                            [movie_name, new_seat_name, user_id],
                            timeout=30
                        )
                        print(f"❌ Failed to release old seat, operation rolled back")
                        return False

                except Exception as e:
                    if "CAS operation result is unknown" in str(e) and attempt < max_retries - 1:
                        # print(f"⚠️ CAS timeout on attempt {attempt + 1}, retrying...")
                        time.sleep(0.1 * (attempt + 1))
                        continue
                    else:
                        raise e

        except Exception as e:
            #print(f"❌ Error updating reservation: {e}")
            return False
            
    def list_all_reservations(self) -> list:
        """
        Lists all reservations stored in the 'reservations' table.
        """
        query = SimpleStatement("SELECT movie_name, user_id, seat, creation, modification FROM reservations")

        reservations_list = []
        try:
            rows: ResultSet = self.session.execute(query)

            print("\n--- All Reservations ---")
            if not rows:
                print("No reservations found.")
                return []

            for row in rows:
                user = self.get_user_by_id(row.user_id)
                username = user.username if user else "Unknown User"
                
                reservation_data = {
                    "movie_name": row.movie_name,
                    "user_id": row.user_id,
                    "username": username,
                    "seat": row.seat,
                    "creation": row.creation,
                    "modification": row.modification
                }
                reservations_list.append(reservation_data)
                print(f"Movie: {row.movie_name}, User: {username} ({row.user_id}), Seat: {row.seat}, Created: {row.creation}, Modified: {row.modification}")

            print("------------------------\n")
            return reservations_list

        except Exception as e:
            print(f"Error listing reservations: {e}")
            return []

    def run_cql_file(self, filepath):
        with open(filepath, 'r') as f:
            cql_statements = f.read()

        statements = [s.strip() for s in cql_statements.split(';') if s.strip()]

        for stmt in statements:
            try:
                self.session.execute(stmt)
            except Exception as e:
                print(f"Error executing statement: {stmt}\nError: {e}")
                raise

    def reset_cassandra(self):
        try:
            self.session.execute(
                """
                DROP KEYSPACE IF EXISTS reservation_system;
                """
            )
            self.run_cql_file("schema.cql")

        except Exception as e:
            print(e)
            
    def set_keyspace(self, keyspace_name):
        """Set the keyspace for the session"""
        self.session.set_keyspace(keyspace_name)

    # STRESS TESTS
    
    def stress_test_1_same_request_rapid(self, num_requests: int = 100):
        """
        Stress Test 1: The client makes the same request very quickly.
        """
        print(f"\n=== STRESS TEST 1: Same Request Rapid Fire ===")

        self.reset_cassandra()
        time.sleep(4)
        movie_name = self.insert_movie("StressMovie1", datetime.datetime(2025, 12, 25, 20, 0, 0))

        user_id = self.insert_user("stresstest0")

        seat_name = "Z9"

        print(f"Making {num_requests} identical reservation requests rapidly...")
        
        start_time = time.time()
        success_count = 0
        cas_timeout_count = 0
        
        def make_single_request():
            nonlocal success_count, cas_timeout_count
            try:
                if self.make_reservation(movie_name, user_id, seat_name):
                    success_count += 1
            except Exception as e:
                if "CAS operation result is unknown" in str(e):
                    cas_timeout_count += 1
                print(f"Request failed: {e}")
        
        max_workers = min(10, num_requests // 10)
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(make_single_request) for _ in range(num_requests)]
            concurrent.futures.wait(futures)
        
        end_time = time.time()
        
        print(f"Stress Test 1 Results:")
        print(f"Total requests: {num_requests}")
        print(f"Successful reservations: {success_count}")
        print(f"CAS timeouts: {cas_timeout_count}")
        print(f"Other failures: {num_requests - success_count - cas_timeout_count}")
        print(f"Time taken: {end_time - start_time:.2f} seconds")
        print(f"Requests per second: {num_requests / (end_time - start_time):.2f}")
        print("Expected: Only 1 reservation should succeed (due to IF NOT EXISTS)")
        
    def stress_test_2_random_operations(self, num_clients: int = 4, operations_per_client: int = 50):
        """
        Stress Test 2: Two or more clients make possible requests randomly.
        Uses database state as source of truth to avoid race conditions.
        """
        print(f"\n=== STRESS TEST 2: Random Operations by Multiple Clients ===")

        self.reset_cassandra()
        time.sleep(4)
        movie_1 = self.insert_movie("StressMovie1", datetime.datetime(2025, 12, 25, 20, 0, 0))
        movie_2 = self.insert_movie("StressMovie2", datetime.datetime(2025, 12, 26, 20, 0, 0))
        movie_names = [movie_1, movie_2]

        print(f"Running {num_clients} clients with {operations_per_client} operations each...")
        
        # Generate seats A1-A10, B1-B10, etc.
        seats = [f"{chr(65+i)}{j}" for i in range(5) for j in range(1, 11)]  # A1-E10 (50 seats)
        
        # Use database as source of truth with a single lock for coordination
        coordination_lock = threading.Lock()
        
        results = defaultdict(int)
        results_lock = threading.Lock()
        
        def get_current_state_from_db():
            """Get current reservation state from database"""
            try:
                rows = self.session.execute("SELECT movie_name, user_id, seat FROM reservations")
                reserved_seats = {}  # {(movie, seat): user_id}
                user_reservations = defaultdict(list)  # {user_id: [(movie, seat)]}
                
                for row in rows:
                    key = (row.movie_name, row.seat)
                    reserved_seats[key] = row.user_id
                    user_reservations[row.user_id].append((row.movie_name, row.seat))
                    
                return reserved_seats, user_reservations
            except Exception as e:
                print(f"Error getting state from DB: {e}")
                return {}, defaultdict(list)
        
        def client_worker(user_id):
            client_results = {"reservations": 0, "updates": 0, "errors": 0, "skipped": 0}
            
            for _ in range(operations_per_client):
                operation = random.choice(["reserve", "update"])
                movie = random.choice(movie_names)
                
                try:
                    with coordination_lock:
                        # Get fresh state from database
                        reserved_seats, user_reservations = get_current_state_from_db()
                        
                        if operation == "reserve":
                            # Find available seats for this movie
                            empty_seats = [seat for seat in seats if (movie, seat) not in reserved_seats]
                            
                            if empty_seats:
                                seat = random.choice(empty_seats)
                                # Attempt reservation immediately while holding lock
                                if self.make_reservation(movie, user_id, seat):
                                    client_results["reservations"] += 1
                                else:
                                    client_results["skipped"] += 1
                            else:
                                client_results["skipped"] += 1  # No empty seats
                                
                        elif operation == "update":
                            # Find seats this user has reserved for any movie
                            user_seats = user_reservations[user_id]
                            if user_seats:
                                # Pick a random reservation from this user
                                current_movie, current_seat = random.choice(user_seats)
                                
                                # Find empty seats for the same movie
                                empty_seats = [seat for seat in seats if (current_movie, seat) not in reserved_seats]
                                
                                if empty_seats:
                                    new_seat = random.choice(empty_seats)
                                    # Attempt update immediately while holding lock
                                    if self.update_reservation(current_movie, current_seat, new_seat, user_id):
                                        client_results["updates"] += 1
                                    else:
                                        client_results["skipped"] += 1
                                else:
                                    client_results["skipped"] += 1  # No empty seats available
                            else:
                                client_results["skipped"] += 1  # User has no reservations
                                
                except Exception as e:
                    client_results["errors"] += 1
                    print(f"Client {user_id} error: {e}")
                
                # Small delay between operations
                #time.sleep(random.uniform(0.001, 0.005))
            
            with results_lock:
                for key, value in client_results.items():
                    results[key] += value
                    
            print(f"Client {user_id} completed: {client_results}")
        
        start_time = time.time()
        
        # Run clients concurrently
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_clients) as executor:
            futures = [executor.submit(client_worker, self.insert_user(f"stresstest{i}")) for i in range(num_clients)]
            concurrent.futures.wait(futures)
        
        end_time = time.time()
        
        print(f"\nStress Test 2 Results:")
        print(f"Total clients: {num_clients}")
        print(f"Operations per client: {operations_per_client}")
        print(f"Total reservations made: {results['reservations']}")
        print(f"Total updates made: {results['updates']}")
        print(f"Total errors: {results['errors']}")
        print(f"Total skipped operations: {results['skipped']}")
        print(f"Success rate: {((results['reservations'] + results['updates']) / (num_clients * operations_per_client)) * 100:.1f}%")
        print(f"Time taken: {end_time - start_time:.2f} seconds")
        
        # Final verification
        print(f"\nFinal state verification:")
        actual_reservations = self.list_all_reservations()
        print(f"Final reservations in DB: {len(actual_reservations)}")
        
        # Check for any inconsistencies
        movie_seat_counts = defaultdict(int)
        for res in actual_reservations:
            movie_seat_counts[res['movie_name']] += 1
        
        for movie, count in movie_seat_counts.items():
            print(f"Movie '{movie}': {count} seats reserved")


    def stress_test_2_random_operations_optimistic(self, num_clients: int = 10, operations_per_client: int = 20):
        """
        Stress Test 2: Optimistic approach - try operations and handle failures gracefully
        """
        print(f"\n=== STRESS TEST 2: Random Operations (Optimistic) ===")

        self.reset_cassandra()
        time.sleep(4)
        movie_1 = self.insert_movie("StressMovie1", datetime.datetime(2025, 12, 25, 20, 0, 0))
        movie_2 = self.insert_movie("StressMovie2", datetime.datetime(2025, 12, 26, 20, 0, 0))
        movie_names = [movie_1, movie_2]

        print(f"Running {num_clients} clients with {operations_per_client} operations each...")
        
        seats = [f"{chr(65+i)}{j}" for i in range(5) for j in range(1, 11)]  # A1-E10
        results = defaultdict(int)
        results_lock = threading.Lock()
        
        def client_worker(user_id):
            user_reservations = []  # Track this user's reservations locally
            client_results = {"reservations": 0, "updates": 0, "errors": 0, "skipped": 0}
            
            for _ in range(operations_per_client):
                operation = random.choice(["reserve", "update"]) if user_reservations else "reserve"
                movie = random.choice(movie_names)
                
                try:
                    if operation == "reserve":
                        # Pick a random seat and try to reserve it
                        seat = random.choice(seats)
                        if self.make_reservation(movie, user_id, seat):
                            user_reservations.append((movie, seat))
                            client_results["reservations"] += 1
                        else:
                            client_results["skipped"] += 1  # Seat was taken
                            
                    elif operation == "update" and user_reservations:
                        # Pick one of this user's reservations to update
                        current_movie, current_seat = random.choice(user_reservations)
                        new_seat = random.choice(seats)
                        
                        if current_seat != new_seat:  # Don't update to same seat
                            if self.update_reservation(current_movie, current_seat, new_seat, user_id):
                                # Update local tracking
                                user_reservations.remove((current_movie, current_seat))
                                user_reservations.append((current_movie, new_seat))
                                client_results["updates"] += 1
                            else:
                                client_results["skipped"] += 1  # Update failed
                        else:
                            client_results["skipped"] += 1  # Same seat
                    else:
                        client_results["skipped"] += 1  # No reservations to update
                        
                except Exception as e:
                    client_results["errors"] += 1
                    print(f"Client {user_id} error: {e}")
                
                #time.sleep(random.uniform(0.001, 0.005))
            
            with results_lock:
                for key, value in client_results.items():
                    results[key] += value
                    
            print(f"Client {user_id} completed: {client_results} (final reservations: {len(user_reservations)})")
        
        start_time = time.time()
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_clients) as executor:
            futures = [executor.submit(client_worker, self.insert_user(f"stresstest{0}")) for i in range(num_clients)]
            concurrent.futures.wait(futures)
        
        end_time = time.time()
        
        print(f"\nOptimistic Stress Test Results:")
        print(f"Total operations attempted: {num_clients * operations_per_client}")
        print(f"Successful reservations: {results['reservations']}")
        print(f"Successful updates: {results['updates']}")
        print(f"Skipped operations: {results['skipped']}")
        print(f"Errors: {results['errors']}")
        print(f"Time taken: {end_time - start_time:.2f} seconds")
        
    def stress_test_3_seat_race_condition(self, num_seats: int = 50, num_clients: int = 2):
        """
        Stress Test 3: Immediate occupancy of all seats/reservations on 2 clients.
        Two parties try to make as many reservations as possible at the same time.
        """
        print(f"\n=== STRESS TEST 3: Seat Race Condition ===")

        self.reset_cassandra()
        time.sleep(4)
        movie_name = self.insert_movie("StressMovie1", datetime.datetime(2025, 12, 25, 20, 0, 0))

        print(f"Two clients racing to reserve {num_seats} seats...")
        
        # Generate seats A1-A10, B1-B10, etc.
        seats = [f"{chr(65+i)}{j}" for i in range(10) for j in range(1, 11)][:num_seats]
        
        client_results = {}
        results_lock = threading.Lock()
        
        def aggressive_client(user_id):
            """Each client tries to reserve ALL seats as fast as possible"""
            reservations_made = []

            client_seats = seats.copy()
            random.shuffle(client_seats)
            
            def reserve_seat(seat):
                if self.make_reservation(movie_name, user_id, seat):
                    return seat
                return None
            
            # Use ThreadPoolExecutor for maximum concurrency within each client
            with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
                futures = {executor.submit(reserve_seat, seat): seat for seat in client_seats}
                
                for future in concurrent.futures.as_completed(futures):
                    result = future.result()
                    if result:
                        reservations_made.append(result)
            
            with results_lock:
                client_results[user_id] = {
                    "user_id": user_id,
                    "reservations": reservations_made,
                    "count": len(reservations_made)
                }
                
            print(f"Client {user_id} secured {len(reservations_made)} seats: {reservations_made}")
        
        # Create two users for the competing clients
        user_1 = self.insert_user(f"stressuser1")
        user_2 = self.insert_user(f"stressuser2")
        
        start_time = time.time()
        
        # Run both clients simultaneously
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_clients) as executor:
            futures = [
                executor.submit(aggressive_client, user_1),
                executor.submit(aggressive_client, user_2)
            ]
            concurrent.futures.wait(futures)
        
        end_time = time.time()
        
        print(f"\nStress Test 3 Results:")
        print(f"Total seats available: {num_seats}")
        print(f"Time taken: {end_time - start_time:.2f} seconds")
        
        total_reserved = 0
        for client_id, result in client_results.items():
            print(f"Client {client_id}: {result['count']} reservations")
            total_reserved += result['count']
        
        print(f"Total seats reserved: {total_reserved}")
        print(f"Seats remaining: {num_seats - total_reserved}")
        
        # Verify no double bookings
        all_reserved_seats = []
        for result in client_results.values():
            all_reserved_seats.extend(result['reservations'])
        
        if len(all_reserved_seats) == len(set(all_reserved_seats)):
            print("✅ SUCCESS: No double bookings detected!")
        else:
            print("❌ ERROR: Double bookings detected!")

    def run_all_stress_tests(self):
        """Run all stress tests in sequence"""
        print("🚀 Starting Comprehensive Stress Testing...")

        if not self.check_cluster_health():
            print("❌ Cluster health check failed. Please ensure all Cassandra nodes are running.")
            return

        print("Checking cluster health before test 1...")
        self.check_cluster_health()
        
        # Test 1: Same request rapid fire
        self.stress_test_1_same_request_rapid(50)

        print("Checking cluster health before test 2...")
        self.check_cluster_health()
        
        # Test 2: Random operations
        self.stress_test_2_random_operations(num_clients=5, operations_per_client=15)

        print("Checking cluster health before test 3...")
        self.check_cluster_health()
        
        # Test 3: Seat race condition
        self.stress_test_3_seat_race_condition(num_seats=30, num_clients=2)


    def check_cluster_health(self):
        """Check if all nodes in the cluster are responding"""
        print("Checking cluster health...")
        try:
            # Check local node
            local_rows = self.session.execute("SELECT key FROM system.local")
            local_data = list(local_rows)
            print(f"✅ Local node accessible: {len(local_data)} entries")
            
            # Check peer nodes
            peer_rows = self.session.execute("SELECT peer, rpc_address FROM system.peers")
            active_peers = list(peer_rows)
            print(f"Active peers: {len(active_peers)}")
            
            for peer in active_peers:
                print(f"  Peer: {peer.peer}, RPC: {peer.rpc_address}")
            
            # Check if we have enough nodes for QUORUM (need at least 2 total)
            total_nodes = len(active_peers) + 1  # +1 for local node
            print(f"Total nodes in cluster: {total_nodes}")
            
            if total_nodes >= 2:
                print("✅ Sufficient nodes for QUORUM operations")
                return True
            else:
                print("❌ Insufficient nodes for QUORUM operations")
                return False
            
        except Exception as e:
            print(f"❌ Cluster health check failed: {e}")
            return False