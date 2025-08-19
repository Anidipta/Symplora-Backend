import sqlite3
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
import re

class DatabaseManager:
    def __init__(self, db_path: str = "data.db"):
        self.db_path = db_path
        self.init_database()
    
    def init_database(self):
        # Initialize database with required tables
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS employees (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    email TEXT UNIQUE NOT NULL,
                    department TEXT NOT NULL,
                    joining_date DATE NOT NULL,
                    annual_leave_balance INTEGER DEFAULT 21,
                    sick_leave_balance INTEGER DEFAULT 10,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    is_active BOOLEAN DEFAULT 1
                )
            ''')
            
            conn.execute('''
                CREATE TABLE IF NOT EXISTS leave_requests (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    employee_id INTEGER NOT NULL,
                    leave_type TEXT NOT NULL CHECK (leave_type IN ('annual', 'sick', 'emergency', 'maternity', 'paternity')),
                    start_date DATE NOT NULL,
                    end_date DATE NOT NULL,
                    days_requested INTEGER NOT NULL,
                    reason TEXT,
                    status TEXT DEFAULT 'pending' CHECK (status IN ('pending', 'approved', 'rejected', 'cancelled')),
                    approved_by INTEGER,
                    approved_at TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (employee_id) REFERENCES employees (id),
                    FOREIGN KEY (approved_by) REFERENCES employees (id)
                )
            ''')
            
            conn.execute('''
                CREATE TABLE IF NOT EXISTS leave_balance_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    employee_id INTEGER NOT NULL,
                    leave_type TEXT NOT NULL,
                    balance_before INTEGER,
                    balance_after INTEGER,
                    change_amount INTEGER,
                    change_reason TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (employee_id) REFERENCES employees (id)
                )
            ''')

class Employee:
    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager
    
    def validate_email(self, email: str) -> bool:
        # Email format validation
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return bool(re.match(pattern, email))
    
    def validate_joining_date(self, joining_date: str) -> bool:
        # Check if joining date is not in future
        try:
            join_date = datetime.strptime(joining_date, '%Y-%m-%d').date()
            return join_date <= datetime.now().date()
        except ValueError:
            return False
    
    def add_employee(self, name: str, email: str, department: str, joining_date: str) -> Dict[str, Any]:
        # Comprehensive validation for new employee
        if not name or len(name.strip()) < 2:
            return {"success": False, "error": "Name must be at least 2 characters long"}
        
        if not self.validate_email(email):
            return {"success": False, "error": "Invalid email format"}
        
        if not department or len(department.strip()) < 2:
            return {"success": False, "error": "Department must be at least 2 characters long"}
        
        if not self.validate_joining_date(joining_date):
            return {"success": False, "error": "Invalid joining date or future date not allowed"}
        
        try:
            with sqlite3.connect(self.db.db_path) as conn:
                # Check for duplicate email
                cursor = conn.execute("SELECT id FROM employees WHERE email = ?", (email.lower(),))
                if cursor.fetchone():
                    return {"success": False, "error": "Employee with this email already exists"}
                
                # Insert new employee
                cursor = conn.execute('''
                    INSERT INTO employees (name, email, department, joining_date)
                    VALUES (?, ?, ?, ?)
                ''', (name.strip().title(), email.lower(), department.strip().title(), joining_date))
                
                employee_id = cursor.lastrowid
                
                # Initialize balance history
                conn.execute('''
                    INSERT INTO leave_balance_history (employee_id, leave_type, balance_before, balance_after, change_amount, change_reason)
                    VALUES (?, 'annual', 0, 21, 21, 'Initial balance'), (?, 'sick', 0, 10, 10, 'Initial balance')
                ''', (employee_id, employee_id))
                
                return {"success": True, "employee_id": employee_id, "message": "Employee added successfully"}
        
        except sqlite3.IntegrityError as e:
            return {"success": False, "error": f"Database constraint violation: {str(e)}"}
        except Exception as e:
            return {"success": False, "error": f"Unexpected error: {str(e)}"}
    
    def get_employee(self, employee_id: int) -> Optional[Dict[str, Any]]:
        # Retrieve employee details with validation
        try:
            with sqlite3.connect(self.db.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.execute('''
                    SELECT id, name, email, department, joining_date, annual_leave_balance, sick_leave_balance, is_active
                    FROM employees WHERE id = ? AND is_active = 1
                ''', (employee_id,))
                
                row = cursor.fetchone()
                return dict(row) if row else None
        except Exception:
            return None
    
    def get_all_employees(self) -> List[Dict[str, Any]]:
        # Get all active employees
        try:
            with sqlite3.connect(self.db.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.execute('''
                    SELECT id, name, email, department, joining_date, annual_leave_balance, sick_leave_balance
                    FROM employees WHERE is_active = 1 ORDER BY name
                ''')
                return [dict(row) for row in cursor.fetchall()]
        except Exception:
            return []

class LeaveRequest:
    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager
    
    def calculate_working_days(self, start_date: datetime, end_date: datetime) -> int:
        # Calculate working days excluding weekends
        current_date = start_date
        working_days = 0
        while current_date <= end_date:
            if current_date.weekday() < 5:  # Monday=0, Friday=4
                working_days += 1
            current_date += timedelta(days=1)
        return working_days
    
    def validate_dates(self, start_date: str, end_date: str) -> Dict[str, Any]:
        # Comprehensive date validation
        try:
            start = datetime.strptime(start_date, '%Y-%m-%d').date()
            end = datetime.strptime(end_date, '%Y-%m-%d').date()
            
            if start > end:
                return {"valid": False, "error": "Start date cannot be after end date"}
            
            if start < datetime.now().date():
                return {"valid": False, "error": "Cannot apply for leave on past dates"}
            
            if start > datetime.now().date() + timedelta(days=365):
                return {"valid": False, "error": "Cannot apply for leave more than 1 year in advance"}
            
            return {"valid": True, "start_date": start, "end_date": end}
        
        except ValueError:
            return {"valid": False, "error": "Invalid date format. Use YYYY-MM-DD"}
    
    def check_overlapping_leaves(self, employee_id: int, start_date: datetime, end_date: datetime, exclude_request_id: int = None) -> bool:
        # Check for overlapping approved/pending leave requests
        try:
            with sqlite3.connect(self.db.db_path) as conn:
                query = '''
                    SELECT id FROM leave_requests 
                    WHERE employee_id = ? AND status IN ('approved', 'pending') 
                    AND ((start_date <= ? AND end_date >= ?) OR (start_date <= ? AND end_date >= ?) OR (start_date >= ? AND end_date <= ?))
                '''
                params = [employee_id, start_date, start_date, end_date, end_date, start_date, end_date]
                
                if exclude_request_id:
                    query += " AND id != ?"
                    params.append(exclude_request_id)
                
                cursor = conn.execute(query, params)
                return cursor.fetchone() is not None
        except Exception:
            return True  # Assume overlap on error for safety
    
    def apply_leave(self, employee_id: int, leave_type: str, start_date: str, end_date: str, reason: str = "") -> Dict[str, Any]:
        # Apply for leave with comprehensive validation
        employee = Employee(self.db).get_employee(employee_id)
        if not employee:
            return {"success": False, "error": "Employee not found or inactive"}
        
        if leave_type not in ['annual', 'sick', 'emergency', 'maternity', 'paternity']:
            return {"success": False, "error": "Invalid leave type"}
        
        date_validation = self.validate_dates(start_date, end_date)
        if not date_validation["valid"]:
            return {"success": False, "error": date_validation["error"]}
        
        start_dt = date_validation["start_date"]
        end_dt = date_validation["end_date"]
        
        # Check if leave start date is before joining date
        joining_date = datetime.strptime(employee["joining_date"], '%Y-%m-%d').date()
        if start_dt < joining_date:
            return {"success": False, "error": "Cannot apply for leave before joining date"}
        
        # Calculate working days
        working_days = self.calculate_working_days(start_dt, end_dt)
        
        if working_days == 0:
            return {"success": False, "error": "Leave period contains no working days"}
        
        # Check leave balance
        if leave_type == 'annual' and working_days > employee["annual_leave_balance"]:
            return {"success": False, "error": f"Insufficient annual leave balance. Available: {employee['annual_leave_balance']}, Requested: {working_days}"}
        
        if leave_type == 'sick' and working_days > employee["sick_leave_balance"]:
            return {"success": False, "error": f"Insufficient sick leave balance. Available: {employee['sick_leave_balance']}, Requested: {working_days}"}
        
        # Check for overlapping leaves
        if self.check_overlapping_leaves(employee_id, start_dt, end_dt):
            return {"success": False, "error": "Overlapping leave request exists"}
        
        # Check for maximum consecutive days (business rule)
        if working_days > 30:
            return {"success": False, "error": "Cannot apply for more than 30 consecutive working days"}
        
        try:
            with sqlite3.connect(self.db.db_path) as conn:
                cursor = conn.execute('''
                    INSERT INTO leave_requests (employee_id, leave_type, start_date, end_date, days_requested, reason)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (employee_id, leave_type, start_date, end_date, working_days, reason.strip()))
                
                request_id = cursor.lastrowid
                return {"success": True, "request_id": request_id, "days_requested": working_days, "message": "Leave request submitted successfully"}
        
        except Exception as e:
            return {"success": False, "error": f"Failed to submit leave request: {str(e)}"}
    
    def approve_reject_leave(self, request_id: int, action: str, approved_by: int) -> Dict[str, Any]:
        # Approve or reject leave request with validation
        if action not in ['approved', 'rejected']:
            return {"success": False, "error": "Invalid action. Use 'approved' or 'rejected'"}
        
        try:
            with sqlite3.connect(self.db.db_path) as conn:
                conn.row_factory = sqlite3.Row
                # Get leave request details
                cursor = conn.execute('''
                    SELECT lr.*, e.name as employee_name, e.annual_leave_balance, e.sick_leave_balance
                    FROM leave_requests lr
                    JOIN employees e ON lr.employee_id = e.id
                    WHERE lr.id = ?
                ''', (request_id,))
                
                request = cursor.fetchone()
                if not request:
                    return {"success": False, "error": "Leave request not found"}
                
                if request['status'] != 'pending':  # Use dictionary access
                    return {"success": False, "error": f"Leave request is already {request['status']}"}
                
                # Convert dates to datetime objects for calculation
                start_date = datetime.strptime(request['start_date'], '%Y-%m-%d')
                end_date = datetime.strptime(request['end_date'], '%Y-%m-%d')
                days_requested = request['days_requested']  # Use existing calculated days
                
                # Check if approver exists
                approver = Employee(self.db).get_employee(approved_by)
                if not approver:
                    return {"success": False, "error": "Approver not found"}
                
                if action == 'approved':
                    # Deduct leave balance
                    leave_type = request['leave_type']
                    
                    if leave_type == 'annual':
                        current_balance = request['annual_leave_balance']
                        new_balance = current_balance - days_requested
                        if new_balance < 0:
                            return {"success": False, "error": "Insufficient leave balance"}
                        
                        conn.execute('UPDATE employees SET annual_leave_balance = ? WHERE id = ?', 
                                   (new_balance, request['employee_id']))
                        
                        # Record balance history
                        conn.execute('''
                            INSERT INTO leave_balance_history 
                            (employee_id, leave_type, balance_before, balance_after, change_amount, change_reason)
                            VALUES (?, 'annual', ?, ?, ?, 'Leave approved')
                        ''', (request['employee_id'], current_balance, new_balance, -days_requested))
                    
                    elif leave_type == 'sick':
                        current_balance = request['sick_leave_balance']
                        new_balance = current_balance - days_requested
                        if new_balance < 0:
                            return {"success": False, "error": "Insufficient sick leave balance"}
                        
                        conn.execute('UPDATE employees SET sick_leave_balance = ? WHERE id = ?', 
                                   (new_balance, request['employee_id']))
                        
                        # Record balance history
                        conn.execute('''
                            INSERT INTO leave_balance_history 
                            (employee_id, leave_type, balance_before, balance_after, change_amount, change_reason)
                            VALUES (?, 'sick', ?, ?, ?, 'Leave approved')
                        ''', (request['employee_id'], current_balance, new_balance, -days_requested))
            
            # Update request status
            conn.execute('''
                UPDATE leave_requests 
                SET status = ?, approved_by = ?, approved_at = CURRENT_TIMESTAMP 
                WHERE id = ?
            ''', (action, approved_by, request_id))
            
            return {
                "success": True, 
                "message": f"Leave request {action} successfully",
                "days_processed": days_requested
            }
    
        except Exception as e:
            return {"success": False, "error": f"Failed to {action} leave request: {str(e)}"}
    
    def get_leave_requests(self, employee_id: int = None, status: str = None) -> List[Dict[str, Any]]:
        # Get leave requests with optional filters
        try:
            with sqlite3.connect(self.db.db_path) as conn:
                conn.row_factory = sqlite3.Row
                
                query = '''
                    SELECT lr.*, e.name as employee_name, e.department,
                           approver.name as approved_by_name
                    FROM leave_requests lr
                    JOIN employees e ON lr.employee_id = e.id
                    LEFT JOIN employees approver ON lr.approved_by = approver.id
                    WHERE 1=1
                '''
                params = []
                
                if employee_id:
                    query += " AND lr.employee_id = ?"
                    params.append(employee_id)
                
                if status:
                    query += " AND lr.status = ?"
                    params.append(status)
                
                query += " ORDER BY lr.created_at DESC"
                
                cursor = conn.execute(query, params)
                return [dict(row) for row in cursor.fetchall()]
        
        except Exception:
            return []

class LeaveBalance:
    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager
    
    def get_employee_balance(self, employee_id: int) -> Dict[str, Any]:
        # Get comprehensive leave balance information
        employee = Employee(self.db).get_employee(employee_id)
        if not employee:
            return {"success": False, "error": "Employee not found"}
        
        try:
            with sqlite3.connect(self.db.db_path) as conn:
                conn.row_factory = sqlite3.Row
                
                # Get pending leave requests
                pending_cursor = conn.execute('''
                    SELECT leave_type, SUM(days_requested) as pending_days
                    FROM leave_requests 
                    WHERE employee_id = ? AND status = 'pending'
                    GROUP BY leave_type
                ''', (employee_id,))
                
                pending_leaves = {row['leave_type']: row['pending_days'] for row in pending_cursor.fetchall()}
                
                # Get approved leaves this year
                current_year = datetime.now().year
                approved_cursor = conn.execute('''
                    SELECT leave_type, SUM(days_requested) as used_days
                    FROM leave_requests 
                    WHERE employee_id = ? AND status = 'approved' 
                    AND strftime('%Y', start_date) = ?
                    GROUP BY leave_type
                ''', (employee_id, str(current_year)))
                
                used_leaves = {row['leave_type']: row['used_days'] for row in approved_cursor.fetchall()}
                
                return {
                    "success": True,
                    "employee": employee,
                    "balances": {
                        "annual_leave": {
                            "total": 21,
                            "available": employee["annual_leave_balance"],
                            "used": used_leaves.get('annual', 0),
                            "pending": pending_leaves.get('annual', 0)
                        },
                        "sick_leave": {
                            "total": 10,
                            "available": employee["sick_leave_balance"],
                            "used": used_leaves.get('sick', 0),
                            "pending": pending_leaves.get('sick', 0)
                        }
                    }
                }
        
        except Exception as e:
            return {"success": False, "error": f"Failed to fetch balance: {str(e)}"}
