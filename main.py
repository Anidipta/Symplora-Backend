from flask import Flask, request, jsonify
from flask_cors import CORS
from models import DatabaseManager, Employee, LeaveRequest, LeaveBalance
import logging
from datetime import datetime
import sqlite3

app = Flask(__name__)
CORS(app)  # Enable CORS for frontend integration

# Configure logging for error tracking
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize database and models
db_manager = DatabaseManager()
employee_service = Employee(db_manager)
leave_service = LeaveRequest(db_manager)
balance_service = LeaveBalance(db_manager)

@app.errorhandler(404)
def not_found(error):
    # Handle 404 errors gracefully
    return jsonify({"success": False, "error": "Endpoint not found"}), 404

@app.errorhandler(500)
def internal_error(error):
    # Handle internal server errors
    logger.error(f"Internal server error: {str(error)}")
    return jsonify({"success": False, "error": "Internal server error"}), 500

@app.route('/health', methods=['GET'])
def health_check():
    # API health check endpoint
    return jsonify({"status": "healthy", "timestamp": datetime.now().isoformat()})

@app.route('/employees', methods=['POST'])
def add_employee():
    # Add new employee with comprehensive validation
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({"success": False, "error": "Request body is required"}), 400
        
        # Validate required fields
        required_fields = ['name', 'email', 'department', 'joining_date']
        missing_fields = [field for field in required_fields if field not in data or not data[field]]
        
        if missing_fields:
            return jsonify({"success": False, "error": f"Missing required fields: {', '.join(missing_fields)}"}), 400
        
        result = employee_service.add_employee(
            data['name'], 
            data['email'], 
            data['department'], 
            data['joining_date']
        )
        
        status_code = 201 if result["success"] else 400
        return jsonify(result), status_code
    
    except Exception as e:
        logger.error(f"Error adding employee: {str(e)}")
        return jsonify({"success": False, "error": "Failed to process request"}), 500

@app.route('/employees', methods=['GET'])
def get_employees():
    # Get all employees
    try:
        employees = employee_service.get_all_employees()
        return jsonify({"success": True, "employees": employees})
    except Exception as e:
        logger.error(f"Error fetching employees: {str(e)}")
        return jsonify({"success": False, "error": "Failed to fetch employees"}), 500

@app.route('/employees/<int:employee_id>', methods=['GET'])
def get_employee(employee_id):
    # Get specific employee by ID
    try:
        if employee_id <= 0:
            return jsonify({"success": False, "error": "Invalid employee ID"}), 400
        
        employee = employee_service.get_employee(employee_id)
        
        if not employee:
            return jsonify({"success": False, "error": "Employee not found"}), 404
        
        return jsonify({"success": True, "employee": employee})
    except Exception as e:
        logger.error(f"Error fetching employee {employee_id}: {str(e)}")
        return jsonify({"success": False, "error": "Failed to fetch employee"}), 500

@app.route('/leave-requests', methods=['POST'])
def apply_leave():
    # Apply for leave with validation
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({"success": False, "error": "Request body is required"}), 400
        
        # Validate required fields
        required_fields = ['employee_id', 'leave_type', 'start_date', 'end_date']
        missing_fields = [field for field in required_fields if field not in data or data[field] is None]
        
        if missing_fields:
            return jsonify({"success": False, "error": f"Missing required fields: {', '.join(missing_fields)}"}), 400
        
        # Validate employee_id is integer
        try:
            employee_id = int(data['employee_id'])
            if employee_id <= 0:
                raise ValueError
        except (ValueError, TypeError):
            return jsonify({"success": False, "error": "Invalid employee ID"}), 400
        
        result = leave_service.apply_leave(
            employee_id,
            data['leave_type'],
            data['start_date'],
            data['end_date'],
            data.get('reason', '')
        )
        
        status_code = 201 if result["success"] else 400
        return jsonify(result), status_code
    
    except Exception as e:
        logger.error(f"Error applying leave: {str(e)}")
        return jsonify({"success": False, "error": "Failed to process leave request"}), 500

@app.route('/leave-requests', methods=['GET'])
def get_leave_requests():
    # Get leave requests with optional filters
    try:
        employee_id = request.args.get('employee_id')
        status = request.args.get('status')
        
        # Validate employee_id if provided
        if employee_id:
            try:
                employee_id = int(employee_id)
                if employee_id <= 0:
                    raise ValueError
            except (ValueError, TypeError):
                return jsonify({"success": False, "error": "Invalid employee ID"}), 400
        
        # Validate status if provided
        if status and status not in ['pending', 'approved', 'rejected', 'cancelled']:
            return jsonify({"success": False, "error": "Invalid status"}), 400
        
        requests = leave_service.get_leave_requests(employee_id, status)
        return jsonify({"success": True, "requests": requests})
    
    except Exception as e:
        logger.error(f"Error fetching leave requests: {str(e)}")
        return jsonify({"success": False, "error": "Failed to fetch leave requests"}), 500

@app.route('/leave-requests/<int:request_id>/approve', methods=['PUT'])
def approve_leave(request_id):
    # Approve leave request
    try:
        if request_id <= 0:
            return jsonify({"success": False, "error": "Invalid request ID"}), 400
        
        data = request.get_json()
        
        if not data or 'approved_by' not in data:
            return jsonify({"success": False, "error": "approved_by field is required"}), 400
        
        # Validate approved_by
        try:
            approved_by = int(data['approved_by'])
            if approved_by <= 0:
                raise ValueError
        except (ValueError, TypeError):
            return jsonify({"success": False, "error": "Invalid approver ID"}), 400
        
        result = leave_service.approve_reject_leave(request_id, 'approved', approved_by)
        
        status_code = 200 if result["success"] else 400
        return jsonify(result), status_code
    
    except Exception as e:
        logger.error(f"Error approving leave request {request_id}: {str(e)}")
        return jsonify({"success": False, "error": "Failed to approve leave request"}), 500

@app.route('/leave-requests/<int:request_id>/reject', methods=['PUT'])
def reject_leave(request_id):
    # Reject leave request
    try:
        if request_id <= 0:
            return jsonify({"success": False, "error": "Invalid request ID"}), 400
        
        data = request.get_json()
        
        if not data or 'approved_by' not in data:
            return jsonify({"success": False, "error": "approved_by field is required"}), 400
        
        # Validate approved_by
        try:
            approved_by = int(data['approved_by'])
            if approved_by <= 0:
                raise ValueError
        except (ValueError, TypeError):
            return jsonify({"success": False, "error": "Invalid approver ID"}), 400
        
        result = leave_service.approve_reject_leave(request_id, 'rejected', approved_by)
        
        status_code = 200 if result["success"] else 400
        return jsonify(result), status_code
    
    except Exception as e:
        logger.error(f"Error rejecting leave request {request_id}: {str(e)}")
        return jsonify({"success": False, "error": "Failed to reject leave request"}), 500

@app.route('/employees/<int:employee_id>/balance', methods=['GET'])
def get_leave_balance(employee_id):
    # Get employee leave balance
    try:
        if employee_id <= 0:
            return jsonify({"success": False, "error": "Invalid employee ID"}), 400
        
        result = balance_service.get_employee_balance(employee_id)
        
        status_code = 200 if result["success"] else 404
        return jsonify(result), status_code
    
    except Exception as e:
        logger.error(f"Error fetching balance for employee {employee_id}: {str(e)}")
        return jsonify({"success": False, "error": "Failed to fetch leave balance"}), 500

@app.route('/dashboard/stats', methods=['GET'])
def get_dashboard_stats():
    try:
        with sqlite3.connect(db_manager.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            # Get total employees
            total_employees = cursor.execute(
                "SELECT COUNT(*) as count FROM employees"
            ).fetchone()['count']

            # Get pending requests count
            pending_count = cursor.execute("""
                SELECT COUNT(*) as count 
                FROM leave_requests 
                WHERE status = 'pending'
            """).fetchone()['count']

            # Get approved requests this month
            approved_this_month = cursor.execute("""
                SELECT COUNT(*) as count 
                FROM leave_requests 
                WHERE status = 'approved' 
                AND strftime('%Y-%m', created_at) = strftime('%Y-%m', 'now')
            """).fetchone()['count']

            # Get leave type distribution
            leave_type_distribution = cursor.execute("""
                SELECT 
                    leave_type,
                    COUNT(*) as count
                FROM leave_requests
                WHERE strftime('%Y', created_at) = strftime('%Y', 'now')
                GROUP BY leave_type
            """).fetchall()

            # Get department stats
            dept_stats = cursor.execute("""
                SELECT 
                    e.department,
                    COUNT(e.id) as total_dept_employees,
                    COUNT(DISTINCT lr.employee_id) as employees_on_leave,
                    COUNT(lr.id) as total_leaves,
                    SUM(CASE WHEN lr.status = 'approved' THEN 1 ELSE 0 END) as approved_leaves
                FROM employees e
                LEFT JOIN leave_requests lr ON e.id = lr.employee_id
                WHERE e.department IS NOT NULL
                GROUP BY e.department
            """).fetchall()

            department_analytics = [{
                'department': row['department'],
                'total_employees': row['total_dept_employees'],
                'employees_on_leave': row['employees_on_leave'],
                'total_leaves': row['total_leaves'],
                'approved_leaves': row['approved_leaves'],
                'approved_rate': round((row['approved_leaves'] / row['total_leaves'] * 100), 2) if row['total_leaves'] > 0 else 0
            } for row in dept_stats]

            return jsonify({
                "success": True,
                "stats": {
                    "total_employees": total_employees,
                    "pending_count": pending_count,
                    "approved_this_month": approved_this_month,
                    "leave_type_distribution": [
                        {"leave_type": row['leave_type'], "count": row['count']}
                        for row in leave_type_distribution
                    ],
                    "department_analytics": department_analytics
                }
            })

    except Exception as e:
        logger.error(f"Error getting dashboard stats: {str(e)}")
        return jsonify({
            "success": False,
            "error": "Failed to fetch dashboard statistics"
        }), 500

@app.route('/employees/<int:employee_id>/leave-history', methods=['GET'])
def get_employee_leave_history(employee_id):
    # Get employee's leave history
    try:
        if employee_id <= 0:
            return jsonify({"success": False, "error": "Invalid employee ID"}), 400
        
        # Check if employee exists
        employee = employee_service.get_employee(employee_id)
        if not employee:
            return jsonify({"success": False, "error": "Employee not found"}), 404
        
        # Get leave history with pagination
        page = request.args.get('page', 1, type=int)
        limit = min(request.args.get('limit', 10, type=int), 100)  # Max 100 records per page
        offset = (page - 1) * limit
        
        with sqlite3.connect(db_manager.db_path) as conn:
            conn.row_factory = sqlite3.Row
            
            # Get total count
            total_count = conn.execute(
                "SELECT COUNT(*) FROM leave_requests WHERE employee_id = ?", 
                (employee_id,)
            ).fetchone()[0]
            
            # Get paginated history
            history = conn.execute('''
                SELECT lr.*, approver.name as approved_by_name
                FROM leave_requests lr
                LEFT JOIN employees approver ON lr.approved_by = approver.id
                WHERE lr.employee_id = ?
                ORDER BY lr.created_at DESC
                LIMIT ? OFFSET ?
            ''', (employee_id, limit, offset)).fetchall()
            
            return jsonify({
                "success": True,
                "history": [dict(row) for row in history],
                "total": total_count,
                "page": page,
                "limit": limit
            })
    
    except Exception as e:
        logger.error(f"Error fetching leave history for employee {employee_id}: {str(e)}")
        return jsonify({"success": False, "error": "Failed to fetch leave history"}), 500

@app.route('/', methods=['GET'])
def index():
    """Root endpoint that provides API information"""
    return jsonify({
        "success": True,
        "message": "Welcome to Leave Management API",
        "version": "1.0",
        "endpoints": {
            "health_check": "/health",
            "employees": "/employees",
            "leave_requests": "/leave-requests",
            "dashboard": "/dashboard/stats"
        }
    })

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)