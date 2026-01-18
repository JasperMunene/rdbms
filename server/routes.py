from flask import request
from flask_restful import Resource, Api
import bcrypt
from .models import db_manager

def init_routes(api: Api):
    # Auth / Registration
    api.add_resource(Register, '/api/register')
    
    # Dashboard
    api.add_resource(DashboardStats, '/api/stats')
    
    # Merchants
    api.add_resource(MerchantList, '/api/merchants')
    api.add_resource(MerchantDetail, '/api/merchants/<int:merchant_id>')
    
    # Transactions
    api.add_resource(TransactionList, '/api/transactions')
    
    # Customers
    api.add_resource(CustomerList, '/api/customers')
    api.add_resource(CustomerDetail, '/api/customers/<string:identifier>') # Can be phone or ID


class Register(Resource):
    def post(self):
        """Standard registration"""
        data = request.get_json()
        if not data: return {'error': 'No data provided'}, 400
        
        required = ['email', 'password', 'business_name']
        if not all(k in data for k in required):
            return {'error': 'Missing required fields'}, 400

        # Check existing user
        if db_manager.get_user_by_email(data['email']):
            return {'error': 'User already exists'}, 409

        # Hash password
        hashed = bcrypt.hashpw(data['password'].encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
        
        # Create User
        user_id = db_manager.register_user(data['email'], hashed)
        
        # Create Merchant Profile
        country = data.get('country', 'Kenya')
        till = data.get('mpesa_till', '')
        
        merchant_id = db_manager.add_merchant(user_id, data['business_name'], till, country)
        
        return {
            'message': 'Registration successful',
            'user': {
                'user_id': str(user_id),
                'merchant_id': str(merchant_id),
                'email': data['email']
            }
        }, 201

class DashboardStats(Resource):
    def get(self):
        return db_manager.get_stats()

class MerchantList(Resource):
    def get(self):
        return db_manager.get_merchants()
    
    def post(self):
        data = request.get_json()
        if not data:
            return {'error': 'No data provided'}, 400
        
        required = ['email', 'business_name']
        if not all(k in data for k in required):
            return {'error': 'Missing required fields: email, business_name'}, 400
        
        # Check existing user
        if db_manager.get_user_by_email(data['email']):
            return {'error': 'User with this email already exists'}, 409
        
        # Hash password
        # Auto-generate default password
        default_password = 'changeme123'
        hashed = bcrypt.hashpw(default_password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
        
        # Create User
        user_id = db_manager.register_user(data['email'], hashed)
        
        # Create Merchant Profile
        country = data.get('country', 'Kenya')
        till = data.get('mpesa_till', '')
        
        merchant_id = db_manager.add_merchant(user_id, data['business_name'], till, country)
        
        return {
            'message': 'Merchant created successfully',
            'merchant_id': merchant_id,
            'user_id': user_id,
            'business_name': data['business_name']
        }, 201
class MerchantDetail(Resource):
    def get(self, merchant_id):
        m = db_manager.get_merchant_by_id(merchant_id)
        if not m:
            return {'error': 'Merchant not found'}, 404
        return m

    def put(self, merchant_id):
        data = request.get_json()
        if not data:
            return {'error': 'No data provided'}, 400
        # Allowed fields to update
        allowed = {'business_name', 'mpesa_till', 'country', 'status'}
        updates = {k: v for k, v in data.items() if k in allowed}
        if not updates:
            return {'error': 'No valid fields to update'}, 400
        res = db_manager.update_merchant(merchant_id, updates)
        if 'error' in res:
            return res, 400
        return {'message': 'Merchant updated', 'merchant_id': merchant_id, 'updates': updates}, 200
    
    def delete(self, merchant_id):
        res = db_manager.delete_merchant(merchant_id)
        if 'error' in res: return res, 400
        return {'message': 'Merchant deleted'}

class TransactionList(Resource):
    def get(self):
        m_id = request.args.get('merchant_id')
        return db_manager.get_transactions(merchant_id=m_id)

    def post(self):
        data = request.get_json()
        m_id = data.get('merchant_id')
        if not m_id:
            # Fallback for demo
            merchants = db_manager.get_merchants(limit=1)
            if merchants: m_id = merchants[0]['merchant_id']
            else: return {'error': 'No merchants exist'}, 400
            
        amount = data.get('amount')
        if not amount: return {'error': 'Amount required'}, 400
        
        # Handle Customer Auto-Creation
        c_id = None
        phone = data.get('customer_phone')
        if phone:
            cust = db_manager.get_customer_by_phone(phone)
            if cust:
                c_id = cust['customer_id']
            else:
                # Create new
                name = data.get('customer_name', 'Guest Customer')
                c_id = db_manager.add_customer(phone, name)
        
        res = db_manager.add_transaction(m_id, c_id, amount)
        if 'error' in res: return res, 400
        return res, 201

class CustomerList(Resource):
    def get(self):
        return db_manager.get_customers()
    
    def post(self):
        # CRUD Add
        data = request.get_json()
        if not data.get('phone') or not data.get('full_name'):
             return {'error': 'Phone and Name required'}, 400
        
        if db_manager.get_customer_by_phone(data['phone']):
            return {'error': 'Customer exists'}, 409
            
        c_id = db_manager.add_customer(data['phone'], data['full_name'], data.get('email', ''))
        return {'customer_id': c_id, 'message': 'Created', 'full_name': data['full_name']}, 201

class CustomerDetail(Resource):
    def get(self, identifier):
        cust = db_manager.get_customer_by_phone(identifier)
        if not cust:
             return {'error': 'Customer not found'}, 404
        return cust

    def delete(self, identifier):
        # Delete by ID is safer.
        # Let's try to interpret identifier as ID.
        try:
            c_id = int(identifier)
            db_manager.delete_customer(c_id)
            return {'message': 'Customer deleted'}
        except ValueError:
            # Maybe it's a phone, look up ID then delete?
            cust = db_manager.get_customer_by_phone(identifier)
            if cust:
                db_manager.delete_customer(cust['customer_id'])
                return {'message': 'Customer deleted'}
            return {'error': 'Invalid ID'}, 400