import sys
import uuid
import datetime
import os
from pathlib import Path
from typing import List, Dict, Any, Optional

# Ensure src is in path
# Find the project root (where src/ is) relative to this file
# this file is at PROJECT_ROOT/server/models.py
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root / 'src'))

from pesasql.storage.file_manager import FileManager
from pesasql.catalog.catalog import Catalog
from pesasql.query.engine import QueryEngine

class PesaSQLManager:
    def __init__(self, db_path='finance.db'):
        self.fm = FileManager(db_path)
        self.catalog = Catalog(self.fm)
        self.engine = QueryEngine(self.fm, self.catalog)
        self._create_default_tables()

    def _create_default_tables(self):
        """Initialize database schema"""
        tables_to_create = [
            {
                'name': 'users',
                'columns': [
                    ('user_id', 'INT PRIMARY KEY'),
                    ('email', 'STRING(100) UNIQUE'),
                    ('password_hash', 'STRING(255)'),
                    ('role', 'STRING(20)'), # merchant, admin
                    ('created_at', 'TIMESTAMP')
                ]
            },
            {
                'name': 'merchants',
                'columns': [
                    ('merchant_id', 'INT PRIMARY KEY'),
                    ('user_id', 'INT'), 
                    ('business_name', 'STRING(100)'),
                    ('mpesa_till', 'STRING(20)'),
                    ('wallet_balance', 'DOUBLE'),
                    ('country', 'STRING(50)'),
                    ('status', 'STRING(20)'),
                    ('CONSTRAINT', 'FOREIGN KEY (user_id) REFERENCES users(user_id)')
                ]
            },
            {
                'name': 'customers',
                'columns': [
                    ('customer_id', 'INT PRIMARY KEY'),
                    ('phone', 'STRING(20) UNIQUE'),
                    ('email', 'STRING(100)'),
                    ('full_name', 'STRING(100)'),
                    ('registration_date', 'TIMESTAMP')
                ]
            },
            {
                'name': 'transactions',
                'columns': [
                    ('transaction_id', 'INT PRIMARY KEY'),
                    ('merchant_id', 'INT NOT NULL'),
                    ('customer_id', 'INT'), 
                    ('amount', 'DOUBLE NOT NULL'),
                    ('status', "STRING(20) DEFAULT 'pending'"),
                    ('reference', 'STRING(50) UNIQUE'),
                    ('created_at', 'TIMESTAMP'),
                    ('CONSTRAINT', 'FOREIGN KEY (merchant_id) REFERENCES merchants(merchant_id)'),
                    ('CONSTRAINT', 'FOREIGN KEY (customer_id) REFERENCES customers(customer_id)')
                ]
            }
        ]

        for table_def in tables_to_create:
            table_name = table_def['name']
            if not self.catalog.get_table(table_name):
                columns_sql = []
                for item in table_def['columns']:
                    if item[0] == 'CONSTRAINT':
                        columns_sql.append(item[1])
                    else:
                        columns_sql.append(f'{item[0]} {item[1]}')
                
                sql = f"CREATE TABLE {table_name} ({', '.join(columns_sql)})"
                try:
                    self.engine.execute_sql(sql)
                    print(f"Created table: {table_name}")
                except Exception as e:
                    print(f"Error creating table {table_name}: {e}")


    def execute_query(self, sql: str) -> Dict[str, Any]:
        try:
            return self.engine.execute_sql(sql)
        except Exception as e:
            print(f"Query Error: {e}")
            return {'error': str(e)}

    def _fetch_all(self, sql: str) -> List[Dict]:
        res = self.execute_query(sql)
        if 'error' in res: return []
        
        columns = res.get('columns', [])
        rows = res.get('data', [])
        
        results = []
        for row in rows:
            results.append(dict(zip(columns, row)))
        return results

    # --- Users ---
    def register_user(self, email, password_hash, role='merchant'):
        ts = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        # Simple ID gen (max + 1)
        res = self.execute_query("SELECT user_id FROM users ORDER BY user_id DESC LIMIT 1")
        new_id = 1
        if res and res.get('data'):
            new_id = int(res['data'][0][0]) + 1
            
        sql = f"INSERT INTO users VALUES ({new_id}, '{email}', '{password_hash}', '{role}', '{ts}')"
        self.execute_query(sql)
        return new_id

    def get_user_by_email(self, email):
        res = self._fetch_all(f"SELECT * FROM users WHERE email = '{email}'")
        return res[0] if res else None

    # --- Merchants ---
    def add_merchant(self, user_id, business_name, mpesa_till, country='Kenya'):
        res = self.execute_query("SELECT merchant_id FROM merchants ORDER BY merchant_id DESC LIMIT 1")
        new_id = 1
        if res and res.get('data'):
            new_id = int(res['data'][0][0]) + 1
            
        sql = f"INSERT INTO merchants VALUES ({new_id}, {user_id}, '{business_name}', '{mpesa_till}', 0.0, '{country}', 'active')"
        self.execute_query(sql)
        return new_id

    def get_merchants(self, limit=100):
        # Using INNER JOIN users
        sql = f"""SELECT 
            m.merchant_id, m.user_id, m.business_name, m.mpesa_till, m.wallet_balance, m.country, m.status,
            u.email
        FROM merchants m
        INNER JOIN users u ON m.user_id = u.user_id"""
        return self._fetch_all(sql)[:limit]

    def get_merchant_by_id(self, m_id):
        res = self._fetch_all(f"SELECT * FROM merchants WHERE merchant_id = {m_id}")
        return res[0] if res else None

    def delete_merchant(self, m_id):
        # Cascading delete not supported? PesaSQL constraints might prevent if Transactions exist.
        return self.execute_query(f"DELETE FROM merchants WHERE merchant_id = {m_id}")

    # --- Customers ---
    def add_customer(self, phone, full_name, email=''):
        res = self.execute_query("SELECT customer_id FROM customers ORDER BY customer_id DESC LIMIT 1")
        new_id = 1
        if res and res.get('data'):
            new_id = int(res['data'][0][0]) + 1
        
        ts = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        sql = f"INSERT INTO customers VALUES ({new_id}, '{phone}', '{email}', '{full_name}', '{ts}')"
        self.execute_query(sql)
        return new_id

    def get_customers(self, limit=100):
        return self._fetch_all(f"SELECT * FROM customers LIMIT {limit}")

    def get_customer_by_phone(self, phone):
        res = self._fetch_all(f"SELECT * FROM customers WHERE phone = '{phone}'")
        return res[0] if res else None
        
    def delete_customer(self, c_id):
        return self.execute_query(f"DELETE FROM customers WHERE customer_id = {c_id}")

    # --- Transactions ---
    def add_transaction(self, merchant_id, customer_id, amount, reference=None, status='pending'):
        res = self.execute_query("SELECT transaction_id FROM transactions ORDER BY transaction_id DESC LIMIT 1")
        new_id = 1
        if res and res.get('data'):
            new_id = int(res['data'][0][0]) + 1
            
        if not reference:
            reference = str(uuid.uuid4())[:8].upper()
            
        ts = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        # Handle nullable customer_id for SQL string construction
        c_id_val = str(customer_id) if customer_id else 'NULL'
        
        sql = f"INSERT INTO transactions VALUES ({new_id}, {merchant_id}, {c_id_val}, {amount}, '{status}', '{reference}', '{ts}')"
        self.execute_query(sql)
        return {'transaction_id': new_id, 'reference': reference, 'created_at': ts}

    def get_transactions(self, merchant_id=None, limit=50):
        # Using JOINs
        sql = """SELECT 
            t.transaction_id, t.merchant_id, t.customer_id, t.amount, t.status, t.reference, t.created_at,
            m.business_name,
            c.full_name, c.phone
        FROM transactions t
        INNER JOIN merchants m ON t.merchant_id = m.merchant_id
        LEFT JOIN customers c ON t.customer_id = c.customer_id"""
        
        if merchant_id:
             sql += f" WHERE t.merchant_id = {merchant_id}"
             
        return self._fetch_all(sql)[:limit]

    def get_stats(self):
        # Aggregate manually for now as PesaSQL aggregate functions might be WIP
        txs = self.get_transactions(limit=1000)
        total_vol = sum(float(t['amount']) for t in txs)
        
        merchants = self.get_merchants()
        
        return {
            'total_volume': total_vol,
            'transaction_count': len(txs),
            'active_merchants': len(merchants)
        }

db_manager = PesaSQLManager()