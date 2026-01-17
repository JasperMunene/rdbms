from flask import Flask
from flask_cors import CORS
from flask_restful import Api
from .routes import init_routes
from .models import db_manager

def create_app():
    app = Flask(__name__)
    CORS(app) # Enable CORS for frontend communication
    api = Api(app)
    
    # Initialize Routes
    init_routes(api)
    
    # Initialize DB (and ensure tables/indices exist)
    # Accessing private method is not ideal but keeping consistent with restored logic
    db_manager._create_default_tables()
    
    return app
