#!/usr/bin/env python3
"""
Neo4j MCP (Microservice Control Protocol) Server Example

This microservice demonstrates basic Neo4j database operations including:
- Connection and authentication
- CRUD operations
- Transaction management
- Query parameterization
"""

import os
import json
import logging
from typing import Dict, List, Any, Optional
from flask import Flask, request, jsonify
from neo4j import GraphDatabase, Session, Transaction
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Flask application
app = Flask(__name__)

# Neo4j connection configuration
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password")

class Neo4jService:
    """Service class to handle Neo4j database operations"""
    
    def __init__(self, uri: str, user: str, password: str):
        """Initialize connection to Neo4j database"""
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        logger.info(f"Connected to Neo4j at {uri}")
        
    def close(self):
        """Close the driver connection"""
        if self.driver:
            self.driver.close()
            logger.info("Neo4j connection closed")
    
    def verify_connectivity(self) -> bool:
        """Verify connection to Neo4j database"""
        try:
            with self.driver.session() as session:
                result = session.run("RETURN 1 AS result")
                return result.single()["result"] == 1
        except Exception as e:
            logger.error(f"Connection verification failed: {e}")
            return False
    
    def execute_read_query(self, query: str, params: Dict = None) -> List[Dict]:
        """Execute a read-only query against Neo4j"""
        with self.driver.session() as session:
            result = session.run(query, params or {})
            return [record.data() for record in result]
    
    def execute_write_query(self, query: str, params: Dict = None) -> List[Dict]:
        """Execute a write query against Neo4j"""
        with self.driver.session() as session:
            result = session.write_transaction(
                lambda tx: tx.run(query, params or {})
            )
            return [record.data() for record in result]
    
    # Node CRUD operations
    def create_node(self, label: str, properties: Dict) -> Dict:
        """Create a new node in the database"""
        query = f"""
        CREATE (n:{label} $props)
        SET n.created_at = datetime()
        RETURN n
        """
        result = self.execute_write_query(query, {"props": properties})
        return result[0] if result else {}
    
    def get_node_by_id(self, node_id: int) -> Optional[Dict]:
        """Get a node by its ID"""
        query = """
        MATCH (n) 
        WHERE id(n) = $node_id 
        RETURN n
        """
        result = self.execute_read_query(query, {"node_id": node_id})
        return result[0] if result else None
    
    def update_node(self, node_id: int, properties: Dict) -> Dict:
        """Update a node's properties"""
        query = """
        MATCH (n) 
        WHERE id(n) = $node_id 
        SET n += $props,
            n.updated_at = datetime()
        RETURN n
        """
        result = self.execute_write_query(
            query, 
            {"node_id": node_id, "props": properties}
        )
        return result[0] if result else {}
    
    def delete_node(self, node_id: int) -> bool:
        """Delete a node by its ID"""
        query = """
        MATCH (n) 
        WHERE id(n) = $node_id 
        DETACH DELETE n
        RETURN count(n) as deleted
        """
        result = self.execute_write_query(query, {"node_id": node_id})
        return result[0]["deleted"] > 0 if result else False
    
    # Relationship operations
    def create_relationship(
        self, 
        from_id: int, 
        to_id: int, 
        rel_type: str, 
        properties: Dict = None
    ) -> Dict:
        """Create a relationship between two nodes"""
        query = f"""
        MATCH (a), (b) 
        WHERE id(a) = $from_id AND id(b) = $to_id
        CREATE (a)-[r:{rel_type} $props]->(b)
        SET r.created_at = datetime()
        RETURN r
        """
        result = self.execute_write_query(
            query, 
            {
                "from_id": from_id, 
                "to_id": to_id, 
                "props": properties or {}
            }
        )
        return result[0] if result else {}
    
    # Query operations
    def run_custom_query(self, query: str, params: Dict = None) -> List[Dict]:
        """Run a custom Cypher query"""
        if query.strip().upper().startswith(("CREATE", "DELETE", "MERGE", "SET")):
            return self.execute_write_query(query, params)
        else:
            return self.execute_read_query(query, params)


# Initialize Neo4j service
neo4j_service = Neo4jService(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)

@app.route("/health", methods=["GET"])
def health_check():
    """Health check endpoint"""
    connectivity = neo4j_service.verify_connectivity()
    status = "healthy" if connectivity else "unhealthy"
    return jsonify({
        "status": status,
        "database_connection": connectivity
    }), 200 if connectivity else 503


@app.route("/nodes/<label>", methods=["POST"])
def create_node(label):
    """Create a new node with the specified label"""
    data = request.get_json()
    if not data:
        return jsonify({"error": "No data provided"}), 400
    
    try:
        result = neo4j_service.create_node(label, data)
        return jsonify(result), 201
    except Exception as e:
        logger.error(f"Error creating node: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/nodes/<int:node_id>", methods=["GET"])
def get_node(node_id):
    """Get a node by its ID"""
    try:
        result = neo4j_service.get_node_by_id(node_id)
        if result:
            return jsonify(result), 200
        return jsonify({"error": "Node not found"}), 404
    except Exception as e:
        logger.error(f"Error retrieving node: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/nodes/<int:node_id>", methods=["PUT"])
def update_node(node_id):
    """Update a node's properties"""
    data = request.get_json()
    if not data:
        return jsonify({"error": "No data provided"}), 400
    
    try:
        result = neo4j_service.update_node(node_id, data)
        if result:
            return jsonify(result), 200
        return jsonify({"error": "Node not found"}), 404
    except Exception as e:
        logger.error(f"Error updating node: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/nodes/<int:node_id>", methods=["DELETE"])
def delete_node(node_id):
    """Delete a node by its ID"""
    try:
        success = neo4j_service.delete_node(node_id)
        if success:
            return "", 204
        return jsonify({"error": "Node not found"}), 404
    except Exception as e:
        logger.error(f"Error deleting node: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/relationships", methods=["POST"])
def create_relationship():
    """Create a relationship between two nodes"""
    data = request.get_json()
    required_fields = ["from_id", "to_id", "type"]
    
    if not data or not all(field in data for field in required_fields):
        return jsonify({
            "error": f"Required fields: {', '.join(required_fields)}"
        }), 400
    
    try:
        result = neo4j_service.create_relationship(
            data["from_id"],
            data["to_id"],
            data["type"],
            data.get("properties", {})
        )
        return jsonify(result), 201
    except Exception as e:
        logger.error(f"Error creating relationship: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/cypher", methods=["POST"])
def run_cypher():
    """Execute a custom Cypher query"""
    data = request.get_json()
    
    if not data or "query" not in data:
        return jsonify({"error": "Query required"}), 400
    
    try:
        result = neo4j_service.run_custom_query(
            data["query"], 
            data.get("params", {})
        )
        return jsonify(result), 200
    except Exception as e:
        logger.error(f"Error executing Cypher query: {e}")
        return jsonify({"error": str(e)}), 500


# Graceful shutdown handler
@app.teardown_appcontext
def shutdown_neo4j(error):
    """Close Neo4j connection on app shutdown"""
    if hasattr(app, 'neo4j_service'):
        neo4j_service.close()


if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
