#!/usr/bin/env python3
"""
Neo4j MCP (Microservice Control Protocol) Client

This script tests the Neo4j MCP server by making requests to all available endpoints:
- Health check
- CRUD operations for nodes
- Creating relationships
- Running custom Cypher queries
"""

import requests
import json
import sys
import logging
from typing import Dict, Any, Optional, List, Union

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class Neo4jMCPClient:
    """Client for interacting with the Neo4j MCP Server"""
    
    def __init__(self, base_url: str = "http://localhost:5000"):
        """Initialize with the base URL of the Neo4j MCP server"""
        self.base_url = base_url
        logger.info(f"Initialized Neo4j MCP client pointing to {base_url}")
    
    def _make_request(
        self, 
        method: str, 
        endpoint: str, 
        data: Dict = None,
        expected_status: Union[int, List[int]] = None
    ) -> Dict:
        """Make an HTTP request to the server and validate the response"""
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        
        headers = {"Content-Type": "application/json"}
        
        try:
            if method.upper() == "GET":
                response = requests.get(url, headers=headers)
            elif method.upper() == "POST":
                response = requests.post(url, headers=headers, json=data)
            elif method.upper() == "PUT":
                response = requests.put(url, headers=headers, json=data)
            elif method.upper() == "DELETE":
                response = requests.delete(url, headers=headers)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")
            
            # Validate expected status if provided
            if expected_status:
                expected_status_list = [expected_status] if isinstance(expected_status, int) else expected_status
                if response.status_code not in expected_status_list:
                    logger.error(f"Expected status {expected_status_list} but got {response.status_code}: {response.text}")
                    raise Exception(f"Request failed with status {response.status_code}")
            
            # Return response data if available
            if response.text and response.headers.get("Content-Type") == "application/json":
                return response.json()
            elif response.status_code in [204]:  # No content
                return {"status": "success", "status_code": response.status_code}
            else:
                return {"status": "success", "status_code": response.status_code, "text": response.text}
                
        except requests.RequestException as e:
            logger.error(f"Request error: {e}")
            raise
    
    def health_check(self) -> Dict:
        """Check if the server is healthy"""
        logger.info("Performing health check...")
        return self._make_request("GET", "/health", expected_status=200)
    
    def create_node(self, label: str, properties: Dict) -> Dict:
        """Create a new node with given label and properties"""
        logger.info(f"Creating {label} node with properties: {properties}")
        return self._make_request("POST", f"/nodes/{label}", data=properties, expected_status=201)
    
    def get_node(self, node_id: int) -> Dict:
        """Get a node by its ID"""
        logger.info(f"Getting node with ID: {node_id}")
        return self._make_request("GET", f"/nodes/{node_id}", expected_status=200)
    
    def update_node(self, node_id: int, properties: Dict) -> Dict:
        """Update a node's properties"""
        logger.info(f"Updating node {node_id} with properties: {properties}")
        return self._make_request("PUT", f"/nodes/{node_id}", data=properties, expected_status=200)
    
    def delete_node(self, node_id: int) -> Dict:
        """Delete a node by its ID"""
        logger.info(f"Deleting node with ID: {node_id}")
        return self._make_request("DELETE", f"/nodes/{node_id}", expected_status=204)
    
    def create_relationship(
        self, 
        from_id: int, 
        to_id: int, 
        rel_type: str, 
        properties: Dict = None
    ) -> Dict:
        """Create a relationship between two nodes"""
        data = {
            "from_id": from_id,
            "to_id": to_id,
            "type": rel_type,
            "properties": properties or {}
        }
        logger.info(f"Creating relationship: {from_id}-[{rel_type}]->{to_id}")
        return self._make_request("POST", "/relationships", data=data, expected_status=201)
    
    def run_cypher(self, query: str, params: Dict = None) -> Dict:
        """Run a custom Cypher query"""
        data = {
            "query": query,
            "params": params or {}
        }
        logger.info(f"Running Cypher query: {query}")
        return self._make_request("POST", "/cypher", data=data, expected_status=200)


def run_comprehensive_test(client: Neo4jMCPClient):
    """Run a comprehensive test of all API endpoints"""
    try:
        # Step 1: Health check
        health_result = client.health_check()
        logger.info(f"Health check result: {health_result}")
        
        if health_result.get("status") != "healthy":
            logger.error("Server reports unhealthy status. Aborting tests.")
            return False
        
        # Step 2: Create person node
        person_data = {
            "name": "Alice Smith",
            "age": 32,
            "occupation": "Software Engineer"
        }
        person_result = client.create_node("Person", person_data)
        logger.info(f"Created person node: {person_result}")
        
        # Extract the node ID (implementation dependent)
        # This assumes the node is returned in a field called 'n'
        person_id = extract_node_id(person_result)
        
        if not person_id:
            logger.error("Failed to extract person node ID. Aborting tests.")
            return False
        
        # Step 3: Create company node
        company_data = {
            "name": "Tech Innovations Inc",
            "industry": "Software",
            "founded": 2010
        }
        company_result = client.create_node("Company", company_data)
        logger.info(f"Created company node: {company_result}")
        
        company_id = extract_node_id(company_result)
        
        if not company_id:
            logger.error("Failed to extract company node ID. Aborting tests.")
            return False
        
        # Step 4: Create relationship between person and company
        rel_props = {
            "role": "Senior Developer",
            "since": 2018,
            "salary": 120000
        }
        rel_result = client.create_relationship(
            person_id, 
            company_id, 
            "WORKS_AT", 
            rel_props
        )
        logger.info(f"Created relationship: {rel_result}")
        
        # Step 5: Get person node details
        get_person = client.get_node(person_id)
        logger.info(f"Retrieved person: {get_person}")
        
        # Step 6: Update person node
        update_data = {"age": 33, "skills": ["Python", "Neo4j", "Docker"]}
        update_result = client.update_node(person_id, update_data)
        logger.info(f"Updated person: {update_result}")
        
        # Step 7: Run a custom Cypher query to get relationships
        cypher_query = """
        MATCH (p:Person)-[r:WORKS_AT]->(c:Company)
        WHERE id(p) = $person_id
        RETURN p, r, c
        """
        query_result = client.run_cypher(cypher_query, {"person_id": person_id})
        logger.info(f"Cypher query result: {query_result}")
        
        # Step 8: Delete nodes (cleanup)
        delete_person = client.delete_node(person_id)
        logger.info(f"Deleted person node: {delete_person}")
        
        delete_company = client.delete_node(company_id)
        logger.info(f"Deleted company node: {delete_company}")
        
        logger.info("All tests completed successfully!")
        return True
        
    except Exception as e:
        logger.error(f"Test failed with error: {e}")
        return False


def extract_node_id(response: Dict) -> Optional[int]:
    """
    Extract node ID from server response
    This needs to be adapted based on the actual response format
    """
    # Assuming the response contains a node under key 'n' with metadata
    if 'n' in response:
        # This is an assumption, adjust based on actual response structure
        return response['n'].get('identity', response['n'].get('id'))
    
    # Try alternate formats
    keys = list(response.keys())
    if keys and isinstance(response[keys[0]], dict):
        potential_node = response[keys[0]]
        if 'identity' in potential_node:
            return potential_node['identity']
        if 'id' in potential_node:
            return potential_node['id']
    
    # If we can't find the ID in the expected structure, search recursively
    def find_id(obj):
        if isinstance(obj, dict):
            for k, v in obj.items():
                if k in ('id', 'identity') and isinstance(v, int):
                    return v
                result = find_id(v)
                if result is not None:
                    return result
        elif isinstance(obj, list):
            for item in obj:
                result = find_id(item)
                if result is not None:
                    return result
        return None
    
    return find_id(response)


if __name__ == "__main__":
    # Default to localhost if no argument is provided
    server_url = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:5000"
    
    client = Neo4jMCPClient(server_url)
    
    print(f"Running comprehensive test against Neo4j MCP server at {server_url}")
    success = run_comprehensive_test(client)
    
    if success:
        print("✅ All tests passed!")
        sys.exit(0)
    else:
        print("❌ Test failed!")
        sys.exit(1)
