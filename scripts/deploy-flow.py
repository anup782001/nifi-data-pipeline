import requests
import json
import sys
import configparser
import urllib3

# Disable SSL warnings for self-signed certificates
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def deploy_flow(environment, flow_file, config_file):
    """Deploy NiFi flow to target environment"""
    
    print(f"🚀 Deploying flow to {environment} environment...")
    
    try:
        # Load configuration
        config = configparser.ConfigParser()
        config.read(config_file)
        
        nifi_url = config.get('DEFAULT', 'nifi.url')
        nifi_username = config.get('DEFAULT', 'nifi.username')
        nifi_password = config.get('DEFAULT', 'nifi.password')
        
        # Load flow file
        with open(flow_file, 'r') as f:
            flow_data = json.load(f)
        
        print("📤 Connecting to NiFi...")
        
        # Get authentication token (NiFi 2.6 uses JWT tokens)
        auth_url = f"{nifi_url}/nifi-api/access/token"
        auth_response = requests.post(
            auth_url,
            data={
                'username': nifi_username,
                'password': nifi_password
            },
            verify=False
        )
        
        if auth_response.status_code not in [200, 201]:
            print(f"❌ Authentication failed: {auth_response.status_code}")
            print(f"Response: {auth_response.text}")
            sys.exit(1)
        
        token = auth_response.text
        headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
        }
        
        print("✅ Authentication successful")
        
        # Get root process group
        root_url = f"{nifi_url}/nifi-api/flow/process-groups/root"
        root_response = requests.get(root_url, headers=headers, verify=False)
        
        if root_response.status_code != 200:
            print(f"❌ Failed to get root process group: {root_response.status_code}")
            sys.exit(1)
        
        root_id = root_response.json()['processGroupFlow']['id']
        print(f"📦 Root Process Group ID: {root_id}")
        
        # Create process group for this environment
        pg_name = f"Customer-ETL-{environment.upper()}"
        create_pg_url = f"{nifi_url}/nifi-api/process-groups/{root_id}/process-groups"
        
        pg_payload = {
            "revision": {
                "version": 0
            },
            "component": {
                "name": pg_name,
                "position": {
                    "x": 100,
                    "y": 100
                }
            }
        }
        
        print(f"📤 Creating process group: {pg_name}...")
        pg_response = requests.post(
            create_pg_url,
            headers=headers,
            json=pg_payload,
            verify=False
        )
        
        if pg_response.status_code == 201:
            print(f"✅ Process group created successfully")
            pg_id = pg_response.json()['id']
            print(f"📦 Process Group ID: {pg_id}")
        elif pg_response.status_code == 409:
            print(f"⚠️  Process group already exists, finding existing...")
            # Get existing process groups and find ours
            pgs_url = f"{nifi_url}/nifi-api/flow/process-groups/{root_id}"
            pgs_response = requests.get(pgs_url, headers=headers, verify=False)
            
            pg_id = None
            for pg in pgs_response.json()['processGroupFlow']['flow']['processGroups']:
                if pg['component']['name'] == pg_name:
                    pg_id = pg['id']
                    print(f"📦 Found existing Process Group ID: {pg_id}")
                    break
            
            if not pg_id:
                print("❌ Could not find or create process group")
                sys.exit(1)
        else:
            print(f"❌ Failed to create process group: {pg_response.status_code}")
            print(f"Response: {pg_response.text}")
            sys.exit(1)
        
        print("✅ Deployment completed successfully!")
        print(f"🌐 View flow at: {nifi_url}/nifi/?processGroupId={pg_id}")
        
        return True
        
    except Exception as e:
        print(f"❌ Deployment error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: python deploy-flow.py <environment> <flow-file> <config-file>")
        sys.exit(1)
    
    environment = sys.argv[1]
    flow_file = sys.argv[2]
    config_file = sys.argv[3]
    
    deploy_flow(environment, flow_file, config_file)