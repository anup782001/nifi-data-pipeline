import json
import sys

def validate_flow(flow_file):
    """Validate NiFi flow definition"""
    print("🔍 Validating NiFi flow definition...")
    
    try:
        with open(flow_file, 'r') as f:
            flow_data = json.load(f)
        
        # Check for required flow structure
        if 'flowContents' not in flow_data:
            print("❌ Invalid flow structure: missing 'flowContents'")
            sys.exit(1)
        
        # Check for processors
        if 'processors' in flow_data['flowContents']:
            processor_count = len(flow_data['flowContents']['processors'])
            print(f"✅ Found {processor_count} processors in flow")
        else:
            print("⚠️  No processors found in flow")
        
        print("✅ Flow validation passed")
        return True
        
    except json.JSONDecodeError as e:
        print(f"❌ Invalid JSON: {e}")
        sys.exit(1)
    except FileNotFoundError:
        print(f"❌ Flow file not found: {flow_file}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Validation error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python validate-flow.py <flow-file>")
        sys.exit(1)
    
    flow_file = sys.argv[1]
    validate_flow(flow_file)