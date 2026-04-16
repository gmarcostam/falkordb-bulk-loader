import csv
import redis
import os
import argparse

def apply_indexes_and_constraints(graph_name, csv_dir):
    r = redis.Redis(host='localhost', port=6379)
    
    # --- 1. SUPPORT INDEX CREATION (From original script) ---
    # FalkorDB often requires an existing index to create a constraint
    constraints_file = os.path.join(csv_dir, 'constraints.csv')
    if os.path.exists(constraints_file):
        print("🔧 Creating support indexes for constraints...")
        with open(constraints_file, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                label = row.get('labels', '').strip()
                prop = row.get('properties', '').strip()
                c_type = row.get('type', '').upper()
                
                if not label or not prop or 'UNIQUE' not in c_type:
                    continue
                
                # Supports multiple labels or properties separated by ;
                label_list = [l.strip() for l in label.split(';') if l.strip()]
                prop_list = [p.strip() for p in prop.split(';') if p.strip()]
                
                for lbl in label_list:
                    try:
                        # Syntax: CREATE INDEX FOR (n:Label) ON (n.Prop)
                        prop_str = ", ".join([f"n.{p}" for p in prop_list])
                        query = f"CREATE INDEX FOR (n:{lbl}) ON ({prop_str})"
                        print(f"  Executing: {query}")
                        r.execute_command("GRAPH.QUERY", graph_name, query)
                    except Exception as e:
                        if any(k in str(e).lower() for k in ['already indexed', 'exists']):
                            pass
                        else:
                            print(f"  ⚠️ Index error: {e}")

    # --- 2. CONSTRAINT CREATION (Crucial part) ---
    if os.path.exists(constraints_file):
        print("\n🔒 Creating uniqueness constraints...")
        with open(constraints_file, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                label = row.get('labels', '').strip()
                prop = row.get('properties', '').strip()
                c_type = row.get('type', '').upper()
                entity_type = row.get('entity_type', 'NODE').upper()

                if not label or not prop or 'UNIQUE' not in c_type:
                    continue

                label_list = [l.strip() for l in label.split(';') if l.strip()]
                prop_list = [p.strip() for p in prop.split(';') if p.strip()]

                for lbl in label_list:
                    try:
                        # EXACT SYNTAX:
                        # GRAPH.CONSTRAINT CREATE <graph> UNIQUE <entity> <label> PROPERTIES <count> <prop1>...
                        command_args = [
                            'GRAPH.CONSTRAINT', 'CREATE', graph_name, 'UNIQUE', 
                            entity_type, lbl, 'PROPERTIES', str(len(prop_list))
                        ] + prop_list
                        
                        result = r.execute_command(*command_args)
                        print(f"  ✅ Constraint created on {lbl}({', '.join(prop_list)}): {result}")
                    except Exception as e:
                        if 'already exists' in str(e).lower():
                            print(f"  ⚠️ Constraint on {lbl} already exists")
                        else:
                            print(f"  ❌ Critical constraint error: {e}")

    # --- 3. STANDARD INDEX CREATION ---
    indexes_file = os.path.join(csv_dir, 'indexes.csv')
    if os.path.exists(indexes_file):
        print("\n⚡ Creating standard indexes...")
        with open(indexes_file, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                label = row.get('labels', '').strip()
                prop = row.get('properties', '').strip()
                if label and prop:
                    lbl = label.split(';')[0]
                    p = prop.split(';')[0]
                    try:
                        query = f"CREATE INDEX FOR (n:{lbl}) ON (n.{p})"
                        r.execute_command("GRAPH.QUERY", graph_name, query)
                        print(f"  ✅ Index created on {lbl}({p})")
                    except Exception as e:
                        if 'already' not in str(e).lower():
                            print(f"  ❌ Index error on {lbl}: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Apply indexes and constraints to a FalkorDB graph.")
    parser.add_argument("graph_name", type=str, help="Name of the graph to apply metadata to (e.g., movies)")
    parser.add_argument("--csv-dir", type=str, default="csv_output", help="Directory containing constraints.csv and indexes.csv (default: csv_output)")
    
    args = parser.parse_args()
    
    apply_indexes_and_constraints(args.graph_name, args.csv_dir)