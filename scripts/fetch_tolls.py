import requests
import pandas as pd
from sqlalchemy import create_engine, text
import os
import sys

OVERPASS_URL = "https://overpass-api.de/api/interpreter"

# Enhanced query that gets toll booths AND the highways they're on
query = """
[out:json][timeout:180];
area["name"="India"]->.searchArea;

(
  // Get all toll booth nodes
  node["barrier"="toll_booth"](area.searchArea);
  
  // Get ways that have toll booths on them
  way(bn)["highway"];
);

out body;
>;
out skel qt;
"""

def fetch_overpass(q):
    print("Sending request to Overpass API...")
    r = requests.post(OVERPASS_URL, data={'data': q})
    r.raise_for_status()
    return r.json()

def parse_elements(elements):
    """Parse toll booths and match them with their highways"""
    
    # Separate nodes and ways
    toll_nodes = {}
    ways = {}
    way_nodes = {}  # Map of way_id to list of node_ids
    
    for el in elements:
        el_type = el.get('type')
        el_id = el.get('id')
        
        if el_type == 'node':
            tags = el.get('tags', {})
            if tags.get('barrier') == 'toll_booth':
                toll_nodes[el_id] = {
                    'osm_id': el_id,
                    'lat': el.get('lat'),
                    'lon': el.get('lon'),
                    'tags': tags
                }
        
        elif el_type == 'way':
            tags = el.get('tags', {})
            highway = tags.get('highway')
            if highway:
                ways[el_id] = {
                    'highway': highway,
                    'name': tags.get('name') or tags.get('ref'),
                    'ref': tags.get('ref'),
                    'toll': tags.get('toll'),
                    'operator': tags.get('operator')
                }
                way_nodes[el_id] = el.get('nodes', [])
    
    # Match toll booths with highways
    rows = []
    matched = 0
    
    for toll_id, toll_data in toll_nodes.items():
        # Find which way this toll booth belongs to
        highway_info = None
        for way_id, nodes in way_nodes.items():
            if toll_id in nodes:
                highway_info = ways.get(way_id)
                if highway_info:
                    matched += 1
                break
        
        # Get toll booth name (prefer toll booth name, fallback to highway name)
        toll_name = (toll_data['tags'].get('name') or 
                    toll_data['tags'].get('ref') or 
                    (highway_info['name'] if highway_info else None))
        
        # Get operator
        operator = (toll_data['tags'].get('operator') or 
                   (highway_info['operator'] if highway_info else None))
        
        rows.append({
            'osm_id': toll_data['osm_id'],
            'name': toll_name,
            'operator': operator,
            'highway_type': highway_info['highway'] if highway_info else None,
            'highway_name': highway_info['name'] if highway_info else None,
            'highway_ref': highway_info['ref'] if highway_info else None,
            'lat': toll_data['lat'],
            'lon': toll_data['lon']
        })
    
    print(f"  → {matched}/{len(toll_nodes)} toll booths matched with highway data")
    return pd.DataFrame(rows)

def create_table_if_not_exists(engine):
    """Create toll_booths table with proper constraints"""
    create_table_sql = text("""
    CREATE TABLE IF NOT EXISTS toll_booths (
        id SERIAL PRIMARY KEY,
        osm_id BIGINT UNIQUE NOT NULL,
        name VARCHAR(255),
        operator VARCHAR(255),
        highway_type VARCHAR(100),
        highway_name VARCHAR(255),
        highway_ref VARCHAR(100),
        lat DOUBLE PRECISION,
        lon DOUBLE PRECISION,
        geom GEOMETRY(Point, 4326),
        source VARCHAR(50),
        last_updated TIMESTAMP DEFAULT NOW(),
        created_at TIMESTAMP DEFAULT NOW()
    );
    
    CREATE INDEX IF NOT EXISTS idx_toll_booths_geom ON toll_booths USING GIST(geom);
    CREATE INDEX IF NOT EXISTS idx_toll_booths_osm_id ON toll_booths(osm_id);
    CREATE INDEX IF NOT EXISTS idx_toll_booths_highway_type ON toll_booths(highway_type);
    """)
    
    with engine.begin() as conn:
        conn.execute(create_table_sql)
    print("✓ Table 'toll_booths' created/verified")

def save_to_db(df, db_uri):
    try:
        engine = create_engine(db_uri)
        # Test connection first
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("✓ Database connection successful")
        
        # Create table if it doesn't exist
        create_table_if_not_exists(engine)
        
        # Insert data with upsert
        inserted = 0
        updated = 0
        with engine.begin() as conn:
            for _, r in df.iterrows():
                stmt = text("""
                INSERT INTO toll_booths (osm_id, name, operator, highway_type, highway_name, 
                                        highway_ref, lat, lon, geom, source)
                VALUES (:osm_id, :name, :operator, :highway_type, :highway_name, 
                        :highway_ref, :lat, :lon, 
                        ST_SetSRID(ST_MakePoint(:lon, :lat),4326), 'overpass')
                ON CONFLICT (osm_id) DO UPDATE
                SET name = EXCLUDED.name,
                    operator = EXCLUDED.operator,
                    highway_type = EXCLUDED.highway_type,
                    highway_name = EXCLUDED.highway_name,
                    highway_ref = EXCLUDED.highway_ref,
                    lat = EXCLUDED.lat,
                    lon = EXCLUDED.lon,
                    geom = EXCLUDED.geom,
                    last_updated = NOW()
                RETURNING (xmax = 0) AS inserted;
                """)
                result = conn.execute(stmt, {
                    'osm_id': int(r['osm_id']),
                    'name': r['name'],
                    'operator': r['operator'],
                    'highway_type': r['highway_type'],
                    'highway_name': r['highway_name'],
                    'highway_ref': r['highway_ref'],
                    'lat': float(r['lat']) if pd.notna(r['lat']) else None,
                    'lon': float(r['lon']) if pd.notna(r['lon']) else None
                })
                if result.fetchone()[0]:
                    inserted += 1
                else:
                    updated += 1
        
        print(f"  → {inserted} new records inserted")
        print(f"  → {updated} existing records updated")
        return True
    except Exception as e:
        print(f"✗ Database error: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    # Configuration - UPDATE THESE VALUES
    DB_CONFIG = {
        'user': 'postgres',    # Change if needed
        'password': 'sakmo', # Change if needed
        'host': 'localhost',
        'port': '5432',
        'database': 'tollpass'
    }
    
    # Construct DB URI
    DB_URI = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    
    print("=" * 60)
    print("TOLL BOOTH DATA FETCHER - ENHANCED VERSION")
    print("=" * 60)
    
    # Step 1: Fetch from Overpass
    print("\n[1/5] Fetching toll booths + highway data from Overpass API...")
    try:
        data = fetch_overpass(query)
        elements = data.get('elements', [])
        print(f"✓ Received {len(elements)} total elements (nodes + ways)")
    except Exception as e:
        print(f"✗ Failed to fetch from Overpass: {e}")
        sys.exit(1)
    
    # Step 2: Parse data
    print("\n[2/5] Parsing and matching toll booths with highways...")
    df = parse_elements(elements)
    df = df.dropna(subset=['lat', 'lon'])
    print(f"✓ Parsed {len(df)} valid toll booths")
    
    if len(df) == 0:
        print("✗ No valid toll booths found. Exiting.")
        sys.exit(1)
    
    # Step 3: Show statistics
    print("\n[3/5] Data Statistics:")
    print(f"  → With highway type: {df['highway_type'].notna().sum()}/{len(df)}")
    print(f"  → With name: {df['name'].notna().sum()}/{len(df)}")
    print(f"  → With operator: {df['operator'].notna().sum()}/{len(df)}")
    print("\n  Highway type breakdown:")
    if df['highway_type'].notna().any():
        for htype, count in df['highway_type'].value_counts().head(10).items():
            print(f"    • {htype}: {count}")
    else:
        print("    (No highway types found)")
    
    # Step 4: Save to CSV
    print("\n[4/5] Saving to CSV...")
    try:
        if not os.path.exists('data'):
            os.makedirs('data')
        df.to_csv('data/toll_booths_overpass.csv', index=False)
        print(f"✓ Saved to: data/toll_booths_overpass.csv")
    except Exception as e:
        print(f"✗ Failed to save CSV: {e}")
    
    # Step 5: Insert into PostgreSQL
    print("\n[5/5] Inserting into PostgreSQL...")
    print(f"Connecting to: {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")
    print(f"Username: {DB_CONFIG['user']}")
    
    if save_to_db(df, DB_URI):
        print(f"✓ Successfully processed {len(df)} records")
    else:
        print("\n" + "=" * 60)
        print("DATABASE CONNECTION FAILED")
        print("=" * 60)
        sys.exit(1)
    
    print("\n" + "=" * 60)
    print("✓ ALL DONE!")
    print("=" * 60)