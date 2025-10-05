CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE IF NOT EXISTS toll_booths (
  id SERIAL PRIMARY KEY,
  osm_id BIGINT,
  name TEXT,
  highway_type TEXT,
  lat DOUBLE PRECISION,
  lon DOUBLE PRECISION,
  geom GEOMETRY(Point,4326),
  source TEXT,
  last_updated TIMESTAMP DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_toll_geom ON toll_booths USING GIST (geom);