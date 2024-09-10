-- Create table for traffic incidents
CREATE TABLE IF NOT EXISTS incident (
    id SERIAL PRIMARY KEY,
    Type VARCHAR(255),
    Latitude DOUBLE PRECISION,
    Longitude DOUBLE PRECISION,
    Message TEXT
);

-- Create table for traffic speedbands
CREATE TABLE IF NOT EXISTS traffic_speedband (
    id SERIAL PRIMARY KEY,
    LinkID VARCHAR(255),
    RoadName VARCHAR(255),
    RoadCategory VARCHAR(255),
    SpeedBand INTEGER,
    MinimumSpeed INTEGER,
    MaximumSpeed INTEGER,
    StartLon DOUBLE PRECISION
);

-- Create table for traffic images
CREATE TABLE IF NOT EXISTS traffic_image (
    id SERIAL PRIMARY KEY,
    CameraID VARCHAR(255),
    Latitude DOUBLE PRECISION,
    Longitude DOUBLE PRECISION,
    ImageLink TEXT
);
