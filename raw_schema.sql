-- Create the schema first if it doesn't exist
CREATE SCHEMA IF NOT EXISTS raw;

-- Move the table
ALTER TABLE public.raw_fixtures SET SCHEMA raw;