CREATE TABLE IF NOT EXISTS sensor_averages_hourly (
    site_id         VARCHAR(50) NOT NULL,
    pollutant_type  VARCHAR(50) NOT NULL,
    start_time      TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    avg_value       DOUBLE PRECISION DEFAULT 0.0,
    PRIMARY KEY (site_id, pollutant_type, start_time)
);

CREATE TABLE IF NOT EXISTS sensor_metric_averages_daily (
    site_id         VARCHAR(50) NOT NULL,
    pollutant_type  VARCHAR(50) NOT NULL,
    log_date        DATE NOT NULL,
    daily_avg       DOUBLE PRECISION DEFAULT 0.0,
    last_updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    PRIMARY KEY (site_id, pollutant_type, log_date)
);

CREATE INDEX IF NOT EXISTS idx_sensor_metrics_site_pollutant 
ON sensor_metric_averages_daily (site_id, pollutant_type);

CREATE OR REPLACE FUNCTION enforce_hourly_cap()
RETURNS TRIGGER AS $$
BEGIN
    -- Check if we already have 25 or more entries for this specific site and pollutant
    IF (SELECT COUNT(*) FROM sensor_averages_hourly 
        WHERE site_id = NEW.site_id AND pollutant_type = NEW.pollutant_type) >= 25 THEN
        
        -- Delete the oldest entry
        DELETE FROM sensor_averages_hourly
        WHERE site_id = NEW.site_id 
          AND pollutant_type = NEW.pollutant_type
          AND start_time = (
              SELECT MIN(start_time) 
              FROM sensor_averages_hourly 
              WHERE site_id = NEW.site_id AND pollutant_type = NEW.pollutant_type
          );
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Attach the trigger to the table
CREATE TRIGGER trg_enforce_hourly_cap
BEFORE INSERT ON sensor_averages_hourly
FOR EACH ROW
EXECUTE FUNCTION enforce_hourly_cap();

CREATE OR REPLACE PROCEDURE compute_daily_averages()
LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO sensor_metric_averages_daily (site_id, pollutant_type, log_date, daily_avg, last_updated_at)
    SELECT 
        site_id, 
        pollutant_type, 
        (CURRENT_DATE - INTERVAL '1 day')::DATE AS log_date,
        AVG(avg_value) AS daily_avg,
        NOW() AS last_updated_at
    FROM sensor_averages_hourly
    -- Filters for data belonging to the previous calendar day
    WHERE start_time >= (CURRENT_DATE - INTERVAL '1 day') 
      AND start_time < CURRENT_DATE
    GROUP BY site_id, pollutant_type
    ON CONFLICT (site_id, pollutant_type, log_date) 
    DO UPDATE SET 
        daily_avg = EXCLUDED.daily_avg,
        last_updated_at = EXCLUDED.last_updated_at;
END;
$$;