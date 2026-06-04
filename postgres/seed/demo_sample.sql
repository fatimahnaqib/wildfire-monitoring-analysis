-- Sample wildfire events for local/AWS demo when FIRMS sync is not run.
-- Safe to re-run: uses ON CONFLICT DO NOTHING.

INSERT INTO wildfire_events (
    latitude, longitude, bright_ti4, bright_ti5, scan, track,
    acq_date, acq_time, satellite, confidence, version, frp, daynight
) VALUES
    (36.7783, -119.4179, 320.5, 310.2, 0.4, 0.3, CURRENT_DATE - 1, '14:22:00', 'N', 'h', '2.0NRT', 12.4, 'D'),
    (37.8651, -119.5383, 365.0, 355.1, 0.4, 0.3, CURRENT_DATE - 2, '03:15:00', 'N20', 'h', '2.0NRT', 28.7, 'N'),
    (38.5816, -121.4944, 298.0, 290.0, 0.4, 0.3, CURRENT_DATE - 3, '22:08:00', 'N21', 'n', '2.0NRT', 5.1, 'N'),
    (34.0522, -118.2437, 340.2, 330.0, 0.4, 0.3, CURRENT_DATE - 5, '18:45:00', 'N', 'l', '2.0NRT', 8.9, 'D'),
    (40.7128, -124.2029, 310.0, 300.5, 0.4, 0.3, CURRENT_DATE - 7, '11:30:00', 'N20', 'h', '2.0NRT', 15.2, 'D'),
    (35.3733, -120.5820, 285.5, 275.0, 0.4, 0.3, CURRENT_DATE - 10, '06:12:00', 'N', 'n', '2.0NRT', 3.2, 'N'),
    (39.7392, -121.8380, 355.8, 348.0, 0.4, 0.3, CURRENT_DATE - 14, '01:55:00', 'N21', 'h', '2.0NRT', 42.0, 'N'),
    (33.6846, -117.8265, 305.0, 295.0, 0.4, 0.3, CURRENT_DATE - 20, '20:40:00', 'N', 'l', '2.0NRT', 6.5, 'D')
ON CONFLICT (latitude, longitude, acq_date, acq_time, satellite) DO NOTHING;
